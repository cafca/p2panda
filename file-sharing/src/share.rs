use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use iroh_blobs::hashseq::HashSeq;
use iroh_blobs::{BlobFormat, HashAndFormat};
use p2panda_blobs::Hash as BlobHash;
use p2panda_net::gossip::GossipHandle;
use p2panda_net::TopicId;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::manifest::{serialize_manifest, sign_manifest, ManifestData, ManifestFile};
use crate::node::AppNode;
use crate::protocol::CollectionAnnouncement;
use crate::share_code::encode_share_code;

const REANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedFile {
    pub relative_path: String,
    pub size: u64,
    pub hash: BlobHash,
}

pub struct ShareSession {
    pub source_dir: PathBuf,
    pub share_code: String,
    pub collection_hash: BlobHash,
    pub manifest_hash: BlobHash,
    pub total_bytes: u64,
    pub files: Vec<SharedFile>,
    topic_id: TopicId,
    gossip_handle: Arc<GossipHandle>,
    reannounce_task: JoinHandle<()>,
}

impl ShareSession {
    pub fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub async fn publish_announcement(&self) -> Result<()> {
        self.gossip_handle
            .publish(CollectionAnnouncement::new(self.collection_hash).encode())
            .await
            .context("failed to publish collection announcement")?;
        Ok(())
    }
}

impl Drop for ShareSession {
    fn drop(&mut self) {
        self.reannounce_task.abort();
    }
}

pub async fn share_directory(node: &AppNode, directory: impl AsRef<Path>) -> Result<ShareSession> {
    let source_dir = directory.as_ref().to_path_buf();
    let files = scan_directory(&source_dir)?;
    ensure!(
        !files.is_empty(),
        "shared directory {} does not contain any eligible files",
        source_dir.display()
    );

    let imported_files = import_files(node, &files).await?;
    ensure!(
        !imported_files.is_empty(),
        "failed to import any files from {}",
        source_dir.display()
    );

    let total_bytes = imported_files.iter().map(|file| file.size).sum();
    let manifest = ManifestData {
        version: 1,
        name: directory_name(&source_dir)?,
        files: imported_files
            .iter()
            .map(|file| ManifestFile {
                relative_path: file.relative_path.clone(),
                size: file.size,
                hash: file.hash.into(),
            })
            .collect(),
    };

    let signed_manifest =
        sign_manifest_from_node(node, &manifest).context("failed to sign share manifest")?;
    let manifest_bytes = serialize_manifest(&signed_manifest)?;
    let manifest_tag = node
        .blobs
        .add_bytes(manifest_bytes)
        .await
        .context("failed to store manifest blob")?;
    let manifest_hash = manifest_tag.hash;

    let links: HashSeq = std::iter::once(manifest_hash)
        .chain(imported_files.iter().map(|file| file.hash))
        .collect();
    let collection_tag = node
        .blobs
        .add_bytes_with_opts((links.into_inner(), BlobFormat::HashSeq))
        .await
        .context("failed to store collection hash sequence")?;
    let collection_hash = collection_tag.hash;

    node.blobs
        .store()
        .tags()
        .set(
            share_pin_name(collection_hash),
            HashAndFormat {
                hash: collection_hash,
                format: BlobFormat::HashSeq,
            },
        )
        .await
        .context("failed to pin shared collection")?;

    let topic_id: TopicId = crate::share_code::derive_topic(*collection_hash.as_bytes());
    let gossip_handle = Arc::new(
        node.join_topic(topic_id)
            .await
            .context("failed to join gossip topic for share")?,
    );

    let share_code = encode_share_code(
        collection_hash,
        node.node_id(),
        node.relay_url.as_ref().map(ToString::to_string),
    )?;

    let session = ShareSession {
        source_dir,
        share_code,
        collection_hash,
        manifest_hash,
        total_bytes,
        files: imported_files,
        topic_id,
        reannounce_task: spawn_reannouncement(Arc::clone(&gossip_handle), collection_hash),
        gossip_handle,
    };

    session.publish_announcement().await?;

    Ok(session)
}

fn spawn_reannouncement(
    gossip_handle: Arc<GossipHandle>,
    collection_hash: BlobHash,
) -> JoinHandle<()> {
    let announcement = CollectionAnnouncement::new(collection_hash).encode();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(REANNOUNCE_INTERVAL);
        interval.tick().await;

        loop {
            interval.tick().await;
            if let Err(err) = gossip_handle.publish(announcement.clone()).await {
                warn!("failed to re-announce shared collection: {err}");
                break;
            }
        }
    })
}

fn sign_manifest_from_node(
    node: &AppNode,
    manifest: &ManifestData,
) -> Result<crate::manifest::SignedManifest> {
    let key_bytes =
        fs::read(node.data_dir.join("node.key")).context("failed to reload node key")?;
    let key_bytes: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid persisted node key length"))?;
    let private_key = p2panda_core::PrivateKey::from_bytes(&key_bytes);
    sign_manifest(&private_key, manifest)
}

#[derive(Debug, Clone)]
struct ScannedFile {
    absolute_path: PathBuf,
    relative_path: String,
    size: u64,
}

fn scan_directory(root: &Path) -> Result<Vec<ScannedFile>> {
    ensure!(root.is_dir(), "{} is not a directory", root.display());

    let mut files = Vec::new();
    scan_directory_recursive(root, root, &mut files)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn scan_directory_recursive(
    root: &Path,
    current: &Path,
    files: &mut Vec<ScannedFile>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        if file_name.starts_with('.') || file_type.is_symlink() {
            continue;
        }

        if file_type.is_dir() {
            scan_directory_recursive(root, &path, files)?;
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        let metadata = entry.metadata()?;
        files.push(ScannedFile {
            absolute_path: path.clone(),
            relative_path: relative_path(root, &path)?,
            size: metadata.len(),
        });
    }

    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("{} is not inside {}", path.display(), root.display()))?;
    let mut parts = Vec::new();

    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("invalid relative path {}", relative.display());
            }
        }
    }

    ensure!(
        !parts.is_empty(),
        "refusing to share directory root as a file"
    );
    Ok(parts.join("/"))
}

fn directory_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("failed to derive directory name from {}", path.display()))
}

async fn import_files(node: &AppNode, files: &[ScannedFile]) -> Result<Vec<SharedFile>> {
    let mut imported = Vec::new();

    for file in files {
        match node.blobs.add_path(&file.absolute_path).await {
            Ok(tag) => imported.push(SharedFile {
                relative_path: file.relative_path.clone(),
                size: file.size,
                hash: tag.hash,
            }),
            Err(err) => {
                warn!(
                    "failed to import {} into blob store: {err}",
                    file.absolute_path.display()
                );
            }
        }
    }

    Ok(imported)
}

pub(crate) fn share_pin_name(collection_hash: BlobHash) -> String {
    format!("share/{}", collection_hash.to_hex())
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use anyhow::Result;
    use futures_util::StreamExt;
    use p2panda_net::addrs::NodeInfo;
    use tempfile::tempdir;
    use tokio::time::timeout;

    use super::*;
    use crate::node::NodeOptions;
    use crate::protocol::CollectionAnnouncement;
    use crate::share_code::decode_share_code;

    #[tokio::test(flavor = "multi_thread")]
    async fn sharing_directory_produces_share_code_and_store_artifacts() -> Result<()> {
        let node_dir = tempdir()?;
        let source_dir = tempdir()?;
        let source_root = source_dir.path().join("share-me");
        fs::create_dir_all(source_root.join("nested"))?;
        fs::create_dir_all(source_root.join(".ignored-dir"))?;

        fs::write(source_root.join("alpha.txt"), b"alpha")?;
        fs::write(source_root.join("nested").join("beta.bin"), [7_u8; 128])?;
        fs::write(source_root.join("nested").join("gamma.txt"), b"gamma")?;
        fs::write(source_root.join(".hidden.txt"), b"ignored")?;

        let node = AppNode::with_data_dir(node_dir.path(), NodeOptions::default()).await?;
        let share = share_directory(&node, &source_root).await?;

        let decoded = decode_share_code(&share.share_code)?;
        assert_eq!(decoded.collection_hash(), share.collection_hash);
        assert_eq!(decoded.node_id()?, node.node_id());
        assert_eq!(share.file_count(), 3);
        assert_eq!(share.total_bytes, 5 + 128 + 5);

        assert!(node.blobs.has(share.manifest_hash).await?);
        assert!(node.blobs.has(share.collection_hash).await?);
        for file in &share.files {
            assert!(node.blobs.has(file.hash).await?);
        }

        let collection_bytes = node.blobs.get_bytes(share.collection_hash).await?;
        let links =
            HashSeq::new(collection_bytes).context("stored collection is not a hash sequence")?;
        assert_eq!(links.len(), share.files.len() + 1);
        assert_eq!(links.get(0), Some(share.manifest_hash));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sharing_directory_publishes_collection_announcement() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let source_root = source_dir.path().join("share-me");
        fs::create_dir_all(&source_root)?;
        let mut file = fs::File::create(source_root.join("payload.txt"))?;
        writeln!(file, "hello gossip")?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        let endpoint_addr = node_a.endpoint.endpoint().await?.addr();
        node_b
            .address_book
            .insert_node_info(NodeInfo::from(endpoint_addr).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let subscriber_handle = node_b.join_topic(share.topic_id()).await?;
        let mut subscription = subscriber_handle.subscribe();

        share.publish_announcement().await?;

        let announcement = timeout(Duration::from_secs(10), async {
            while let Some(Ok(bytes)) = subscription.next().await {
                if let Ok(announcement) = CollectionAnnouncement::decode(&bytes) {
                    return Some(announcement);
                }
            }
            None
        })
        .await
        .context("timed out waiting for collection announcement")?
        .context("subscription ended before receiving collection announcement")?;

        assert_eq!(announcement.collection_hash, share.collection_hash);

        Ok(())
    }
}
