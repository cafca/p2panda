use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use p2panda_blobs::Hash as BlobHash;
use p2panda_net::gossip::GossipHandle;
use p2panda_core::Topic;
use tracing::warn;

use crate::manifest::{serialize_manifest, sign_manifest, ManifestData, ManifestFile};
use crate::node::AppNode;
use crate::persist::ShareRecord;
use crate::profile::ProfileStore;
use crate::profile_sync::ProfileSyncService;
use crate::share_code::encode_share_code;

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
    pub manifest_bytes: Vec<u8>,
    pub total_bytes: u64,
    pub files: Vec<SharedFile>,
    pub owner_profile_id: Option<String>,
    topic_id: Topic,
    _gossip_handle: GossipHandle,
}

struct ShareMetadataPublisher {
    _profile_sync: ProfileSyncService,
}

impl ShareSession {
    pub fn topic_id(&self) -> Topic {
        self.topic_id
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

pub async fn share_directory(node: &AppNode, directory: impl AsRef<Path>) -> Result<ShareSession> {
    share_directory_for_owner(node, directory, None).await
}

pub async fn publish_share_metadata(
    node: &AppNode,
    share: &ShareSession,
) -> Result<impl Send + 'static> {
    let mut profile_store = ProfileStore::load_or_create(&node.data_dir)?;
    let profile_id = share
        .owner_profile_id
        .clone()
        .unwrap_or_else(|| profile_store.profile().profile_id.clone());
    let mut share_record = ShareRecord::from(share);
    share_record.owner_profile_id = Some(profile_id.clone());
    profile_store.ensure_share_ownership_record(&share_record)?;

    let mut profile_sync = ProfileSyncService::new(node, profile_id).await?;
    profile_sync.refresh_local_profile().await?;
    Ok(ShareMetadataPublisher {
        _profile_sync: profile_sync,
    })
}

pub async fn share_directory_for_owner(
    node: &AppNode,
    directory: impl AsRef<Path>,
    owner_profile_id: Option<&str>,
) -> Result<ShareSession> {
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
    let collection_hash = BlobHash::new(&manifest_bytes);

    pin_shared_blobs(node, collection_hash, &imported_files).await?;

    let topic_id: Topic = crate::share_code::derive_topic(*collection_hash.as_bytes());
    let gossip_handle = node
        .join_topic(topic_id)
        .await
        .context("failed to join gossip topic for share")?;

    let share_code = encode_share_code(
        collection_hash,
        node.node_id(),
        node.relay_url.as_ref().map(ToString::to_string),
        owner_profile_id.map(str::to_owned),
    )?;

    let session = ShareSession {
        source_dir,
        share_code,
        collection_hash,
        manifest_bytes,
        total_bytes,
        files: imported_files,
        owner_profile_id: owner_profile_id.map(str::to_owned),
        topic_id,
        _gossip_handle: gossip_handle,
    };

    Ok(session)
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
    let private_key = p2panda_core::SigningKey::from_bytes(&key_bytes);
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

pub(crate) fn share_pin_prefix(collection_hash: BlobHash) -> String {
    share_pin_name(collection_hash)
}

pub(crate) fn share_file_pin_name(collection_hash: BlobHash, file_hash: BlobHash) -> String {
    format!(
        "{}/file/{}",
        share_pin_prefix(collection_hash),
        file_hash.to_hex()
    )
}

async fn pin_shared_blobs(
    node: &AppNode,
    collection_hash: BlobHash,
    files: &[SharedFile],
) -> Result<()> {
    for file in files {
        node.blobs
            .pins()
            .set(share_file_pin_name(collection_hash, file.hash), file.hash)
            .await
            .with_context(|| {
                format!(
                    "failed to pin shared file {} for collection {}",
                    file.relative_path, collection_hash
                )
            })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::*;
    use crate::manifest::verify_manifest;
    use crate::node::NodeOptions;
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
        assert_eq!(BlobHash::new(&share.manifest_bytes), share.collection_hash);

        for file in &share.files {
            assert!(node.blobs.has(file.hash).await?);
        }

        let verified_manifest = verify_manifest(&share.manifest_bytes)?;
        assert_eq!(verified_manifest.data.name, "share-me");
        assert_eq!(verified_manifest.data.files.len(), share.files.len());

        Ok(())
    }
}
