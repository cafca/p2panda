use std::collections::HashSet;
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use futures_util::StreamExt;
use iroh_blobs::hashseq::HashSeq;
use p2panda_blobs::{DownloadProgress, DownloadProgressItem, Hash as BlobHash};
use p2panda_core::PublicKey;
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr};

use crate::manifest::verify_manifest;
use crate::node::AppNode;
use crate::share_code::{decode_share_code, ShareCode};

const DOWNLOAD_ATTEMPTS: usize = 5;
const DOWNLOAD_RETRY_DELAY: Duration = Duration::from_millis(250);
const DOWNLOAD_BLOB_PIN_PREFIX: &str = "downloaded/";
const COMPLETED_BLOBS_DIR: &str = "completed_blobs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadEvent {
    DownloadStarted {
        directory_name: String,
        total_bytes: u64,
        file_count: usize,
        collection_hash: String,
    },
    FileDownloadProgress {
        file_index: usize,
        bytes_downloaded: u64,
    },
    FileCompleted {
        file_index: usize,
    },
    FileError {
        file_index: usize,
        error_message: String,
    },
    TransferCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedFile {
    pub relative_path: String,
    pub size: u64,
    pub hash: BlobHash,
    pub skipped_download: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadSession {
    pub share_code: ShareCode,
    pub collection_hash: BlobHash,
    pub manifest_hash: BlobHash,
    pub output_root: PathBuf,
    pub directory_name: String,
    pub total_bytes: u64,
    pub sharer_public_key: PublicKey,
    pub files: Vec<DownloadedFile>,
}

pub async fn download_share(
    node: &AppNode,
    share_code: &str,
    output_directory: impl AsRef<Path>,
) -> Result<DownloadSession> {
    download_share_with_progress(node, share_code, output_directory, |_| {}).await
}

pub async fn download_share_with_progress<F>(
    node: &AppNode,
    share_code: &str,
    output_directory: impl AsRef<Path>,
    mut on_event: F,
) -> Result<DownloadSession>
where
    F: FnMut(DownloadEvent),
{
    let share_code = decode_share_code(share_code).context("failed to decode share code")?;
    let sharer_node_id = share_code.node_id()?;
    let completed_blobs = load_completed_blobs(&node.data_dir);
    bootstrap_sharer(node, &share_code)
        .await
        .context("failed to bootstrap sharer from share code")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let providers = download_providers(node, sharer_node_id).await?;

    let collection_hash = share_code.collection_hash();
    retry_download("collection blob", || {
        download_collection(node, collection_hash, &providers)
    })
    .await
    .context("failed to download collection blob")?;

    let collection_bytes = node
        .blobs
        .get_bytes(collection_hash)
        .await
        .context("failed to read downloaded collection blob")?;
    let mut collection = HashSeq::new(collection_bytes)
        .ok_or_else(|| anyhow!("collection blob is not a valid hash sequence"))?;
    let manifest_hash = collection
        .pop_front()
        .ok_or_else(|| anyhow!("collection hash sequence does not contain a manifest hash"))?;

    retry_download("manifest blob", || {
        download_blob_from_providers(node, manifest_hash, &providers)
    })
    .await
    .context("failed to download manifest blob")?;

    let manifest_bytes = node
        .blobs
        .get_bytes(manifest_hash)
        .await
        .context("failed to read downloaded manifest blob")?;
    let verified_manifest = verify_manifest(&manifest_bytes)?;
    ensure!(
        verified_manifest.public_key == share_code.node_id()?,
        "manifest signer does not match share code node id"
    );
    ensure!(
        verified_manifest.data.files.len() == collection.len(),
        "manifest file count ({}) does not match collection payload count ({})",
        verified_manifest.data.files.len(),
        collection.len()
    );

    let output_root = output_directory.as_ref().join(&verified_manifest.data.name);
    tokio::fs::create_dir_all(&output_root)
        .await
        .with_context(|| {
            format!(
                "failed to create output directory {}",
                output_root.display()
            )
        })?;

    let total_bytes = verified_manifest
        .data
        .files
        .iter()
        .map(|file| file.size)
        .sum();
    on_event(DownloadEvent::DownloadStarted {
        directory_name: verified_manifest.data.name.clone(),
        total_bytes,
        file_count: verified_manifest.data.files.len(),
        collection_hash: collection_hash.to_string(),
    });

    let mut downloaded_files = Vec::with_capacity(verified_manifest.data.files.len());
    let mut file_errors = Vec::new();

    for (file_index, (manifest_file, expected_hash)) in verified_manifest
        .data
        .files
        .iter()
        .zip(collection.into_iter())
        .enumerate()
    {
        let file_hash = BlobHash::from_bytes(manifest_file.hash);
        if file_hash != expected_hash {
            let error = format!(
                "verification failed for {}: manifest hash mismatch",
                manifest_file.relative_path
            );
            on_event(DownloadEvent::FileError {
                file_index,
                error_message: error.clone(),
            });
            file_errors.push(error);
            continue;
        }

        match download_one_file(
            node,
            file_index,
            manifest_file,
            &output_root,
            &providers,
            &completed_blobs,
            &mut on_event,
        )
        .await
        {
            Ok(downloaded_file) => downloaded_files.push(downloaded_file),
            Err(err) => {
                let error = format!(
                    "verification failed for {}: {err}",
                    manifest_file.relative_path
                );
                on_event(DownloadEvent::FileError {
                    file_index,
                    error_message: error.clone(),
                });
                file_errors.push(error);
            }
        }
    }

    if !file_errors.is_empty() {
        bail!("verification failed: {}", file_errors.join("; "));
    }

    on_event(DownloadEvent::TransferCompleted);

    Ok(DownloadSession {
        share_code,
        collection_hash,
        manifest_hash,
        output_root,
        directory_name: verified_manifest.data.name,
        total_bytes,
        sharer_public_key: verified_manifest.public_key,
        files: downloaded_files,
    })
}

async fn bootstrap_sharer(node: &AppNode, share_code: &ShareCode) -> Result<()> {
    let sharer_node_id = share_code.node_id()?;
    if let Some(node_info) = node.address_book.node_info(sharer_node_id).await? {
        node.address_book
            .insert_node_info(node_info.bootstrap())
            .await
            .context("failed to mark existing sharer node as bootstrap")?;
        return Ok(());
    }

    let mut endpoint_addr = EndpointAddr::new(from_public_key(sharer_node_id));
    if let Some(relay_url) = &share_code.relay_url {
        endpoint_addr = endpoint_addr.with_relay_url(relay_url.parse()?);
    }

    node.address_book
        .insert_node_info(NodeInfo::from(endpoint_addr).bootstrap())
        .await
        .context("failed to insert sharer bootstrap node into address book")?;

    Ok(())
}

async fn download_providers(node: &AppNode, sharer_node_id: PublicKey) -> Result<Vec<PublicKey>> {
    let mut providers = node
        .address_book
        .node_ids()
        .await
        .context("failed to read provider node ids from address book")?;
    providers.retain(|node_id| *node_id != node.node_id());
    if !providers.contains(&sharer_node_id) {
        providers.push(sharer_node_id);
    }
    Ok(providers)
}

async fn download_collection(
    node: &AppNode,
    collection_hash: BlobHash,
    providers: &[PublicKey],
) -> Result<()> {
    let endpoint = node
        .endpoint
        .endpoint()
        .await
        .map_err(|err| anyhow!(err))
        .context("failed to access iroh endpoint for collection download")?;

    node.blobs
        .store()
        .downloader(&endpoint)
        .download(
            iroh_blobs::HashAndFormat::hash_seq(collection_hash),
            providers
                .iter()
                .copied()
                .map(from_public_key)
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(|err| anyhow!(err))
        .context("hashseq collection download failed")?;

    Ok(())
}

async fn download_blob_from_providers(
    node: &AppNode,
    hash: BlobHash,
    providers: &[PublicKey],
) -> Result<()> {
    let progress = download_blob_with_progress_from_providers(node, hash, providers).await?;
    progress.await.map_err(|err| anyhow!(err))?;
    Ok(())
}

async fn download_blob_with_progress_from_providers(
    node: &AppNode,
    hash: BlobHash,
    providers: &[PublicKey],
) -> Result<DownloadProgress> {
    let endpoint = node
        .endpoint
        .endpoint()
        .await
        .map_err(|err| anyhow!(err))
        .context("failed to access iroh endpoint for blob download")?;
    Ok(node.blobs.store().downloader(&endpoint).download(
        hash,
        providers
            .iter()
            .copied()
            .map(from_public_key)
            .collect::<Vec<_>>(),
    ))
}

async fn retry_download<T, F, Fut>(label: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_err = None;

    for attempt in 1..=DOWNLOAD_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < DOWNLOAD_ATTEMPTS => {
                last_err = Some(err);
                tokio::time::sleep(DOWNLOAD_RETRY_DELAY).await;
            }
            Err(err) => {
                last_err = Some(err);
                break;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("download failed for {label}")))
        .with_context(|| format!("exhausted retries for {label}"))
}

async fn download_one_file<F>(
    node: &AppNode,
    file_index: usize,
    manifest_file: &crate::manifest::ManifestFile,
    output_root: &Path,
    providers: &[PublicKey],
    completed_blobs: &HashSet<BlobHash>,
    on_event: &mut F,
) -> Result<DownloadedFile>
where
    F: FnMut(DownloadEvent),
{
    let file_hash = BlobHash::from_bytes(manifest_file.hash);
    let previously_completed = completed_blobs.contains(&file_hash);
    let present_now = node.blobs.has(file_hash).await?;
    let skipped_download = present_now && previously_completed;

    if !present_now {
        let progress = retry_download("file blob", || {
            download_blob_with_progress_from_providers(node, file_hash, providers)
        })
        .await
        .with_context(|| {
            format!(
                "failed to start blob download for {}",
                manifest_file.relative_path
            )
        })?;
        let mut progress_stream = progress
            .stream()
            .await
            .context("failed to open blob download progress stream")?;

        while let Some(item) = progress_stream.next().await {
            match item {
                DownloadProgressItem::Progress(bytes_downloaded) => {
                    on_event(DownloadEvent::FileDownloadProgress {
                        file_index,
                        bytes_downloaded,
                    });
                }
                DownloadProgressItem::Error(err) => {
                    return Err(anyhow!(err)).with_context(|| {
                        format!("blob download failed for {}", manifest_file.relative_path)
                    });
                }
                DownloadProgressItem::DownloadError => {
                    bail!("blob download failed for {}", manifest_file.relative_path);
                }
                DownloadProgressItem::TryProvider { .. }
                | DownloadProgressItem::ProviderFailed { .. }
                | DownloadProgressItem::PartComplete { .. } => {}
            }
        }
    } else if !skipped_download {
        on_event(DownloadEvent::FileDownloadProgress {
            file_index,
            bytes_downloaded: manifest_file.size,
        });
    }

    let blob_bytes = node
        .blobs
        .get_bytes(file_hash)
        .await
        .with_context(|| format!("failed to read blob for {}", manifest_file.relative_path))?;
    ensure!(
        blob_bytes.len() as u64 == manifest_file.size,
        "downloaded size mismatch for {}: expected {}, got {}",
        manifest_file.relative_path,
        manifest_file.size,
        blob_bytes.len()
    );

    let relative_path = sanitize_relative_path(&manifest_file.relative_path)?;
    let destination = output_root.join(&relative_path);
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create parent directory for {}",
                destination.display()
            )
        })?;
    }
    tokio::fs::write(&destination, blob_bytes.as_ref())
        .await
        .with_context(|| format!("failed to write {}", destination.display()))?;

    mark_blob_completed(&node.data_dir, file_hash).with_context(|| {
        format!(
            "failed to record completed blob for {}",
            manifest_file.relative_path
        )
    })?;

    node.blobs
        .pins()
        .set(download_blob_pin_name(file_hash), file_hash)
        .await
        .with_context(|| {
            format!(
                "failed to pin downloaded blob for {}",
                manifest_file.relative_path
            )
        })?;

    on_event(DownloadEvent::FileCompleted { file_index });

    Ok(DownloadedFile {
        relative_path: relative_path.to_string_lossy().replace('\\', "/"),
        size: manifest_file.size,
        hash: file_hash,
        skipped_download,
    })
}

fn download_blob_pin_name(hash: BlobHash) -> String {
    format!("{DOWNLOAD_BLOB_PIN_PREFIX}{hash}")
}

fn completed_blobs_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(COMPLETED_BLOBS_DIR)
}

fn load_completed_blobs(data_dir: &Path) -> HashSet<BlobHash> {
    let dir = completed_blobs_dir(data_dir);
    let mut set = HashSet::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return set,
    };
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(hash) = name.parse::<BlobHash>() {
                set.insert(hash);
            }
        }
    }
    set
}

fn mark_blob_completed(data_dir: &Path, hash: BlobHash) -> Result<()> {
    let dir = completed_blobs_dir(data_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create completed blobs dir {}", dir.display()))?;
    std::fs::write(dir.join(hash.to_string()), b"")
        .with_context(|| format!("failed to mark blob {hash} as completed"))?;
    Ok(())
}

fn sanitize_relative_path(path: &str) -> Result<PathBuf> {
    let parsed = Path::new(path);
    ensure!(
        !parsed.is_absolute(),
        "absolute paths are not allowed: {path}"
    );

    let mut sanitized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(segment) => sanitized.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("unsafe relative path in manifest: {path}");
            }
        }
    }

    ensure!(
        !sanitized.as_os_str().is_empty(),
        "empty relative path in manifest"
    );
    Ok(sanitized)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::*;
    use crate::node::NodeOptions;
    use crate::share::share_directory;

    #[tokio::test(flavor = "multi_thread")]
    async fn downloads_all_files_with_progress_and_nested_directories() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;
        let source_root = source_dir.path().join("share-me");
        std::fs::create_dir_all(source_root.join("nested"))?;

        std::fs::write(source_root.join("alpha.txt"), b"alpha")?;
        std::fs::write(
            source_root.join("nested").join("beta.bin"),
            vec![7_u8; 512 * 1024],
        )?;
        std::fs::write(source_root.join("nested").join("gamma.txt"), b"gamma")?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let mut events = Vec::new();
        let session =
            download_share_with_progress(&node_b, &share.share_code, output_dir.path(), |event| {
                events.push(event)
            })
            .await?;

        assert_eq!(session.directory_name, "share-me");
        assert_eq!(session.output_root, output_dir.path().join("share-me"));
        assert_eq!(session.total_bytes, 5 + 512 * 1024 + 5);
        assert_eq!(session.files.len(), 3);
        assert!(session.files.iter().all(|file| !file.skipped_download));

        for relative_path in ["alpha.txt", "nested/beta.bin", "nested/gamma.txt"] {
            let expected = std::fs::read(source_root.join(relative_path))?;
            let actual = std::fs::read(session.output_root.join(relative_path))?;
            assert_eq!(actual, expected, "mismatch for {relative_path}");
        }

        assert!(session.output_root.join("nested").is_dir());
        assert!(matches!(
            events.first(),
            Some(DownloadEvent::DownloadStarted {
                directory_name,
                total_bytes,
                file_count,
                collection_hash,
            }) if directory_name == "share-me"
                && *total_bytes == 5 + 512 * 1024 + 5
                && *file_count == 3
                && collection_hash == &share.collection_hash.to_string()
        ));
        assert!(matches!(
            events.last(),
            Some(DownloadEvent::TransferCompleted)
        ));

        let mut progress_by_file: HashMap<usize, Vec<u64>> = HashMap::new();
        let mut completed = Vec::new();
        for event in events {
            match event {
                DownloadEvent::FileDownloadProgress {
                    file_index,
                    bytes_downloaded,
                } => progress_by_file
                    .entry(file_index)
                    .or_default()
                    .push(bytes_downloaded),
                DownloadEvent::FileCompleted { file_index } => completed.push(file_index),
                _ => {}
            }
        }

        assert_eq!(completed.len(), 3);
        assert!(
            !progress_by_file.is_empty(),
            "expected at least one byte-level progress event"
        );
        for updates in progress_by_file.values() {
            assert!(
                updates.windows(2).all(|window| window[0] <= window[1]),
                "progress values must be nondecreasing: {updates:?}"
            );
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn skips_preexisting_blobs_without_re_downloading() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;
        let source_root = source_dir.path().join("share-me");
        std::fs::create_dir_all(source_root.join("nested"))?;

        std::fs::write(source_root.join("alpha.txt"), vec![1_u8; 256 * 1024])?;
        std::fs::write(source_root.join("nested").join("beta.bin"), b"beta")?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let preseeded_hash = share.files[0].hash;
        node_b.blobs.download(preseeded_hash).await?;
        assert!(node_b.blobs.has(preseeded_hash).await?);
        mark_blob_completed(node_b_dir.path(), preseeded_hash)?;

        let mut events = Vec::new();
        let session =
            download_share_with_progress(&node_b, &share.share_code, output_dir.path(), |event| {
                events.push(event)
            })
            .await?;

        let skipped_file = session
            .files
            .iter()
            .find(|file| file.hash == preseeded_hash)
            .context("preseeded file missing from session results")?;
        assert!(skipped_file.skipped_download);

        assert!(
            !events.iter().any(|event| matches!(
                event,
                DownloadEvent::FileDownloadProgress { file_index: 0, .. }
            )),
            "skipped file should not emit progress events"
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, DownloadEvent::FileCompleted { file_index: 0 })));

        for relative_path in ["alpha.txt", "nested/beta.bin"] {
            let expected = std::fs::read(source_root.join(relative_path))?;
            let actual = std::fs::read(session.output_root.join(relative_path))?;
            assert_eq!(actual, expected, "mismatch for {relative_path}");
        }

        Ok(())
    }

    #[test]
    fn rejects_unsafe_relative_paths() {
        for path in ["../escape.txt", "/absolute/path", "nested/../../escape.txt"] {
            assert!(sanitize_relative_path(path).is_err(), "{path} should fail");
        }
    }

    #[test]
    fn accepts_safe_relative_paths() {
        assert_eq!(
            sanitize_relative_path("nested/file.txt").unwrap(),
            PathBuf::from("nested/file.txt")
        );
    }
}
