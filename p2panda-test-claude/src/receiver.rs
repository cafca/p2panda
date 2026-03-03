use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio::signal;

use crate::node::FileSharingNode;
use crate::protocol::BlobAnnouncement;

pub async fn run(node: &FileSharingNode, output_dir: &Path) -> Result<()> {
    // Print node ID so senders can find us.
    let node_id = node.endpoint.node_id();
    tracing::info!("Node ID: {}", node_id);

    tokio::fs::create_dir_all(output_dir)
        .await
        .context("Failed to create output directory")?;

    tracing::info!("Listening for files in: {}", output_dir.display());

    let gossip_handle = node.gossip.stream(node.topic_id).await?;
    let mut subscription = gossip_handle.subscribe();

    loop {
        tokio::select! {
            msg = subscription.next() => {
                match msg {
                    Some(Ok(bytes)) => {
                        match BlobAnnouncement::decode(&bytes) {
                            Ok(ann) => {
                                tracing::info!(
                                    "Received announcement: '{}' hash={}",
                                    ann.filename,
                                    &ann.hash.to_hex()[..8],
                                );

                                // Download blob from known peers.
                                if let Err(e) = node.blobs.download(ann.hash).await {
                                    tracing::warn!("Download failed for {}: {}", ann.filename, e);
                                    continue;
                                }

                                // Sanitize filename to prevent path traversal.
                                let safe_name = Path::new(&ann.filename)
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_string_lossy()
                                    .to_string();

                                if safe_name.is_empty() {
                                    tracing::warn!("Invalid filename, skipping");
                                    continue;
                                }

                                // Read blob bytes and write to disk.
                                match node.blobs.get_bytes(ann.hash).await {
                                    Ok(data) => {
                                        let output_path: PathBuf = output_dir.join(&safe_name);
                                        tokio::fs::write(&output_path, &data)
                                            .await
                                            .context("Failed to write file")?;
                                        tracing::info!(
                                            "Saved: {} ({} bytes) -> {}",
                                            safe_name,
                                            data.len(),
                                            output_path.display(),
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to read blob {}: {}", ann.filename, e);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!("Ignoring unrecognised gossip message: {}", e);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Gossip stream error: {}", e);
                    }
                    None => {
                        tracing::info!("Gossip stream closed");
                        break;
                    }
                }
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Shutting down...");
                break;
            }
        }
    }

    Ok(())
}
