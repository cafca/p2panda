use anyhow::{Context, Result};
use std::path::Path;
use tokio::signal;

use crate::node::FileSharingNode;
use crate::protocol::BlobAnnouncement;

pub async fn run(node: &FileSharingNode, file_path: &Path) -> Result<()> {
    // Print node ID so receivers can use it with --peer.
    let node_id = node.endpoint.node_id();
    tracing::info!("Node ID: {}", node_id);

    // Read file from disk.
    let content = tokio::fs::read(file_path)
        .await
        .context("Failed to read file")?;

    let filename = file_path
        .file_name()
        .context("Invalid file path")?
        .to_string_lossy()
        .to_string();

    tracing::info!("Importing file: {} ({} bytes)", filename, content.len());

    // Import file into blob store and get its content-addressed hash.
    let tag_info = node
        .blobs
        .add_slice(&content)
        .await
        .context("Failed to import blob")?;
    let hash = tag_info.hash;

    tracing::info!("Blob hash: {}", hash.to_hex());

    // Build and publish gossip announcement.
    let announcement = BlobAnnouncement::new(hash, filename.clone());
    let gossip_handle = node.gossip.stream(node.topic_id).await?;
    gossip_handle.publish(announcement.encode()).await?;

    tracing::info!(
        "Announced '{}' (hash {}) on topic",
        filename,
        &hash.to_hex()[..8],
    );
    tracing::info!("Press Ctrl+C to exit (keep running so peers can download)...");

    signal::ctrl_c().await?;
    Ok(())
}
