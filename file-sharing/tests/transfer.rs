use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use iroh::test_utils::run_relay_server;
use p2panda_file_sharing_gui::bridge::{AsyncBridge, NetworkCommand, NetworkEvent};
use p2panda_file_sharing_gui::download::{download_share_with_progress, DownloadEvent};
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::share::share_directory;
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr, RelayUrl};
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn local_two_node_directory_transfer() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;

        let source_root = source_dir.path().join("source");
        fs::create_dir_all(source_root.join("nested"))?;

        let small = b"0123456789".repeat(10);
        let medium: Vec<u8> = (0..100 * 1024).map(|idx| (idx % 251) as u8).collect();
        let nested: Vec<u8> = (0..500).map(|idx| (idx % 199) as u8).collect();

        fs::write(source_root.join("small.txt"), &small)?;
        fs::write(source_root.join("medium.bin"), &medium)?;
        fs::write(source_root.join("nested").join("inside.dat"), &nested)?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;
        let endpoint_addr: EndpointAddr = node_a.endpoint.endpoint().await?.addr();

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(endpoint_addr).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let mut events = Vec::new();
        let session =
            download_share_with_progress(&node_b, &share.share_code, output_dir.path(), |event| {
                events.push(event)
            })
            .await?;

        let destination_root = session.output_root.clone();
        compare_tree_bytes(&source_root, &destination_root)?;

        let mut progress_by_file: HashMap<usize, Vec<u64>> = HashMap::new();
        for event in events {
            if let DownloadEvent::FileDownloadProgress {
                file_index,
                bytes_downloaded,
            } = event
            {
                progress_by_file
                    .entry(file_index)
                    .or_default()
                    .push(bytes_downloaded);
            }
        }

        assert!(
            !progress_by_file.is_empty(),
            "expected byte-level progress events"
        );
        for updates in progress_by_file.values() {
            assert!(
                updates.windows(2).all(|pair| pair[0] <= pair[1]),
                "progress values must be nondecreasing: {updates:?}"
            );
        }

        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("local transfer test timed out after 30 seconds")??;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_based_transfer() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;

        let source_root = source_dir.path().join("source");
        fs::create_dir_all(source_root.join("nested"))?;

        let small = b"relay-small".repeat(10);
        let medium: Vec<u8> = (0..32 * 1024).map(|idx| (idx % 251) as u8).collect();
        let nested: Vec<u8> = (0..500).map(|idx| (idx % 199) as u8).collect();

        fs::write(source_root.join("small.txt"), &small)?;
        fs::write(source_root.join("medium.bin"), &medium)?;
        fs::write(source_root.join("nested").join("inside.dat"), &nested)?;

        let opts = NodeOptions {
            relay_url: Some(relay_url.clone()),
            insecure_skip_relay_cert_verify: true,
        };
        let node_a = AppNode::with_data_dir(node_a_dir.path(), opts.clone()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), opts).await?;

        node_b
            .address_book
            .insert_node_info(relay_bootstrap_node_info(node_a.node_id(), relay_url))
            .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let share = share_directory(&node_a, &source_root).await?;
        let session =
            download_share_with_progress(&node_b, &share.share_code, output_dir.path(), |_| {})
                .await?;

        compare_tree_bytes(&source_root, &session.output_root)?;

        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("relay transfer test timed out after 30 seconds")??;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_source_download_from_two_seeders() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let node_c_dir = tempdir()?;
        let source_dir = tempdir()?;
        let node_b_output_dir = tempdir()?;
        let node_c_output_dir = tempdir()?;

        let source_root = source_dir.path().join("source");
        fs::create_dir_all(source_root.join("nested"))?;

        let large: Vec<u8> = (0..1024 * 1024).map(|idx| (idx % 251) as u8).collect();
        let nested: Vec<u8> = (0..500).map(|idx| (idx % 199) as u8).collect();
        fs::write(source_root.join("large.bin"), &large)?;
        fs::write(source_root.join("nested").join("inside.dat"), &nested)?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;
        let node_c = AppNode::with_data_dir(node_c_dir.path(), NodeOptions::default()).await?;

        let node_a_addr: EndpointAddr = node_a.endpoint.endpoint().await?.addr();
        let node_b_addr: EndpointAddr = node_b.endpoint.endpoint().await?.addr();

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a_addr.clone()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let _session_b = download_share_with_progress(
            &node_b,
            &share.share_code,
            node_b_output_dir.path(),
            |_| {},
        )
        .await?;

        node_c
            .address_book
            .insert_node_info(NodeInfo::from(node_a_addr).bootstrap())
            .await?;
        node_c
            .address_book
            .insert_node_info(NodeInfo::from(node_b_addr).bootstrap())
            .await?;

        let known_provider_ids = node_c.address_book.node_ids().await?;
        assert!(known_provider_ids.contains(&node_a.node_id()));
        assert!(known_provider_ids.contains(&node_b.node_id()));

        let session_c = download_share_with_progress(
            &node_c,
            &share.share_code,
            node_c_output_dir.path(),
            |_| {},
        )
        .await?;

        compare_tree_bytes(&source_root, &session_c.output_root)?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("multi-source transfer test timed out after 30 seconds")??;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn removed_share_is_not_downloadable() -> Result<()> {
    timeout(Duration::from_secs(40), async {
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
        let bridge_data_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_b = tempdir()?;

        let source_root = source_dir.path().join("source");
        fs::create_dir_all(&source_root)?;
        fs::write(source_root.join("payload.txt"), b"share then remove")?;

        let bridge = AsyncBridge::spawn_with_data_dir(
            NodeOptions {
                relay_url: Some(relay_url.clone()),
                insecure_skip_relay_cert_verify: true,
            },
            bridge_data_dir.path().to_path_buf(),
        )?;

        bridge.send(NetworkCommand::ShareDirectory {
            transfer_id: 1,
            directory_path: source_root.clone(),
        })?;
        let share_code = wait_for_share_ready(&bridge, 1).await?;

        bridge.send(NetworkCommand::RemoveShare { transfer_id: 1 })?;
        wait_for_transfer_cancelled(&bridge, 1).await?;
        let state_json = fs::read_to_string(bridge_data_dir.path().join("state.json"))?;
        let state: serde_json::Value = serde_json::from_str(&state_json)?;
        let active_shares = state
            .get("active_shares")
            .and_then(|value| value.as_array())
            .context("missing active_shares in state.json")?;
        assert!(
            active_shares.is_empty(),
            "expected removed share to be deleted from state.json"
        );

        let node_b = AppNode::with_data_dir(
            node_b_dir.path(),
            NodeOptions {
                relay_url: Some(relay_url.clone()),
                insecure_skip_relay_cert_verify: true,
            },
        )
        .await?;
        // This path waits for provider-side unavailability to surface through downloader retries.
        // Give it enough headroom to avoid sandbox/network timing flakes.
        let removed_attempt = timeout(
            Duration::from_secs(25),
            download_share_with_progress(&node_b, &share_code, output_b.path(), |_| {}),
        )
        .await
        .context("timed out waiting for removed-share download attempt")?;
        assert!(
            removed_attempt.is_err(),
            "expected removed share to be unavailable"
        );

        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("removed-share transfer test timed out")??;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_one_share_does_not_break_another_share_with_shared_file_blob() -> Result<()> {
    timeout(Duration::from_secs(40), async {
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
        let bridge_data_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_b = tempdir()?;

        let source_a = source_dir.path().join("source-a");
        let source_b = source_dir.path().join("source-b");
        fs::create_dir_all(&source_a)?;
        fs::create_dir_all(&source_b)?;
        let shared_payload = b"shared blob payload";
        fs::write(source_a.join("shared.bin"), shared_payload)?;
        fs::write(source_b.join("shared.bin"), shared_payload)?;
        fs::write(source_a.join("unique-a.txt"), b"only in A")?;
        fs::write(source_b.join("unique-b.txt"), b"only in B")?;

        let bridge = AsyncBridge::spawn_with_data_dir(
            NodeOptions {
                relay_url: Some(relay_url.clone()),
                insecure_skip_relay_cert_verify: true,
            },
            bridge_data_dir.path().to_path_buf(),
        )?;

        bridge.send(NetworkCommand::ShareDirectory {
            transfer_id: 1,
            directory_path: source_a.clone(),
        })?;
        let _share_code_a = wait_for_share_ready(&bridge, 1).await?;

        bridge.send(NetworkCommand::ShareDirectory {
            transfer_id: 2,
            directory_path: source_b.clone(),
        })?;
        let share_code_b = wait_for_share_ready(&bridge, 2).await?;

        bridge.send(NetworkCommand::RemoveShare { transfer_id: 1 })?;
        wait_for_transfer_cancelled(&bridge, 1).await?;

        let node_b = AppNode::with_data_dir(
            node_b_dir.path(),
            NodeOptions {
                relay_url: Some(relay_url.clone()),
                insecure_skip_relay_cert_verify: true,
            },
        )
        .await?;
        let session_b = timeout(
            Duration::from_secs(10),
            download_share_with_progress(&node_b, &share_code_b, output_b.path(), |_| {}),
        )
        .await
        .context("timed out waiting for second share download after first share removal")??;
        compare_tree_bytes(&source_b, &session_b.output_root)?;

        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("shared-blob share-removal test timed out")??;

    Ok(())
}

async fn wait_for_share_ready(bridge: &AsyncBridge, transfer_id: u64) -> Result<String> {
    let started = tokio::time::Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(15) {
            anyhow::bail!("timed out waiting for ShareReady event");
        }

        if let Some(event) = bridge
            .try_recv()
            .map_err(|err| anyhow::anyhow!("bridge receive failed: {err}"))?
        {
            match event {
                NetworkEvent::ShareReady {
                    transfer_id: event_transfer_id,
                    share_code,
                    ..
                } if event_transfer_id == transfer_id => return Ok(share_code),
                NetworkEvent::Error {
                    transfer_id: event_transfer_id,
                    error_message,
                } if event_transfer_id == transfer_id => {
                    anyhow::bail!("share failed: {error_message}");
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_transfer_cancelled(bridge: &AsyncBridge, transfer_id: u64) -> Result<()> {
    let started = tokio::time::Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(15) {
            anyhow::bail!("timed out waiting for TransferCancelled event");
        }

        if let Some(event) = bridge
            .try_recv()
            .map_err(|err| anyhow::anyhow!("bridge receive failed: {err}"))?
        {
            match event {
                NetworkEvent::TransferCancelled {
                    transfer_id: event_transfer_id,
                } if event_transfer_id == transfer_id => return Ok(()),
                NetworkEvent::Error {
                    transfer_id: event_transfer_id,
                    error_message,
                } if event_transfer_id == transfer_id => {
                    anyhow::bail!("cancel failed: {error_message}");
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn relay_bootstrap_node_info(node_id: p2panda_core::PublicKey, relay_url: RelayUrl) -> NodeInfo {
    let endpoint_addr = EndpointAddr::new(from_public_key(node_id)).with_relay_url(relay_url);
    NodeInfo::from(endpoint_addr).bootstrap()
}

fn compare_tree_bytes(source_root: &Path, destination_root: &Path) -> Result<()> {
    for relative in list_relative_files(source_root)? {
        let source_path = source_root.join(&relative);
        let destination_path = destination_root.join(&relative);
        let source = fs::read(&source_path)
            .with_context(|| format!("failed to read source file {}", source_path.display()))?;
        let destination = fs::read(&destination_path).with_context(|| {
            format!(
                "failed to read destination file {}",
                destination_path.display()
            )
        })?;
        assert_eq!(destination, source, "content mismatch for {relative}");
    }

    Ok(())
}

fn list_relative_files(root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    list_relative_files_recursive(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

fn list_relative_files_recursive(
    root: &Path,
    current: &Path,
    files: &mut Vec<String>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            list_relative_files_recursive(root, &path, files)?;
            continue;
        }

        if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .with_context(|| format!("{} is not under {}", path.display(), root.display()))?;
            files.push(path_to_forward_slashes(relative));
        }
    }

    Ok(())
}

fn path_to_forward_slashes(path: &Path) -> String {
    let mut out = PathBuf::new();
    for component in path.components() {
        out.push(component);
    }
    out.to_string_lossy().replace('\\', "/")
}
