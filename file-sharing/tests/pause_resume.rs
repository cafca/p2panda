use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use iroh::test_utils::run_relay_server;
use p2panda_file_sharing_gui::bridge::{AsyncBridge, NetworkCommand, NetworkEvent};
use p2panda_file_sharing_gui::download::download_share_with_progress;
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::share::share_directory;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn pausing_one_share_does_not_affect_another_and_resumed_share_downloads() -> Result<()> {
    let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
    let opts = NodeOptions {
        relay_url: Some(relay_url.clone()),
        insecure_skip_relay_cert_verify: true,
    };

    let bridge_dir = tempdir()?;
    let bridge = AsyncBridge::spawn_with_data_dir(opts.clone(), bridge_dir.path().to_path_buf())?;
    let downloader_dir = tempdir()?;
    let downloader = AppNode::with_data_dir(downloader_dir.path(), opts).await?;
    let output_a = tempdir()?;
    let output_b = tempdir()?;

    let source_one = tempdir()?;
    let source_two = tempdir()?;
    let share_one_dir = source_one.path().join("alpha");
    let share_two_dir = source_two.path().join("beta");
    fs::create_dir_all(&share_one_dir)?;
    fs::create_dir_all(&share_two_dir)?;
    fs::write(share_one_dir.join("a.txt"), b"alpha")?;
    fs::write(share_two_dir.join("b.txt"), b"beta")?;

    let share_one = start_share_and_wait(&bridge, 1, &share_one_dir).await?;
    let share_two = start_share_and_wait(&bridge, 2, &share_two_dir).await?;

    bridge.send(NetworkCommand::PauseTransfer { transfer_id: 1 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferPaused { transfer_id: 1 })
    })
    .await?;

    let paused_download_attempt =
        download_share_with_progress(&downloader, &share_one, output_a.path(), |_| {}).await;
    assert!(
        paused_download_attempt.is_err(),
        "paused share should not be downloadable"
    );

    let downloaded_share_two =
        download_share_with_progress(&downloader, &share_two, output_b.path(), |_| {}).await;
    assert!(
        downloaded_share_two.is_ok(),
        "pausing share 1 should not impact share 2"
    );

    bridge.send(NetworkCommand::ResumeTransfer { transfer_id: 1 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferResumed { transfer_id: 1 })
    })
    .await?;

    let resumed_download_attempt =
        download_share_with_progress(&downloader, &share_one, output_a.path(), |_| {}).await;
    assert!(
        resumed_download_attempt.is_ok(),
        "resumed share should become downloadable again"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn global_pause_respects_individual_flags_and_toggle_during_pause() -> Result<()> {
    let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
    let opts = NodeOptions {
        relay_url: Some(relay_url.clone()),
        insecure_skip_relay_cert_verify: true,
    };

    let bridge_dir = tempdir()?;
    let bridge = AsyncBridge::spawn_with_data_dir(opts.clone(), bridge_dir.path().to_path_buf())?;
    let downloader_dir = tempdir()?;
    let downloader = AppNode::with_data_dir(downloader_dir.path(), opts).await?;
    let output_a = tempdir()?;
    let output_b = tempdir()?;

    let source_one = tempdir()?;
    let source_two = tempdir()?;
    let share_one_dir = source_one.path().join("global-alpha");
    let share_two_dir = source_two.path().join("global-beta");
    fs::create_dir_all(&share_one_dir)?;
    fs::create_dir_all(&share_two_dir)?;
    fs::write(share_one_dir.join("a.txt"), b"alpha")?;
    fs::write(share_two_dir.join("b.txt"), b"beta")?;

    let share_one = start_share_and_wait(&bridge, 101, &share_one_dir).await?;
    let share_two = start_share_and_wait(&bridge, 102, &share_two_dir).await?;

    bridge.send(NetworkCommand::PauseTransfer { transfer_id: 102 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferPaused { transfer_id: 102 })
    })
    .await?;

    bridge.send(NetworkCommand::PauseAll)?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::GlobalPauseChanged { paused: true })
    })
    .await?;

    // Phase 1: without changing per-share flags during global pause, share #102 stays paused.
    bridge.send(NetworkCommand::ResumeAll)?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::GlobalPauseChanged { paused: false })
    })
    .await?;

    let share_one_download =
        download_share_with_progress(&downloader, &share_one, output_a.path(), |_| {}).await;
    assert!(
        share_one_download.is_ok(),
        "active share should resume after ResumeAll"
    );

    let share_two_download =
        download_share_with_progress(&downloader, &share_two, output_b.path(), |_| {}).await;
    assert!(
        share_two_download.is_err(),
        "individually paused share should remain paused after ResumeAll"
    );

    bridge.send(NetworkCommand::PauseAll)?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::GlobalPauseChanged { paused: true })
    })
    .await?;

    bridge.send(NetworkCommand::ResumeTransfer { transfer_id: 102 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferResumed { transfer_id: 102 })
    })
    .await?;

    let share_two_while_global_paused =
        download_share_with_progress(&downloader, &share_two, output_b.path(), |_| {}).await;
    assert!(
        share_two_while_global_paused.is_err(),
        "global pause should still block share while per-share flag is toggled"
    );

    // Phase 2: after toggling per-share resume while globally paused, ResumeAll should resume #102.
    bridge.send(NetworkCommand::ResumeAll)?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::GlobalPauseChanged { paused: false })
    })
    .await?;

    let share_two_after_toggle =
        download_share_with_progress(&downloader, &share_two, output_b.path(), |_| {}).await;
    assert!(
        share_two_after_toggle.is_ok(),
        "per-share toggle during global pause should apply after ResumeAll"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_share_state_survives_bridge_restart() -> Result<()> {
    let bridge_dir = tempdir()?;
    let source = tempdir()?;
    let share_dir = source.path().join("paused-share");
    fs::create_dir_all(&share_dir)?;
    fs::write(share_dir.join("payload.txt"), b"persist me")?;

    let bridge =
        AsyncBridge::spawn_with_data_dir(NodeOptions::default(), bridge_dir.path().to_path_buf())?;
    let share_code = start_share_and_wait(&bridge, 9, &share_dir).await?;
    bridge.send(NetworkCommand::PauseTransfer { transfer_id: 9 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferPaused { transfer_id: 9 })
    })
    .await?;
    drop(bridge);

    tokio::time::sleep(Duration::from_millis(250)).await;

    let restarted =
        AsyncBridge::spawn_with_data_dir(NodeOptions::default(), bridge_dir.path().to_path_buf())?;
    let mut recovered_share_transfer = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(event) = restarted.try_recv().unwrap() {
            match event {
                NetworkEvent::ShareReady {
                    transfer_id,
                    share_code: recovered_code,
                    ..
                } if recovered_code == share_code => {
                    recovered_share_transfer = Some(transfer_id);
                }
                NetworkEvent::TransferPaused { transfer_id } => {
                    if Some(transfer_id) == recovered_share_transfer {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    bail!("did not observe paused recovered share after restart");
}

#[tokio::test(flavor = "multi_thread")]
async fn global_pause_state_survives_bridge_restart() -> Result<()> {
    let bridge_dir = tempdir()?;
    let source = tempdir()?;
    let share_dir = source.path().join("global-paused-share");
    fs::create_dir_all(&share_dir)?;
    fs::write(share_dir.join("payload.txt"), b"persist global pause")?;

    let bridge =
        AsyncBridge::spawn_with_data_dir(NodeOptions::default(), bridge_dir.path().to_path_buf())?;
    let share_code = start_share_and_wait(&bridge, 201, &share_dir).await?;

    bridge.send(NetworkCommand::PauseAll)?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::GlobalPauseChanged { paused: true })
    })
    .await?;
    drop(bridge);
    tokio::time::sleep(Duration::from_millis(250)).await;

    let restarted =
        AsyncBridge::spawn_with_data_dir(NodeOptions::default(), bridge_dir.path().to_path_buf())?;
    let mut saw_global_pause = false;
    let mut saw_share_paused = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(event) = restarted.try_recv().unwrap() {
            match event {
                NetworkEvent::GlobalPauseChanged { paused: true } => saw_global_pause = true,
                NetworkEvent::ShareReady {
                    share_code: code, ..
                } if code == share_code => {}
                NetworkEvent::TransferPaused { .. } => saw_share_paused = true,
                _ => {}
            }
        }
        if saw_global_pause && saw_share_paused {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    bail!("did not observe globally paused recovery state after restart");
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_download_resumes_without_re_downloading_completed_files() -> Result<()> {
    let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
    let opts = NodeOptions {
        relay_url: Some(relay_url.clone()),
        insecure_skip_relay_cert_verify: true,
    };

    let sharer_dir = tempdir()?;
    let source = tempdir()?;
    let output = tempdir()?;
    let bridge_dir = tempdir()?;

    let source_root = source.path().join("download-source");
    fs::create_dir_all(source_root.join("nested"))?;
    fs::write(source_root.join("a-first.txt"), b"first")?;
    fs::write(
        source_root.join("b-large.bin"),
        (0..2 * 1024 * 1024)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>(),
    )?;
    fs::write(
        source_root.join("nested").join("c-large.bin"),
        (0..1024 * 1024)
            .map(|idx| (idx % 199) as u8)
            .collect::<Vec<_>>(),
    )?;

    let sharer = AppNode::with_data_dir(sharer_dir.path(), opts.clone()).await?;
    let share = share_directory(&sharer, &source_root).await?;
    let bridge = AsyncBridge::spawn_with_data_dir(opts, bridge_dir.path().to_path_buf())?;

    bridge.send(NetworkCommand::StartDownload {
        transfer_id: 77,
        share_code: share.share_code.clone(),
        output_directory: output.path().to_path_buf(),
    })?;

    let mut events = Vec::new();
    loop {
        let event = recv_event(&bridge, Duration::from_secs(30)).await?;
        let first_completed = matches!(
            event,
            NetworkEvent::FileCompleted {
                transfer_id: 77,
                file_index: 0
            }
        );
        events.push(event);
        if first_completed {
            break;
        }
    }

    bridge.send(NetworkCommand::PauseTransfer { transfer_id: 77 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferPaused { transfer_id: 77 })
    })
    .await?;

    bridge.send(NetworkCommand::ResumeTransfer { transfer_id: 77 })?;
    expect_event(&bridge, Duration::from_secs(5), |event| {
        matches!(event, NetworkEvent::TransferResumed { transfer_id: 77 })
    })
    .await?;

    let resume_marker = events.len();
    loop {
        let event = recv_event(&bridge, Duration::from_secs(30)).await?;
        let done = matches!(event, NetworkEvent::TransferCompleted { transfer_id: 77 });
        events.push(event);
        if done {
            break;
        }
    }

    let post_resume = &events[resume_marker..];
    assert!(
        !post_resume.iter().any(|event| matches!(
            event,
            NetworkEvent::FileDownloadProgress {
                transfer_id: 77,
                file_index: 0,
                ..
            }
        )),
        "file 0 emitted progress after resume: {post_resume:?}"
    );

    compare_tree_bytes(&source_root, &output.path().join("download-source"))?;
    Ok(())
}

async fn start_share_and_wait(
    bridge: &AsyncBridge,
    transfer_id: u64,
    path: &Path,
) -> Result<String> {
    bridge.send(NetworkCommand::ShareDirectory {
        transfer_id,
        directory_path: path.to_path_buf(),
    })?;

    let event = expect_event(bridge, Duration::from_secs(10), |event| {
        matches!(event, NetworkEvent::ShareReady { transfer_id: id, .. } if *id == transfer_id)
    })
    .await?;

    let NetworkEvent::ShareReady { share_code, .. } = event else {
        bail!("expected ShareReady event");
    };

    Ok(share_code)
}

async fn recv_event(bridge: &AsyncBridge, timeout: Duration) -> Result<NetworkEvent> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(event) = bridge
            .try_recv()
            .context("bridge event channel disconnected")?
        {
            return Ok(event);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("timed out waiting for bridge event")
}

async fn expect_event<F>(
    bridge: &AsyncBridge,
    timeout: Duration,
    predicate: F,
) -> Result<NetworkEvent>
where
    F: Fn(&NetworkEvent) -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(event) = bridge
            .try_recv()
            .context("bridge event channel disconnected")?
        {
            if predicate(&event) {
                return Ok(event);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("timed out waiting for expected bridge event")
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
