use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use p2panda_file_sharing_gui::download::{download_share_with_progress, DownloadEvent};
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::share::{publish_share_metadata, share_directory};
use p2panda_net::addrs::NodeInfo;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn resumes_after_crash_without_redownloading_completed_file() -> Result<()> {
    timeout(Duration::from_secs(60), async {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let first_output_dir = tempdir()?;
        let second_output_dir = tempdir()?;

        let source_root = source_dir.path().join("source");
        fs::create_dir_all(source_root.join("nested"))?;

        let first_file = b"first file payload".repeat(8);
        let second_file: Vec<u8> = (0..1024 * 1024).map(|idx| (idx % 251) as u8).collect();
        let third_file: Vec<u8> = (0..768 * 1024).map(|idx| (idx % 199) as u8).collect();

        fs::write(source_root.join("a-first.txt"), first_file)?;
        fs::write(source_root.join("b-large.bin"), second_file)?;
        fs::write(source_root.join("nested").join("c-large.bin"), third_file)?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let _publisher = publish_share_metadata(&node_a, &share).await?;
        let blocked_parent = first_output_dir.path().join("source").join("nested");
        fs::create_dir_all(blocked_parent.parent().context("missing parent")?)?;
        fs::write(&blocked_parent, b"block nested dir creation")?;

        let mut interrupted_events = Vec::new();
        let first_run = download_share_with_progress(
            &node_b,
            &share.share_code,
            first_output_dir.path(),
            |event| interrupted_events.push(event),
        )
        .await;
        assert!(
            first_run.is_err(),
            "first run should fail due to blocked nested path"
        );
        assert!(
            interrupted_events
                .iter()
                .any(|event| matches!(event, DownloadEvent::FileCompleted { file_index: 0 })),
            "first run should complete file index 0 before interruption"
        );

        drop(node_b);

        // Keep sharer running and recreate downloader with the same data directory.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let node_b_restarted = timeout(
            Duration::from_secs(5),
            AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()),
        )
        .await
        .context("timed out reopening crashed node data directory")??;
        node_b_restarted
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let mut resumed_events = Vec::new();
        let resumed_session = timeout(
            Duration::from_secs(30),
            download_share_with_progress(
                &node_b_restarted,
                &share.share_code,
                second_output_dir.path(),
                |event| resumed_events.push(event),
            ),
        )
        .await
        .context("timed out while resuming interrupted download")??;

        compare_tree_bytes(&source_root, &resumed_session.output_root)?;

        let completed_files: HashSet<usize> = resumed_events
            .iter()
            .filter_map(|event| match event {
                DownloadEvent::FileCompleted { file_index } => Some(*file_index),
                _ => None,
            })
            .collect();
        assert_eq!(completed_files, HashSet::from([0, 1, 2]));

        assert!(
            !resumed_events.iter().any(|event| matches!(
                event,
                DownloadEvent::FileDownloadProgress { file_index: 0, .. }
            )),
            "skipped file should not emit progress events during resumed run"
        );

        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("crash recovery test timed out after 60 seconds")??;

    Ok(())
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
