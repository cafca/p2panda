use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use p2panda_file_sharing_gui::download::{download_share_with_progress, DownloadEvent};
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::share::share_directory;
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::EndpointAddr;
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
