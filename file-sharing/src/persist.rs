use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use p2panda_blobs::Hash as BlobHash;
use p2panda_net::gossip::GossipHandle;
use p2panda_net::TopicId;
use serde::{Deserialize, Serialize};

use crate::download::{download_share, DownloadSession};
use crate::node::AppNode;
use crate::protocol::CollectionAnnouncement;
use crate::share::ShareSession;
use crate::share_code::derive_topic;

const STATE_FILE_NAME: &str = "state.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedState {
    #[serde(default)]
    pub active_downloads: Vec<DownloadRecord>,
    #[serde(default)]
    pub active_shares: Vec<ShareRecord>,
    #[serde(default)]
    pub global_paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub share_code: String,
    pub output_dir: PathBuf,
    pub collection_hash: String,
    #[serde(default)]
    pub paused: bool,
}

impl DownloadRecord {
    pub fn new(
        share_code: impl Into<String>,
        output_dir: impl Into<PathBuf>,
        collection_hash: BlobHash,
    ) -> Self {
        Self {
            share_code: share_code.into(),
            output_dir: output_dir.into(),
            collection_hash: collection_hash.to_hex(),
            paused: false,
        }
    }

    pub fn collection_hash(&self) -> Result<BlobHash> {
        self.collection_hash
            .parse()
            .with_context(|| format!("invalid collection hash {}", self.collection_hash))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRecord {
    pub source_dir: PathBuf,
    pub share_code: String,
    pub collection_hash: String,
    #[serde(default)]
    pub directory_name: String,
    #[serde(default)]
    pub file_count: usize,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub paused: bool,
}

impl ShareRecord {
    pub fn new(
        source_dir: impl Into<PathBuf>,
        share_code: impl Into<String>,
        collection_hash: BlobHash,
        directory_name: impl Into<String>,
        file_count: usize,
        total_bytes: u64,
    ) -> Self {
        Self {
            source_dir: source_dir.into(),
            share_code: share_code.into(),
            collection_hash: collection_hash.to_hex(),
            directory_name: directory_name.into(),
            file_count,
            total_bytes,
            paused: false,
        }
    }

    pub fn collection_hash(&self) -> Result<BlobHash> {
        self.collection_hash
            .parse()
            .with_context(|| format!("invalid collection hash {}", self.collection_hash))
    }
}

impl From<&ShareSession> for ShareRecord {
    fn from(value: &ShareSession) -> Self {
        let directory_name = value
            .source_dir
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("Shared directory");
        ShareRecord::new(
            value.source_dir.clone(),
            value.share_code.clone(),
            value.collection_hash,
            directory_name,
            value.file_count(),
            value.total_bytes,
        )
    }
}

pub struct RecoveredShare {
    pub record: ShareRecord,
    pub topic_id: TopicId,
    pub handle: GossipHandle,
}

pub struct RecoveryState {
    pub shares: Vec<RecoveredShare>,
    pub downloads: Vec<DownloadSession>,
}

#[derive(Debug)]
pub struct StateStore {
    path: PathBuf,
    state: PersistedState,
}

impl StateStore {
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Self> {
        let path = state_file_path(data_dir.as_ref());
        let state = load_from_path(&path)?;
        Ok(Self { path, state })
    }

    pub fn state(&self) -> &PersistedState {
        &self.state
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self) -> Result<()> {
        write_atomic(&self.path, &self.state)
    }

    pub fn set_global_paused(&mut self, paused: bool) -> Result<()> {
        if self.state.global_paused != paused {
            self.state.global_paused = paused;
            self.save()?;
        }
        Ok(())
    }

    pub fn add_share(&mut self, record: ShareRecord) -> Result<()> {
        if let Some(existing) = self
            .state
            .active_shares
            .iter_mut()
            .find(|existing| existing.share_code == record.share_code)
        {
            if *existing != record {
                *existing = record;
                self.save()?;
            }
            return Ok(());
        }
        self.state.active_shares.push(record);
        self.save()?;
        Ok(())
    }

    pub fn add_download(&mut self, record: DownloadRecord) -> Result<()> {
        if let Some(existing) = self.state.active_downloads.iter_mut().find(|existing| {
            existing.share_code == record.share_code && existing.output_dir == record.output_dir
        }) {
            if *existing != record {
                *existing = record;
                self.save()?;
            }
            return Ok(());
        }
        self.state.active_downloads.push(record);
        self.save()?;
        Ok(())
    }

    pub fn remove_share_by_code(&mut self, share_code: &str) -> Result<bool> {
        let before = self.state.active_shares.len();
        self.state
            .active_shares
            .retain(|record| record.share_code != share_code);
        let changed = self.state.active_shares.len() != before;
        if changed {
            self.save()?;
        }
        Ok(changed)
    }

    pub fn remove_download_by_code(&mut self, share_code: &str, output_dir: &Path) -> Result<bool> {
        let before = self.state.active_downloads.len();
        self.state
            .active_downloads
            .retain(|record| !(record.share_code == share_code && record.output_dir == output_dir));
        let changed = self.state.active_downloads.len() != before;
        if changed {
            self.save()?;
        }
        Ok(changed)
    }
}

pub fn load_state(data_dir: impl AsRef<Path>) -> Result<PersistedState> {
    load_from_path(&state_file_path(data_dir.as_ref()))
}

pub fn save_state(data_dir: impl AsRef<Path>, state: &PersistedState) -> Result<()> {
    write_atomic(&state_file_path(data_dir.as_ref()), state)
}

pub async fn resume_shares(node: &AppNode, state: &PersistedState) -> Result<Vec<RecoveredShare>> {
    if state.global_paused {
        return Ok(Vec::new());
    }

    let mut recovered = Vec::with_capacity(state.active_shares.len());

    for record in &state.active_shares {
        if record.paused {
            continue;
        }
        recovered.push(resume_share_record(node, record).await?);
    }

    Ok(recovered)
}

pub async fn resume_downloads(
    node: &AppNode,
    state: &PersistedState,
) -> Result<Vec<DownloadSession>> {
    if state.global_paused {
        return Ok(Vec::new());
    }

    let mut resumed = Vec::with_capacity(state.active_downloads.len());

    for record in &state.active_downloads {
        if record.paused {
            continue;
        }
        resumed.push(
            download_share(node, &record.share_code, &record.output_dir)
                .await
                .with_context(|| {
                    format!(
                        "failed to resume download for {} into {}",
                        record.share_code,
                        record.output_dir.display()
                    )
                })?,
        );
    }

    Ok(resumed)
}

pub async fn resume_active_transfers(
    node: &AppNode,
    state: &PersistedState,
) -> Result<RecoveryState> {
    let shares = resume_shares(node, state).await?;
    let downloads = resume_downloads(node, state).await?;

    Ok(RecoveryState { shares, downloads })
}

pub async fn resume_share_record(node: &AppNode, record: &ShareRecord) -> Result<RecoveredShare> {
    let collection_hash = record.collection_hash()?;
    let topic_id = derive_topic(*collection_hash.as_bytes());
    let handle = node
        .join_topic(topic_id)
        .await
        .with_context(|| format!("failed to join topic for {}", record.share_code))?;

    handle
        .publish(CollectionAnnouncement::new(collection_hash).encode())
        .await
        .with_context(|| {
            format!(
                "failed to re-announce collection {}",
                record.collection_hash
            )
        })?;

    Ok(RecoveredShare {
        record: record.clone(),
        topic_id,
        handle,
    })
}

fn state_file_path(data_dir: &Path) -> PathBuf {
    data_dir.join(STATE_FILE_NAME)
}

fn load_from_path(path: &Path) -> Result<PersistedState> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse persisted state at {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(PersistedState::default()),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read persisted state at {}", path.display())),
    }
}

fn write_atomic(path: &Path, state: &PersistedState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    }

    let bytes = serde_json::to_vec_pretty(state).context("failed to serialize persisted state")?;
    let tmp_path = path.with_extension("json.tmp");

    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary state file {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    Ok(())
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
    use crate::share::share_directory;

    #[test]
    fn missing_state_file_loads_default_state() -> Result<()> {
        let dir = tempdir()?;
        let state = load_state(dir.path())?;

        assert!(state.active_shares.is_empty());
        assert!(state.active_downloads.is_empty());

        Ok(())
    }

    #[test]
    fn add_share_and_download_write_state_json() -> Result<()> {
        let dir = tempdir()?;
        let mut store = StateStore::load(dir.path())?;
        let hash: BlobHash = "f627847f3d5ebecf169f2e08e10c22f14e8e3a25f8b73f7f15f2f6f5ddf7c905"
            .parse()
            .unwrap();

        store.add_share(ShareRecord::new(
            dir.path().join("source"),
            "p2p-SHARE",
            hash,
            "source",
            2,
            42,
        ))?;
        store.add_download(DownloadRecord::new(
            "p2p-DOWNLOAD",
            dir.path().join("output"),
            hash,
        ))?;

        let state_path = dir.path().join(STATE_FILE_NAME);
        assert!(state_path.is_file());

        let loaded = load_state(dir.path())?;
        assert_eq!(loaded.active_shares.len(), 1);
        assert_eq!(loaded.active_downloads.len(), 1);
        assert!(!loaded.active_shares[0].paused);
        assert!(!loaded.active_downloads[0].paused);
        assert!(!loaded.global_paused);

        Ok(())
    }

    #[test]
    fn legacy_state_without_share_metadata_fields_loads_with_defaults() -> Result<()> {
        let dir = tempdir()?;
        let state_path = dir.path().join(STATE_FILE_NAME);
        std::fs::write(
            &state_path,
            r#"{
  "active_shares": [
    {
      "source_dir": "/tmp/source",
      "share_code": "p2p-LEGACY",
      "collection_hash": "f627847f3d5ebecf169f2e08e10c22f14e8e3a25f8b73f7f15f2f6f5ddf7c905"
    }
  ],
  "active_downloads": []
}"#,
        )?;

        let state = load_state(dir.path())?;
        assert_eq!(state.active_shares.len(), 1);
        let share = &state.active_shares[0];
        assert_eq!(share.directory_name, "");
        assert_eq!(share.file_count, 0);
        assert_eq!(share.total_bytes, 0);
        assert!(!share.paused);
        assert!(!state.global_paused);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_resumes_incomplete_downloads() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;

        let source_root = source_dir.path().join("share-me");
        std::fs::create_dir_all(source_root.join("nested"))?;
        std::fs::write(source_root.join("alpha.txt"), vec![1_u8; 256 * 1024])?;
        std::fs::write(source_root.join("nested").join("beta.txt"), b"beta")?;

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

        let mut store = StateStore::load(node_b_dir.path())?;
        store.add_download(DownloadRecord::new(
            share.share_code.clone(),
            output_dir.path(),
            share.collection_hash,
        ))?;

        drop(node_b);
        tokio::time::sleep(Duration::from_millis(250)).await;

        let restarted = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;
        restarted
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let state = load_state(node_b_dir.path())?;
        let sessions = resume_downloads(&restarted, &state).await?;

        assert_eq!(sessions.len(), 1);
        assert!(!sessions[0].files.is_empty());

        for relative_path in ["alpha.txt", "nested/beta.txt"] {
            let expected = std::fs::read(source_root.join(relative_path))?;
            let actual = std::fs::read(output_dir.path().join("share-me").join(relative_path))?;
            assert_eq!(actual, expected, "mismatch for {relative_path}");
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_reannounces_shares() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;

        let source_root = source_dir.path().join("share-me");
        std::fs::create_dir_all(&source_root)?;
        let mut file = std::fs::File::create(source_root.join("payload.txt"))?;
        writeln!(file, "hello recovery")?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let collection_hash = share.collection_hash;
        let mut store = StateStore::load(node_a_dir.path())?;
        store.add_share(ShareRecord::from(&share))?;

        drop(share);
        drop(node_a);
        tokio::time::sleep(Duration::from_millis(250)).await;

        let restarted = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        node_b
            .address_book
            .insert_node_info(
                NodeInfo::from(restarted.endpoint.endpoint().await?.addr()).bootstrap(),
            )
            .await?;

        let topic_id = derive_topic(*collection_hash.as_bytes());
        let subscriber = node_b.join_topic(topic_id).await?;
        let mut subscription = subscriber.subscribe();

        let state = load_state(node_a_dir.path())?;
        let recovered = resume_shares(&restarted, &state).await?;
        assert_eq!(recovered.len(), 1);

        let announcement = timeout(Duration::from_secs(10), async {
            while let Some(Ok(bytes)) = subscription.next().await {
                if let Ok(announcement) = CollectionAnnouncement::decode(&bytes) {
                    return Some(announcement);
                }
            }
            None
        })
        .await
        .context("timed out waiting for re-announcement")?
        .context("subscription ended before receiving re-announcement")?;

        assert_eq!(
            announcement.collection_hash,
            state.active_shares[0].collection_hash()?
        );

        drop(recovered);

        Ok(())
    }
}
