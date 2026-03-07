use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::{Body, Extension, Hash, Header, PrivateKey, PublicKey};
use p2panda_net::TopicId;
use p2panda_store::{LogStore, OperationStore};
use p2panda_stream::operation::{ingest_operation, IngestResult};
use p2panda_sync::protocols::Logs;
use p2panda_sync::traits::TopicMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::profile::{load_profile_records_from_path, profile_records_path, ProfileRecord};

const OPERATION_DOMAIN_TOPIC_NAMESPACE: &[u8] = b"p2panda-file-sharing/operation-domain/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainLogKind {
    Profile,
    Shares,
    Contacts,
}

impl DomainLogKind {
    const fn rank(self) -> u8 {
        match self {
            Self::Profile => 0,
            Self::Shares => 1,
            Self::Contacts => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DomainLogId {
    pub profile_id: String,
    pub kind: DomainLogKind,
}

impl DomainLogId {
    pub fn new(profile_id: impl Into<String>, kind: DomainLogKind) -> Self {
        Self {
            profile_id: profile_id.into(),
            kind,
        }
    }

    pub fn all_for_profile(profile_id: &str) -> Vec<Self> {
        [
            DomainLogKind::Profile,
            DomainLogKind::Shares,
            DomainLogKind::Contacts,
        ]
        .into_iter()
        .map(|kind| Self::new(profile_id, kind))
        .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainExtensions {
    pub log_id: DomainLogId,
}

impl Extension<DomainLogId> for DomainExtensions {
    fn extract(header: &Header<Self>) -> Option<DomainLogId> {
        Some(header.extensions.log_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DomainOperation {
    ProfileUpdated {
        profile_id: String,
        display_name: String,
        created_at: u64,
        updated_at: u64,
    },
    SharePublished {
        profile_id: String,
        collection_hash: String,
        share_code: String,
        source_dir: PathBuf,
        recorded_at: u64,
        #[serde(default)]
        source_contact_profile_id: Option<String>,
        #[serde(default)]
        source_contact_display_name: Option<String>,
    },
    ShareRemoved {
        profile_id: String,
        collection_hash: String,
        share_code: String,
        recorded_at: u64,
    },
    ContactFollowChanged {
        profile_id: String,
        followed_profile_id: String,
        recorded_at: u64,
        active: bool,
    },
}

impl DomainOperation {
    fn profile_id(&self) -> &str {
        match self {
            Self::ProfileUpdated { profile_id, .. }
            | Self::SharePublished { profile_id, .. }
            | Self::ShareRemoved { profile_id, .. }
            | Self::ContactFollowChanged { profile_id, .. } => profile_id,
        }
    }

    fn log_kind(&self) -> DomainLogKind {
        match self {
            Self::ProfileUpdated { .. } => DomainLogKind::Profile,
            Self::SharePublished { .. } | Self::ShareRemoved { .. } => DomainLogKind::Shares,
            Self::ContactFollowChanged { .. } => DomainLogKind::Contacts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducedShareState {
    pub profile_id: String,
    pub collection_hash: String,
    pub share_code: String,
    pub source_dir: PathBuf,
    pub recorded_at: u64,
    pub source_contact_profile_id: Option<String>,
    pub source_contact_display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducedProfileState {
    pub profile_id: String,
    pub display_name: Option<String>,
    pub shares: Vec<ReducedShareState>,
    pub followed_profile_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MigrationReport {
    pub profile_operations: usize,
    pub share_operations: usize,
    pub contact_operations: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredDomainOperation {
    pub author: PublicKey,
    pub log_id: DomainLogId,
    pub header: Header<DomainExtensions>,
    pub operation: DomainOperation,
}

#[derive(Clone, Default, Debug)]
pub struct FileSharingTopicMap(Arc<RwLock<TopicMapState>>);

#[derive(Debug, Default)]
struct TopicMapState {
    profiles_by_topic: HashMap<TopicId, String>,
    authors_by_profile: HashMap<String, HashSet<PublicKey>>,
}

impl FileSharingTopicMap {
    pub async fn register_profile(&self, profile_id: &str) -> TopicId {
        let topic = profile_sync_topic(profile_id);
        let mut state = self.0.write().await;
        state.profiles_by_topic.insert(topic, profile_id.to_owned());
        state
            .authors_by_profile
            .entry(profile_id.to_owned())
            .or_default();
        topic
    }

    pub async fn register_profile_author(&self, profile_id: &str, author: PublicKey) -> TopicId {
        let topic = self.register_profile(profile_id).await;
        let mut state = self.0.write().await;
        state
            .authors_by_profile
            .entry(profile_id.to_owned())
            .or_default()
            .insert(author);
        topic
    }

    pub async fn known_authors(&self, profile_id: &str) -> Vec<PublicKey> {
        let state = self.0.read().await;
        let mut authors = state
            .authors_by_profile
            .get(profile_id)
            .into_iter()
            .flat_map(|authors| authors.iter().copied())
            .collect::<Vec<_>>();
        authors.sort_by_key(|author| author.to_string());
        authors
    }
}

impl TopicMap<TopicId, Logs<DomainLogId>> for FileSharingTopicMap {
    type Error = io::Error;

    async fn get(&self, topic_query: &TopicId) -> Result<Logs<DomainLogId>, Self::Error> {
        let state = self.0.read().await;
        let Some(profile_id) = state.profiles_by_topic.get(topic_query) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "unknown file-sharing operation-domain topic",
            ));
        };

        let logs = state
            .authors_by_profile
            .get(profile_id)
            .into_iter()
            .flat_map(|authors| authors.iter().copied())
            .map(|author| (author, DomainLogId::all_for_profile(profile_id)))
            .collect();
        Ok(logs)
    }
}

#[derive(Debug, Clone)]
pub struct FileSharingOperationDomain<S> {
    store: S,
    topic_map: FileSharingTopicMap,
}

impl<S> FileSharingOperationDomain<S>
where
    S: OperationStore<DomainLogId, DomainExtensions> + LogStore<DomainLogId, DomainExtensions>,
{
    pub fn new(store: S, topic_map: FileSharingTopicMap) -> Self {
        Self { store, topic_map }
    }

    pub fn into_store(self) -> S {
        self.store
    }

    pub fn topic_map(&self) -> &FileSharingTopicMap {
        &self.topic_map
    }

    pub async fn append_operation(
        &mut self,
        private_key: &PrivateKey,
        operation: DomainOperation,
    ) -> Result<Header<DomainExtensions>> {
        let profile_id = operation.profile_id().to_owned();
        let log_id = DomainLogId::new(&profile_id, operation.log_kind());
        let body_bytes =
            encode_cbor(&operation).context("failed to encode operation-domain body")?;
        let body = Body::from(body_bytes.clone());
        let latest = self
            .store
            .latest_operation(&private_key.public_key(), &log_id)
            .await
            .map_err(|err| anyhow::anyhow!("failed to load latest domain operation: {err}"))?;
        let (seq_num, backlink) = latest
            .as_ref()
            .map(|(header, _)| (header.seq_num + 1, Some(header.hash())))
            .unwrap_or((0, None));

        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp: now_unix_secs(),
            seq_num,
            backlink,
            previous: backlink.into_iter().collect(),
            extensions: DomainExtensions {
                log_id: log_id.clone(),
            },
        };
        header.sign(private_key);
        let header_bytes = header.to_bytes();

        match ingest_operation(
            &mut self.store,
            header.clone(),
            Some(body),
            header_bytes,
            &log_id,
            false,
        )
        .await
        .map_err(|err| anyhow::anyhow!("failed to ingest domain operation: {err}"))?
        {
            IngestResult::Complete(_) => {
                self.topic_map
                    .register_profile_author(&profile_id, private_key.public_key())
                    .await;
                Ok(header)
            }
            IngestResult::Retry(_, _, _, behind) => {
                bail!("domain operation requires retry and is {behind} entries behind")
            }
            IngestResult::Outdated(_) => bail!("domain operation was treated as outdated"),
        }
    }

    pub async fn append_profile_update(
        &mut self,
        private_key: &PrivateKey,
        profile_id: &str,
        display_name: impl Into<String>,
        created_at: u64,
        updated_at: u64,
    ) -> Result<Header<DomainExtensions>> {
        self.append_operation(
            private_key,
            DomainOperation::ProfileUpdated {
                profile_id: profile_id.to_owned(),
                display_name: display_name.into(),
                created_at,
                updated_at,
            },
        )
        .await
    }

    pub async fn append_share_published(
        &mut self,
        private_key: &PrivateKey,
        profile_id: &str,
        collection_hash: impl Into<String>,
        share_code: impl Into<String>,
        source_dir: impl Into<PathBuf>,
        recorded_at: u64,
        source_contact_profile_id: Option<String>,
        source_contact_display_name: Option<String>,
    ) -> Result<Header<DomainExtensions>> {
        self.append_operation(
            private_key,
            DomainOperation::SharePublished {
                profile_id: profile_id.to_owned(),
                collection_hash: collection_hash.into(),
                share_code: share_code.into(),
                source_dir: source_dir.into(),
                recorded_at,
                source_contact_profile_id,
                source_contact_display_name,
            },
        )
        .await
    }

    pub async fn append_share_removed(
        &mut self,
        private_key: &PrivateKey,
        profile_id: &str,
        collection_hash: impl Into<String>,
        share_code: impl Into<String>,
        recorded_at: u64,
    ) -> Result<Header<DomainExtensions>> {
        self.append_operation(
            private_key,
            DomainOperation::ShareRemoved {
                profile_id: profile_id.to_owned(),
                collection_hash: collection_hash.into(),
                share_code: share_code.into(),
                recorded_at,
            },
        )
        .await
    }

    pub async fn append_contact_follow_changed(
        &mut self,
        private_key: &PrivateKey,
        profile_id: &str,
        followed_profile_id: impl Into<String>,
        recorded_at: u64,
        active: bool,
    ) -> Result<Header<DomainExtensions>> {
        self.append_operation(
            private_key,
            DomainOperation::ContactFollowChanged {
                profile_id: profile_id.to_owned(),
                followed_profile_id: followed_profile_id.into(),
                recorded_at,
                active,
            },
        )
        .await
    }

    pub async fn read_profile_state(
        &self,
        profile_id: &str,
    ) -> Result<Option<ReducedProfileState>> {
        let mut operations = self.operations_for_profile(profile_id).await?;
        if operations.is_empty() {
            return Ok(None);
        }

        operations.sort_by(|left, right| {
            left.header
                .timestamp
                .cmp(&right.header.timestamp)
                .then_with(|| left.log_id.kind.rank().cmp(&right.log_id.kind.rank()))
                .then_with(|| left.author.to_string().cmp(&right.author.to_string()))
                .then_with(|| left.header.seq_num.cmp(&right.header.seq_num))
        });

        let mut display_name = None;
        let mut shares = HashMap::<(String, String), ReducedShareState>::new();
        let mut follows = HashMap::<String, bool>::new();

        for entry in operations {
            match entry.operation {
                DomainOperation::ProfileUpdated {
                    display_name: next_display_name,
                    ..
                } => {
                    display_name = Some(next_display_name);
                }
                DomainOperation::SharePublished {
                    profile_id,
                    collection_hash,
                    share_code,
                    source_dir,
                    recorded_at,
                    source_contact_profile_id,
                    source_contact_display_name,
                } => {
                    shares.insert(
                        (collection_hash.clone(), share_code.clone()),
                        ReducedShareState {
                            profile_id,
                            collection_hash,
                            share_code,
                            source_dir,
                            recorded_at,
                            source_contact_profile_id,
                            source_contact_display_name,
                        },
                    );
                }
                DomainOperation::ShareRemoved {
                    collection_hash,
                    share_code,
                    ..
                } => {
                    shares.remove(&(collection_hash, share_code));
                }
                DomainOperation::ContactFollowChanged {
                    followed_profile_id,
                    active,
                    ..
                } => {
                    follows.insert(followed_profile_id, active);
                }
            }
        }

        let mut shares = shares.into_values().collect::<Vec<_>>();
        shares.sort_by(|left, right| left.share_code.cmp(&right.share_code));

        let mut followed_profile_ids = follows
            .into_iter()
            .filter_map(|(profile_id, active)| active.then_some(profile_id))
            .collect::<Vec<_>>();
        followed_profile_ids.sort();

        Ok(Some(ReducedProfileState {
            profile_id: profile_id.to_owned(),
            display_name,
            shares,
            followed_profile_ids,
        }))
    }

    pub async fn operations_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<StoredDomainOperation>> {
        let mut operations = Vec::new();
        for kind in [
            DomainLogKind::Profile,
            DomainLogKind::Shares,
            DomainLogKind::Contacts,
        ] {
            let log_id = DomainLogId::new(profile_id, kind);
            let log_heights = self
                .store
                .get_log_heights(&log_id)
                .await
                .map_err(|err| anyhow::anyhow!("failed to query domain log heights: {err}"))?;

            for (author, _) in log_heights {
                let entries = self
                    .store
                    .get_log(&author, &log_id, None)
                    .await
                    .map_err(|err| anyhow::anyhow!("failed to load domain log: {err}"))?
                    .unwrap_or_default();

                for (header, body) in entries {
                    let body = body.context("domain operation payload is missing")?;
                    let operation = decode_cbor::<DomainOperation, _>(&body.to_bytes()[..])
                        .context("failed to decode operation-domain body")?;
                    operations.push(StoredDomainOperation {
                        author,
                        log_id: log_id.clone(),
                        header,
                        operation,
                    });
                }
            }
        }

        Ok(operations)
    }

    pub async fn migrate_legacy_profile_snapshot(
        &mut self,
        data_dir: impl AsRef<Path>,
        private_key: &PrivateKey,
    ) -> Result<MigrationReport> {
        let path = profile_records_path(data_dir);
        let records = load_profile_records_from_path(&path).with_context(|| {
            format!(
                "failed to load legacy profile snapshot from {}",
                path.display()
            )
        })?;

        let mut report = MigrationReport::default();
        for record in records {
            match record {
                ProfileRecord::Metadata(record) => {
                    self.append_profile_update(
                        private_key,
                        &record.profile_id,
                        record.display_name,
                        record.created_at,
                        record.updated_at,
                    )
                    .await?;
                    report.profile_operations += 1;
                }
                ProfileRecord::ShareOwnership(record) => {
                    self.append_share_published(
                        private_key,
                        &record.profile_id,
                        record.collection_hash,
                        record.share_code,
                        record.source_dir,
                        record.recorded_at,
                        record.source_contact_profile_id,
                        record.source_contact_display_name,
                    )
                    .await?;
                    report.share_operations += 1;
                }
                ProfileRecord::ContactFollow(record) => {
                    self.append_contact_follow_changed(
                        private_key,
                        &record.profile_id,
                        record.followed_profile_id,
                        record.recorded_at,
                        record.active,
                    )
                    .await?;
                    report.contact_operations += 1;
                }
            }
        }

        Ok(report)
    }
}

fn profile_sync_topic(profile_id: &str) -> TopicId {
    let mut bytes =
        Vec::with_capacity(OPERATION_DOMAIN_TOPIC_NAMESPACE.len() + profile_id.len() + 1);
    bytes.extend_from_slice(OPERATION_DOMAIN_TOPIC_NAMESPACE);
    bytes.push(b'/');
    bytes.extend_from_slice(profile_id.as_bytes());
    Hash::new(&bytes).into()
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use p2panda_blobs::Hash as BlobHash;
    use p2panda_net::LogSync;
    use p2panda_store::{LogStore, MemoryStore};
    use tempfile::tempdir;

    use super::*;
    use crate::node::{AppNode, NodeOptions};
    use crate::persist::ShareRecord;
    use crate::profile::ProfileStore;

    #[tokio::test]
    async fn topic_map_resolves_profile_topic_to_all_domain_logs() -> Result<()> {
        let private_key = PrivateKey::new();
        let profile_id = private_key.public_key().to_string();
        let topic_map = FileSharingTopicMap::default();
        let topic = topic_map
            .register_profile_author(&profile_id, private_key.public_key())
            .await;

        let logs = topic_map.get(&topic).await?;
        assert_eq!(
            logs.get(&private_key.public_key()),
            Some(&DomainLogId::all_for_profile(&profile_id))
        );

        Ok(())
    }

    #[tokio::test]
    async fn reducers_reconstruct_current_state_from_operation_history() -> Result<()> {
        let private_key = PrivateKey::new();
        let profile_id = private_key.public_key().to_string();
        let followed_profile_id = PrivateKey::new().public_key().to_string();

        let topic_map = FileSharingTopicMap::default();
        let mut domain = FileSharingOperationDomain::new(
            MemoryStore::<DomainLogId, DomainExtensions>::new(),
            topic_map.clone(),
        );

        let first_header = domain
            .append_profile_update(&private_key, &profile_id, "Amber Apple", 10, 10)
            .await?;
        let second_header = domain
            .append_profile_update(&private_key, &profile_id, "Velvet Pear", 10, 20)
            .await?;
        domain
            .append_share_published(
                &private_key,
                &profile_id,
                BlobHash::new(b"share-a").to_string(),
                "p2p-A",
                PathBuf::from("/tmp/share-a"),
                30,
                None,
                None,
            )
            .await?;
        domain
            .append_share_published(
                &private_key,
                &profile_id,
                BlobHash::new(b"share-b").to_string(),
                "p2p-B",
                PathBuf::from("/tmp/share-b"),
                31,
                None,
                None,
            )
            .await?;
        domain
            .append_share_removed(
                &private_key,
                &profile_id,
                BlobHash::new(b"share-a").to_string(),
                "p2p-A",
                32,
            )
            .await?;
        domain
            .append_contact_follow_changed(
                &private_key,
                &profile_id,
                &followed_profile_id,
                40,
                true,
            )
            .await?;

        let reduced = domain
            .read_profile_state(&profile_id)
            .await?
            .expect("state should exist");
        assert_eq!(reduced.display_name.as_deref(), Some("Velvet Pear"));
        assert_eq!(
            reduced.followed_profile_ids,
            vec![followed_profile_id.clone()]
        );
        assert_eq!(reduced.shares.len(), 1);
        assert_eq!(reduced.shares[0].share_code, "p2p-B");

        let topic = topic_map
            .register_profile_author(&profile_id, private_key.public_key())
            .await;
        let logs = topic_map.get(&topic).await?;
        assert_eq!(logs.len(), 1);

        let profile_log = domain
            .store
            .get_log(
                &private_key.public_key(),
                &DomainLogId::new(&profile_id, DomainLogKind::Profile),
                None,
            )
            .await?
            .expect("profile log to exist");
        assert_eq!(profile_log.len(), 2);
        assert_eq!(profile_log[0].0.seq_num, 0);
        assert_eq!(profile_log[1].0.seq_num, 1);
        assert_eq!(profile_log[1].0.backlink, Some(first_header.hash()));
        assert_eq!(second_header.backlink, Some(first_header.hash()));

        Ok(())
    }

    #[tokio::test]
    async fn legacy_profile_snapshot_migrates_into_split_domain_logs() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        fs::write(dir.path().join("node.key"), private_key.as_bytes())?;

        let mut legacy_store = ProfileStore::load_or_create(dir.path())?;
        let local_profile_id = legacy_store.profile().profile_id.clone();
        legacy_store.update_display_name("Solar Melon")?;
        let share = ShareRecord::new(
            dir.path().join("shared"),
            "p2p-SHARE",
            BlobHash::new(b"legacy-share"),
            "shared",
            1,
            42,
        )
        .with_owner_profile_id(Some(local_profile_id.clone()));
        legacy_store.ensure_share_ownership_record(&share)?;
        let followed_profile_id = PrivateKey::new().public_key().to_string();
        legacy_store.follow_contact(&followed_profile_id)?;

        let mut domain = FileSharingOperationDomain::new(
            MemoryStore::<DomainLogId, DomainExtensions>::new(),
            FileSharingTopicMap::default(),
        );
        let report = domain
            .migrate_legacy_profile_snapshot(dir.path(), &private_key)
            .await?;

        assert_eq!(report.profile_operations, 2);
        assert_eq!(report.share_operations, 1);
        assert_eq!(report.contact_operations, 1);

        let reduced = domain
            .read_profile_state(&local_profile_id)
            .await?
            .expect("migrated state should exist");
        assert_eq!(reduced.display_name.as_deref(), Some("Solar Melon"));
        assert_eq!(reduced.followed_profile_ids, vec![followed_profile_id]);
        assert_eq!(reduced.shares.len(), 1);
        assert_eq!(reduced.shares[0].share_code, "p2p-SHARE");

        let profile_log = domain
            .store
            .get_log(
                &private_key.public_key(),
                &DomainLogId::new(&local_profile_id, DomainLogKind::Profile),
                None,
            )
            .await?
            .expect("profile log to exist after migration");
        let share_log = domain
            .store
            .get_log(
                &private_key.public_key(),
                &DomainLogId::new(&local_profile_id, DomainLogKind::Shares),
                None,
            )
            .await?
            .expect("share log to exist after migration");
        let contact_log = domain
            .store
            .get_log(
                &private_key.public_key(),
                &DomainLogId::new(&local_profile_id, DomainLogKind::Contacts),
                None,
            )
            .await?
            .expect("contact log to exist after migration");

        assert_eq!(profile_log.len(), 2);
        assert_eq!(share_log.len(), 1);
        assert_eq!(contact_log.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn topic_map_is_used_when_joining_log_sync_topics() -> Result<()> {
        let dir = tempdir()?;
        let node = AppNode::with_data_dir(
            dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let profile_id = node.node_id().to_string();

        let topic_map = FileSharingTopicMap::default();
        let topic = topic_map
            .register_profile_author(&profile_id, node.node_id())
            .await;

        let sync = LogSync::builder(
            MemoryStore::<DomainLogId, DomainExtensions>::new(),
            topic_map.clone(),
            node.endpoint.clone(),
            node.gossip.clone(),
        )
        .spawn()
        .await?;
        let _handle = sync.stream(topic, false).await?;

        assert_eq!(
            topic_map.known_authors(&profile_id).await,
            vec![node.node_id()]
        );

        Ok(())
    }
}
