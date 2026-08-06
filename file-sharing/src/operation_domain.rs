use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::timestamp::HybridTimestamp;
use p2panda_core::Topic;
use p2panda_core::{Body, Extension, Hash, Header, Operation, SigningKey, VerifyingKey};
use p2panda_store::logs::LogStore;
use p2panda_store::topics::TopicStore;
use p2panda_store::{SqliteError, SqliteStore, Transaction};
use p2panda_stream::ingest::ingest_operation;
use serde::{Deserialize, Serialize};
use tracing::warn;

const OPERATION_DOMAIN_TOPIC_NAMESPACE: &[u8] = b"p2panda-file-sharing/operation-domain/v1";
const REDUCED_PROFILE_STATE_VERSION: u8 = 1;
const DOMAIN_OPERATION_CACHE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    #[serde(default = "HybridTimestamp::now")]
    pub ordering_timestamp: HybridTimestamp,
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
        #[serde(default, with = "serde_bytes")]
        manifest_bytes: Vec<u8>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducedShareState {
    pub profile_id: String,
    pub collection_hash: String,
    pub share_code: String,
    pub manifest_bytes: Vec<u8>,
    pub source_dir: PathBuf,
    pub recorded_at: u64,
    pub source_contact_profile_id: Option<String>,
    pub source_contact_display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducedProfileState {
    pub profile_id: String,
    pub display_name: Option<String>,
    pub shares: Vec<ReducedShareState>,
    pub followed_profile_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducedContactFollowState {
    pub followed_profile_id: String,
    pub recorded_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharePublication {
    pub collection_hash: String,
    pub share_code: String,
    pub manifest_bytes: Vec<u8>,
    pub source_dir: PathBuf,
    pub recorded_at: u64,
    pub source_contact_profile_id: Option<String>,
    pub source_contact_display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredDomainOperation {
    pub author: VerifyingKey,
    pub log_id: DomainLogId,
    pub header: Header<DomainExtensions>,
    pub operation: DomainOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedReducedProfileState {
    version: u8,
    state: ReducedProfileState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PersistedDomainOperations {
    version: u8,
    operations: Vec<StoredRawDomainOperation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredRawDomainOperation {
    header: Header<DomainExtensions>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

/// Profile-oriented view onto the store's topic associations.
///
/// Topic resolution during sync is handled by the store itself (via the `TopicStore` trait); this
/// wrapper only offers profile-level helpers for registering authors and listing known ones.
#[derive(Clone, Debug)]
pub struct FileSharingTopicMap {
    store: SqliteStore,
}

impl FileSharingTopicMap {
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }

    pub async fn register_profile(&self, profile_id: &str) -> Topic {
        profile_sync_topic(profile_id)
    }

    pub async fn register_profile_author(&self, profile_id: &str, author: VerifyingKey) -> Topic {
        let topic = profile_sync_topic(profile_id);
        if let Err(err) = self
            .associate_profile_logs(profile_id, &topic, &author)
            .await
        {
            warn!("failed to associate domain logs with topic: {err}");
        }
        topic
    }

    /// Associates all domain logs of a profile with its sync topic in one store transaction.
    async fn associate_profile_logs(
        &self,
        profile_id: &str,
        topic: &Topic,
        author: &VerifyingKey,
    ) -> Result<(), SqliteError> {
        let permit = self.store.begin().await?;
        for log_id in DomainLogId::all_for_profile(profile_id) {
            if let Err(err) = TopicStore::<Topic, VerifyingKey, DomainLogId>::associate(
                &self.store,
                topic,
                author,
                &log_id,
            )
            .await
            {
                self.store.rollback(permit).await?;
                return Err(err);
            }
        }
        self.store.commit(permit).await?;
        Ok(())
    }

    pub async fn known_authors(&self, profile_id: &str) -> Vec<VerifyingKey> {
        let topic = profile_sync_topic(profile_id);
        let associations =
            TopicStore::<Topic, VerifyingKey, DomainLogId>::resolve(&self.store, &topic)
                .await
                .unwrap_or_default();
        let mut authors = associations.into_keys().collect::<Vec<_>>();
        authors.sort_by_key(|author| author.to_string());
        authors
    }
}

#[derive(Debug, Clone)]
pub struct FileSharingOperationDomain {
    store: SqliteStore,
}

impl FileSharingOperationDomain {
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> SqliteStore {
        self.store
    }

    pub fn topic_map(&self) -> FileSharingTopicMap {
        FileSharingTopicMap::new(self.store.clone())
    }

    pub async fn append_operation(
        &mut self,
        private_key: &SigningKey,
        operation: DomainOperation,
    ) -> Result<Header<DomainExtensions>> {
        let profile_id = operation.profile_id().to_owned();
        let log_id = DomainLogId::new(&profile_id, operation.log_kind());
        let body_bytes =
            encode_cbor(&operation).context("failed to encode operation-domain body")?;
        let body = Body::from(body_bytes.clone());
        let latest: Option<Operation<DomainExtensions>> = self
            .store
            .get_latest_entry(&private_key.verifying_key(), &log_id)
            .await
            .map_err(|err| anyhow::anyhow!("failed to load latest domain operation: {err}"))?;
        let (seq_num, backlink) = latest
            .as_ref()
            .map(|operation| (operation.header.seq_num + 1, Some(operation.hash)))
            .unwrap_or((0, None));

        // Chain the ordering timestamp off the newest operation known for this profile (from any
        // author and device) so new operations always sort after everything they were created in
        // response to, even when wall clocks are skewed or frozen.
        let previous_ordering = self
            .operations_for_profile(&profile_id)
            .await?
            .into_iter()
            .map(|operation| operation.header.extensions.ordering_timestamp)
            .max();
        let ordering_timestamp = next_ordering_timestamp(previous_ordering);
        let mut header = Header {
            version: 1,
            verifying_key: private_key.verifying_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            seq_num,
            backlink,
            extensions: DomainExtensions {
                log_id: log_id.clone(),
                ordering_timestamp,
            },
        };
        header.sign(private_key);

        let operation = Operation {
            hash: header.hash(),
            header: header.clone(),
            body: Some(body),
        };
        let topic = profile_sync_topic(&profile_id);
        ingest_operation(&self.store, &operation, &log_id, &topic, false)
            .await
            .map_err(|err| anyhow::anyhow!("failed to ingest domain operation: {err}"))?;

        Ok(header)
    }

    pub async fn ingest_remote_operation(
        &mut self,
        operation: Operation<DomainExtensions>,
    ) -> Result<()> {
        let log_id = operation.header.extensions.log_id.clone();
        let topic = profile_sync_topic(&log_id.profile_id);

        ingest_operation(&self.store, &operation, &log_id, &topic, false)
            .await
            .map_err(|err| {
                anyhow::anyhow!("failed to ingest replicated domain operation: {err}")
            })?;

        Ok(())
    }

    pub async fn append_profile_update(
        &mut self,
        private_key: &SigningKey,
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
        private_key: &SigningKey,
        profile_id: &str,
        share: SharePublication,
    ) -> Result<Header<DomainExtensions>> {
        self.append_operation(
            private_key,
            DomainOperation::SharePublished {
                profile_id: profile_id.to_owned(),
                collection_hash: share.collection_hash,
                share_code: share.share_code,
                manifest_bytes: share.manifest_bytes,
                source_dir: share.source_dir,
                recorded_at: share.recorded_at,
                source_contact_profile_id: share.source_contact_profile_id,
                source_contact_display_name: share.source_contact_display_name,
            },
        )
        .await
    }

    pub async fn append_share_removed(
        &mut self,
        private_key: &SigningKey,
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
        private_key: &SigningKey,
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
                .extensions
                .ordering_timestamp
                .cmp(&right.header.extensions.ordering_timestamp)
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
                    manifest_bytes,
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
                            manifest_bytes,
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

    pub async fn read_followed_contact_state(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ReducedContactFollowState>> {
        let mut operations = self.operations_for_profile(profile_id).await?;
        operations.retain(|entry| {
            matches!(
                entry.operation,
                DomainOperation::ContactFollowChanged { .. }
            )
        });
        operations.sort_by(|left, right| {
            left.header
                .extensions
                .ordering_timestamp
                .cmp(&right.header.extensions.ordering_timestamp)
                .then_with(|| left.log_id.kind.rank().cmp(&right.log_id.kind.rank()))
                .then_with(|| left.author.to_string().cmp(&right.author.to_string()))
                .then_with(|| left.header.seq_num.cmp(&right.header.seq_num))
        });

        let mut follows = HashMap::<String, ReducedContactFollowState>::new();
        for entry in operations {
            let DomainOperation::ContactFollowChanged {
                followed_profile_id,
                recorded_at,
                active,
                ..
            } = entry.operation
            else {
                continue;
            };

            if active {
                follows.insert(
                    followed_profile_id.clone(),
                    ReducedContactFollowState {
                        followed_profile_id,
                        recorded_at,
                    },
                );
            } else {
                follows.remove(&followed_profile_id);
            }
        }

        let mut follows = follows.into_values().collect::<Vec<_>>();
        follows.sort_by(|left, right| {
            left.recorded_at
                .cmp(&right.recorded_at)
                .then_with(|| left.followed_profile_id.cmp(&right.followed_profile_id))
        });
        Ok(follows)
    }

    pub async fn operations_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<StoredDomainOperation>> {
        let topic = profile_sync_topic(profile_id);
        let associations =
            TopicStore::<Topic, VerifyingKey, DomainLogId>::resolve(&self.store, &topic)
                .await
                .map_err(|err| {
                    anyhow::anyhow!("failed to resolve domain topic associations: {err}")
                })?;

        let mut operations = Vec::new();
        for (author, log_ids) in associations {
            for log_id in log_ids {
                let entries = self
                    .store
                    .get_log_entries(&author, &log_id, None, None)
                    .await
                    .map_err(|err| anyhow::anyhow!("failed to load domain log: {err}"))?
                    .unwrap_or_default();

                for (operation, _header_bytes) in entries {
                    let operation: Operation<DomainExtensions> = operation;
                    let body = operation
                        .body
                        .context("domain operation payload is missing")?;
                    let domain_operation = decode_cbor::<DomainOperation, _>(&body.to_bytes()[..])
                        .context("failed to decode operation-domain body")?;
                    operations.push(StoredDomainOperation {
                        author,
                        log_id: log_id.clone(),
                        header: operation.header,
                        operation: domain_operation,
                    });
                }
            }
        }

        Ok(operations)
    }
}

fn profile_sync_topic(profile_id: &str) -> Topic {
    let mut bytes =
        Vec::with_capacity(OPERATION_DOMAIN_TOPIC_NAMESPACE.len() + profile_id.len() + 1);
    bytes.extend_from_slice(OPERATION_DOMAIN_TOPIC_NAMESPACE);
    bytes.push(b'/');
    bytes.extend_from_slice(profile_id.as_bytes());
    Hash::digest(&bytes).into()
}

/// Returns a hybrid timestamp strictly after `previous` (when given) and never behind the local
/// wall clock.
///
/// Unlike `HybridTimestamp::increment` this never moves backwards when the local clock lags
/// behind a previously observed timestamp; the logical clock component is bumped instead.
fn next_ordering_timestamp(previous: Option<HybridTimestamp>) -> HybridTimestamp {
    let now = HybridTimestamp::now();
    match previous {
        Some(previous) if now <= previous => {
            let (wall, logical) = previous.to_parts();
            HybridTimestamp::from_parts(wall, logical.increment())
        }
        _ => now,
    }
}

pub fn write_reduced_profile_state_to_path(
    path: impl AsRef<Path>,
    state: &ReducedProfileState,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for reduced profile cache at {}",
                parent.display()
            )
        })?;
    }

    let persisted = PersistedReducedProfileState {
        version: REDUCED_PROFILE_STATE_VERSION,
        state: state.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted)
        .context("failed to serialize reduced profile cache")?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary reduced profile cache {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move reduced profile cache {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

pub fn load_reduced_profile_state_from_path(path: impl AsRef<Path>) -> Result<ReducedProfileState> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read reduced profile cache {}", path.display()))?;
    let persisted: PersistedReducedProfileState = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse reduced profile cache {}", path.display()))?;
    if persisted.version != REDUCED_PROFILE_STATE_VERSION {
        anyhow::bail!(
            "unsupported reduced profile cache version {} in {}",
            persisted.version,
            path.display()
        );
    }
    Ok(persisted.state)
}

pub fn load_raw_domain_operations_from_path(
    path: impl AsRef<Path>,
) -> Result<Vec<Operation<DomainExtensions>>> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read domain operation cache {}", path.display()))?;
    let persisted: PersistedDomainOperations = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse domain operation cache {}", path.display()))?;
    if persisted.version != DOMAIN_OPERATION_CACHE_VERSION {
        anyhow::bail!(
            "unsupported domain operation cache version {} in {}",
            persisted.version,
            path.display()
        );
    }
    persisted
        .operations
        .into_iter()
        .map(|operation| {
            let validated = Operation {
                hash: operation.header.hash(),
                header: operation.header,
                body: Some(Body::from(operation.body)),
            };
            p2panda_core::validate_operation(&validated)
                .context("cached domain operation validation failed")?;
            Ok(validated)
        })
        .collect()
}

pub fn write_raw_domain_operations_to_path(
    path: impl AsRef<Path>,
    operations: impl IntoIterator<Item = Operation<DomainExtensions>>,
) -> Result<()> {
    let operations = operations
        .into_iter()
        .map(|operation| {
            let body = operation
                .body
                .context("domain operation is missing a body while persisting cache")?;
            Ok(StoredRawDomainOperation {
                header: operation.header,
                body: body.to_bytes(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let persisted = PersistedDomainOperations {
        version: DOMAIN_OPERATION_CACHE_VERSION,
        operations,
    };
    write_json_atomic(path.as_ref(), &persisted, "domain operation cache")
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, label: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for {} at {}",
                label,
                parent.display()
            )
        })?;
    }

    let bytes =
        serde_json::to_vec_pretty(value).with_context(|| format!("failed to serialize {label}"))?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary {} file {}",
            label,
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move temporary {} file {} to {}",
            label,
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use p2panda_blobs::Hash as BlobHash;
    use p2panda_net::LogSync;
    use tempfile::tempdir;

    use super::*;
    use crate::node::{AppNode, NodeOptions};

    #[tokio::test]
    async fn topic_map_resolves_profile_topic_to_all_domain_logs() -> Result<()> {
        let private_key = SigningKey::generate();
        let profile_id = private_key.verifying_key().to_string();
        let store = SqliteStore::temporary().await;
        let topic_map = FileSharingTopicMap::new(store.clone());
        let topic = topic_map
            .register_profile_author(&profile_id, private_key.verifying_key())
            .await;

        let logs = TopicStore::<Topic, VerifyingKey, DomainLogId>::resolve(&store, &topic).await?;
        let mut resolved = logs
            .get(&private_key.verifying_key())
            .cloned()
            .unwrap_or_default();
        resolved.sort();
        let mut expected = DomainLogId::all_for_profile(&profile_id);
        expected.sort();
        assert_eq!(resolved, expected);

        Ok(())
    }

    #[tokio::test]
    async fn reducers_reconstruct_current_state_from_operation_history() -> Result<()> {
        let private_key = SigningKey::generate();
        let profile_id = private_key.verifying_key().to_string();
        let followed_profile_id = SigningKey::generate().verifying_key().to_string();

        let store = SqliteStore::temporary().await;
        let topic_map = FileSharingTopicMap::new(store.clone());
        let mut domain = FileSharingOperationDomain::new(store);

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
                SharePublication {
                    collection_hash: BlobHash::new(b"share-a").to_string(),
                    share_code: "p2p-A".into(),
                    manifest_bytes: b"manifest-a".to_vec(),
                    source_dir: PathBuf::from("/tmp/share-a"),
                    recorded_at: 30,
                    source_contact_profile_id: None,
                    source_contact_display_name: None,
                },
            )
            .await?;
        domain
            .append_share_published(
                &private_key,
                &profile_id,
                SharePublication {
                    collection_hash: BlobHash::new(b"share-b").to_string(),
                    share_code: "p2p-B".into(),
                    manifest_bytes: b"manifest-b".to_vec(),
                    source_dir: PathBuf::from("/tmp/share-b"),
                    recorded_at: 31,
                    source_contact_profile_id: None,
                    source_contact_display_name: None,
                },
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

        assert_eq!(
            topic_map.known_authors(&profile_id).await,
            vec![private_key.verifying_key()]
        );

        let profile_log: Vec<(Operation<DomainExtensions>, Vec<u8>)> = domain
            .store
            .get_log_entries(
                &private_key.verifying_key(),
                &DomainLogId::new(&profile_id, DomainLogKind::Profile),
                None,
                None,
            )
            .await?
            .expect("profile log to exist");
        assert_eq!(profile_log.len(), 2);
        assert_eq!(profile_log[0].0.header.seq_num, 0);
        assert_eq!(profile_log[1].0.header.seq_num, 1);
        assert_eq!(profile_log[1].0.header.backlink, Some(first_header.hash()));
        assert_eq!(second_header.backlink, Some(first_header.hash()));

        Ok(())
    }

    #[tokio::test]
    async fn followed_contact_reducer_tracks_latest_active_records() -> Result<()> {
        let private_key = SigningKey::generate();
        let profile_id = private_key.verifying_key().to_string();
        let first_followed_profile_id = SigningKey::generate().verifying_key().to_string();
        let second_followed_profile_id = SigningKey::generate().verifying_key().to_string();

        let mut domain = FileSharingOperationDomain::new(SqliteStore::temporary().await);

        domain
            .append_contact_follow_changed(
                &private_key,
                &profile_id,
                &first_followed_profile_id,
                10,
                true,
            )
            .await?;
        domain
            .append_contact_follow_changed(
                &private_key,
                &profile_id,
                &second_followed_profile_id,
                20,
                true,
            )
            .await?;
        domain
            .append_contact_follow_changed(
                &private_key,
                &profile_id,
                &first_followed_profile_id,
                30,
                false,
            )
            .await?;

        assert_eq!(
            domain.read_followed_contact_state(&profile_id).await?,
            vec![ReducedContactFollowState {
                followed_profile_id: second_followed_profile_id,
                recorded_at: 20,
            }]
        );

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

        let store = SqliteStore::temporary().await;
        let topic_map = FileSharingTopicMap::new(store.clone());
        let topic = topic_map
            .register_profile_author(&profile_id, node.node_id())
            .await;

        let sync: LogSync<SqliteStore, DomainLogId, DomainExtensions> =
            LogSync::builder(store, node.endpoint.clone(), node.gossip.clone())
                .spawn()
                .await?;
        let _handle = sync.stream(topic, false).await?;

        assert_eq!(
            topic_map.known_authors(&profile_id).await,
            vec![node.node_id()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn ingest_remote_operation_is_idempotent_for_duplicate_delivery() -> Result<()> {
        let private_key = SigningKey::generate();
        let profile_id = private_key.verifying_key().to_string();
        let operation = DomainOperation::ProfileUpdated {
            profile_id: profile_id.clone(),
            display_name: "Stable Name".to_owned(),
            created_at: 10,
            updated_at: 20,
        };

        let mut source_domain = FileSharingOperationDomain::new(SqliteStore::temporary().await);
        let header = source_domain
            .append_operation(&private_key, operation.clone())
            .await?;
        let replicated_operation = Operation {
            hash: header.hash(),
            header,
            body: Some(Body::from(encode_cbor(&operation)?)),
        };

        let mut target_domain = FileSharingOperationDomain::new(SqliteStore::temporary().await);
        target_domain
            .ingest_remote_operation(replicated_operation.clone())
            .await?;
        target_domain
            .ingest_remote_operation(replicated_operation)
            .await?;

        let operations = target_domain.operations_for_profile(&profile_id).await?;
        assert_eq!(operations.len(), 1);
        assert_eq!(
            target_domain
                .read_profile_state(&profile_id)
                .await?
                .expect("reduced profile state to exist")
                .display_name
                .as_deref(),
            Some("Stable Name")
        );

        Ok(())
    }
}
