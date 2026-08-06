use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use p2panda_core::cbor::encode_cbor;
use p2panda_core::Topic;
use p2panda_core::{Body, Operation, SigningKey, VerifyingKey};
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::{EndpointAddr, RelayUrl};
use p2panda_net::sync::{SyncHandle, SyncSubscription};
use p2panda_net::utils::from_verifying_key;
use p2panda_net::LogSync;
use p2panda_store::SqliteStore;
use p2panda_sync::protocols::TopicLogSyncEvent;
use serde::{Deserialize, Serialize};

use crate::contacts::{contact_cache_exists, write_contact_cache, ContactsStore};
use crate::node::AppNode;
use crate::operation_domain::{
    DomainExtensions, DomainLogId, DomainOperation, FileSharingOperationDomain,
    FileSharingTopicMap, ReducedProfileState,
};
use crate::profile::{load_private_key_from_data_dir, load_profile_records, ProfileRecord};
use crate::profile_data::{load_json_key, open_profile_data_store, write_json_key};

const CONTACT_PROFILE_BOOTSTRAP_SETTLE_MILLIS: u64 = 750;
const CONTACT_PROFILE_SYNC_RETRY_ATTEMPTS: usize = 6;
const CONTACT_PROFILE_SYNC_RETRY_INTERVAL_MILLIS: u64 = 1_000;
const LOCAL_PROFILE_SYNC_OPERATIONS_KEY: &str = "local_profile_sync_operations";
const DOMAIN_OPERATION_CACHE_VERSION: u8 = 1;

type DomainStore = SqliteStore;
type DomainSync = LogSync<DomainStore, DomainLogId, DomainExtensions>;
type DomainSyncHandle =
    SyncHandle<Operation<DomainExtensions>, TopicLogSyncEvent<DomainExtensions>>;
type DomainSyncSubscription = SyncSubscription<TopicLogSyncEvent<DomainExtensions>>;

struct ContactProfileSync {
    handle: DomainSyncHandle,
    _task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PersistedDomainOperations {
    version: u8,
    operations: Vec<StoredRawDomainOperation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredRawDomainOperation {
    header: p2panda_core::Header<DomainExtensions>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

pub(crate) struct ProfileSyncService {
    data_dir: PathBuf,
    store: DomainStore,
    topic_map: FileSharingTopicMap,
    domain: FileSharingOperationDomain,
    log_sync: DomainSync,
    local_handle: DomainSyncHandle,
    local_topic: Topic,
    address_book: p2panda_net::AddressBook,
    relay_url: Option<RelayUrl>,
    local_profile_id: String,
    local_private_key: SigningKey,
    local_record_count: usize,
    contact_streams: HashMap<String, ContactProfileSync>,
    _local_task: tokio::task::JoinHandle<()>,
}

impl ProfileSyncService {
    pub(crate) async fn new(node: &AppNode, local_profile_id: impl Into<String>) -> Result<Self> {
        let local_profile_id = local_profile_id.into();
        let local_private_key = load_private_key_from_data_dir(&node.data_dir)?;
        // In-memory SQLite must use a single connection: every further pooled connection would
        // receive its own empty database without the migrated tables.
        let store = p2panda_store::SqliteStoreBuilder::new()
            .max_connections(1)
            .build()
            .await
            .context("failed to open in-memory domain operation store")?;
        let topic_map = FileSharingTopicMap::new(store.clone());
        let mut domain = FileSharingOperationDomain::new(store.clone());
        let local_record_count =
            migrate_local_profile_records(&mut domain, &local_private_key, &node.data_dir).await?;
        load_local_profile_sync_cache(&mut domain, &node.data_dir).await?;

        let topic = topic_map
            .register_profile_author(&local_profile_id, local_private_key.verifying_key())
            .await;
        node.address_book
            .add_topic(node.node_id(), topic)
            .await
            .context("failed to register local profile sync topic")?;

        let log_sync = LogSync::builder(store.clone(), node.endpoint.clone(), node.gossip.clone())
            .spawn()
            .await
            .context("failed to spawn LogSync for profile sync")?;
        let local_handle = log_sync
            .stream(topic, true)
            .await
            .context("failed to join local profile sync topic")?;
        let local_subscription = local_handle
            .subscribe()
            .await
            .context("failed to subscribe to local profile sync topic")?;
        let local_task = spawn_local_profile_task(
            local_subscription,
            store.clone(),
            node.data_dir.clone(),
            local_profile_id.clone(),
            local_private_key.verifying_key(),
        );

        reconcile_local_contacts_projection(&node.data_dir, &domain, &local_profile_id).await?;

        let service = Self {
            data_dir: node.data_dir.clone(),
            store,
            topic_map,
            domain,
            log_sync,
            local_handle,
            local_topic: topic,
            address_book: node.address_book.clone(),
            relay_url: node.relay_url.clone(),
            local_profile_id,
            local_private_key,
            local_record_count,
            contact_streams: HashMap::new(),
            _local_task: local_task,
        };
        let _ = service.sync_local_profile_peers().await;
        Ok(service)
    }

    pub(crate) async fn refresh_local_profile(&mut self) -> Result<()> {
        let records = load_profile_records(&self.data_dir)
            .context("failed to read local profile records from profile store")?;

        if records.len() < self.local_record_count {
            tracing::warn!(
                previous_count = self.local_record_count,
                current_count = records.len(),
                "local profile record count shrank; skipping LogSync refresh"
            );
            self.local_record_count = records.len();
            return Ok(());
        }

        for record in &records[self.local_record_count..] {
            let operation =
                append_profile_record(&mut self.domain, &self.local_private_key, record).await?;
            self.topic_map
                .register_profile_author(&self.local_profile_id, operation.header.verifying_key)
                .await;
            self.local_handle
                .publish(operation)
                .context("failed to publish local profile operation to LogSync live mode")?;
        }

        self.local_record_count = records.len();
        reconcile_local_contacts_projection(&self.data_dir, &self.domain, &self.local_profile_id)
            .await?;
        let _ = self.sync_local_profile_peers().await;
        Ok(())
    }

    pub(crate) async fn sync_followed_contacts_from_disk(&mut self) -> Result<usize> {
        reconcile_local_contacts_projection(&self.data_dir, &self.domain, &self.local_profile_id)
            .await?;
        let _ = self.sync_local_profile_peers().await;
        let contacts = ContactsStore::load(&self.data_dir)
            .context("failed to load contacts for profile sync startup")?;
        let profile_ids = contacts
            .contacts()
            .iter()
            .map(|contact| contact.profile_id.clone())
            .collect::<Vec<_>>();

        let mut started = 0usize;
        for profile_id in profile_ids {
            if self.sync_contact_profile(&profile_id).await? {
                started += 1;
            }
        }
        Ok(started)
    }

    pub(crate) async fn sync_local_profile_peers(&self) -> Result<usize> {
        let node_infos = self
            .address_book
            .node_infos_by_topics([self.local_topic])
            .await
            .context("failed to query local profile topic peers")?;
        let peer_ids = node_infos
            .into_iter()
            .map(|node_info| node_info.node_id)
            .filter(|node_id| *node_id != self.local_private_key.verifying_key())
            .collect::<Vec<_>>();
        if peer_ids.is_empty() {
            return Ok(0);
        }

        if self.relay_url.is_some() {
            tokio::time::sleep(Duration::from_millis(
                CONTACT_PROFILE_BOOTSTRAP_SETTLE_MILLIS,
            ))
            .await;
        }
        for peer_id in &peer_ids {
            self.topic_map
                .register_profile_author(&self.local_profile_id, *peer_id)
                .await;
            self.local_handle.initiate_session(*peer_id);
        }

        Ok(peer_ids.len())
    }

    pub(crate) async fn sync_contact_profile(&mut self, profile_id: &str) -> Result<bool> {
        if profile_id == self.local_profile_id {
            return Ok(false);
        }

        let public_key = normalize_profile_id(profile_id)?;
        self.seed_contact_bootstrap(public_key).await?;

        let topic = self
            .topic_map
            .register_profile_author(profile_id, public_key)
            .await;
        self.address_book
            .add_topic(self.local_private_key.verifying_key(), topic)
            .await
            .with_context(|| {
                format!("failed to register local interest in profile topic for {profile_id}")
            })?;
        self.address_book
            .set_topics(public_key, [topic])
            .await
            .with_context(|| format!("failed to register profile sync topic for {profile_id}"))?;
        wait_for_topic_registration(&self.address_book, topic, public_key, profile_id).await?;

        let mut started = false;
        if !self.contact_streams.contains_key(profile_id) {
            let handle = self
                .log_sync
                .stream(topic, true)
                .await
                .with_context(|| format!("failed to join LogSync topic for {profile_id}"))?;
            let subscription = handle.subscribe().await.with_context(|| {
                format!("failed to subscribe to LogSync topic for {profile_id}")
            })?;
            let task = spawn_contact_profile_task(
                subscription,
                self.store.clone(),
                self.data_dir.clone(),
                profile_id.to_owned(),
            );
            self.contact_streams.insert(
                profile_id.to_owned(),
                ContactProfileSync {
                    handle,
                    _task: task,
                },
            );
            started = true;
        }

        let sync = self
            .contact_streams
            .get(profile_id)
            .expect("contact sync task inserted");
        if started && self.relay_url.is_some() {
            tokio::time::sleep(Duration::from_millis(
                CONTACT_PROFILE_BOOTSTRAP_SETTLE_MILLIS,
            ))
            .await;
        }
        sync.handle.initiate_session(public_key);
        if !contact_cache_exists(&self.data_dir, profile_id)? {
            for _ in 0..CONTACT_PROFILE_SYNC_RETRY_ATTEMPTS {
                if contact_cache_exists(&self.data_dir, profile_id)? {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(
                    CONTACT_PROFILE_SYNC_RETRY_INTERVAL_MILLIS,
                ))
                .await;
                sync.handle.initiate_session(public_key);
            }
        }

        Ok(started)
    }

    async fn seed_contact_bootstrap(&self, public_key: VerifyingKey) -> Result<()> {
        let Some(relay_url) = self.relay_url.clone() else {
            return Ok(());
        };

        let endpoint_addr =
            EndpointAddr::new(from_verifying_key(public_key)).with_relay_url(relay_url);
        self.address_book
            .insert_node_info(NodeInfo::from(endpoint_addr).bootstrap())
            .await
            .with_context(|| format!("failed to seed bootstrap info for {}", public_key))?;
        Ok(())
    }
}

async fn migrate_local_profile_records(
    domain: &mut FileSharingOperationDomain,
    private_key: &SigningKey,
    data_dir: &Path,
) -> Result<usize> {
    let records = load_profile_records(data_dir)
        .context("failed to load local profile records for LogSync migration")?;
    for record in &records {
        append_profile_record(domain, private_key, record).await?;
    }
    Ok(records.len())
}

async fn append_profile_record(
    domain: &mut FileSharingOperationDomain,
    private_key: &SigningKey,
    record: &ProfileRecord,
) -> Result<Operation<DomainExtensions>> {
    let domain_operation = domain_operation_from_profile_record(record);
    let header = domain
        .append_operation(private_key, domain_operation.clone())
        .await?;
    let body = Body::from(
        encode_cbor(&domain_operation)
            .context("failed to encode profile LogSync operation body")?,
    );
    Ok(Operation {
        hash: header.hash(),
        header,
        body: Some(body),
    })
}

fn domain_operation_from_profile_record(record: &ProfileRecord) -> DomainOperation {
    match record {
        ProfileRecord::Metadata(record) => DomainOperation::ProfileUpdated {
            profile_id: record.profile_id.clone(),
            display_name: record.display_name.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
        },
        ProfileRecord::ShareOwnership(record) => {
            if record.active {
                DomainOperation::SharePublished {
                    profile_id: record.profile_id.clone(),
                    collection_hash: record.collection_hash.clone(),
                    share_code: record.share_code.clone(),
                    manifest_bytes: record.manifest_bytes.clone(),
                    source_dir: record.source_dir.clone(),
                    recorded_at: record.recorded_at,
                    source_contact_profile_id: record.source_contact_profile_id.clone(),
                    source_contact_display_name: record.source_contact_display_name.clone(),
                }
            } else {
                DomainOperation::ShareRemoved {
                    profile_id: record.profile_id.clone(),
                    collection_hash: record.collection_hash.clone(),
                    share_code: record.share_code.clone(),
                    recorded_at: record.recorded_at,
                }
            }
        }
        ProfileRecord::ContactFollow(record) => DomainOperation::ContactFollowChanged {
            profile_id: record.profile_id.clone(),
            followed_profile_id: record.followed_profile_id.clone(),
            recorded_at: record.recorded_at,
            active: record.active,
        },
    }
}

fn spawn_contact_profile_task(
    mut subscription: DomainSyncSubscription,
    store: DomainStore,
    data_dir: std::path::PathBuf,
    profile_id: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut domain = FileSharingOperationDomain::new(store);

        while let Some(message) = subscription.next().await {
            let Ok(message) = message else {
                continue;
            };

            match message.event {
                TopicLogSyncEvent::OperationReceived { operation, .. } => {
                    if let Err(err) = domain.ingest_remote_operation(*operation).await {
                        tracing::warn!(
                            remote_profile_id = %profile_id,
                            "failed to ingest LogSync profile operation: {err:#}"
                        );
                        continue;
                    }
                    if let Err(err) = persist_contact_cache(&domain, &data_dir, &profile_id).await {
                        tracing::warn!(
                            remote_profile_id = %profile_id,
                            "failed to persist reduced profile cache from LogSync operation: {err:#}"
                        );
                    }
                }
                TopicLogSyncEvent::SyncFinished { .. }
                | TopicLogSyncEvent::LiveModeStarted
                | TopicLogSyncEvent::SessionFinished { .. } => {
                    if let Err(err) = persist_contact_cache(&domain, &data_dir, &profile_id).await {
                        tracing::warn!(
                            remote_profile_id = %profile_id,
                            "failed to persist reduced profile cache after sync milestone: {err:#}"
                        );
                    }
                }
                TopicLogSyncEvent::Failed { error } => {
                    tracing::warn!(
                        remote_profile_id = %profile_id,
                        "LogSync profile replication failed: {error}"
                    );
                }
                TopicLogSyncEvent::SessionStarted | TopicLogSyncEvent::SyncStarted { .. } => {}
            }
        }
    })
}

fn spawn_local_profile_task(
    mut subscription: DomainSyncSubscription,
    store: DomainStore,
    data_dir: PathBuf,
    profile_id: String,
    local_author: VerifyingKey,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut domain = FileSharingOperationDomain::new(store);

        while let Some(message) = subscription.next().await {
            let Ok(message) = message else {
                continue;
            };

            match message.event {
                TopicLogSyncEvent::OperationReceived { operation, .. } => {
                    if let Err(err) = domain.ingest_remote_operation(*operation).await {
                        tracing::warn!(
                            local_profile_id = %profile_id,
                            "failed to ingest synced local profile operation: {err:#}"
                        );
                        continue;
                    }
                    if let Err(err) = persist_local_profile_sync_state(
                        &domain,
                        &data_dir,
                        &profile_id,
                        local_author,
                    )
                    .await
                    {
                        tracing::warn!(
                            local_profile_id = %profile_id,
                            "failed to persist synced local profile state: {err:#}"
                        );
                    }
                }
                TopicLogSyncEvent::SyncFinished { .. }
                | TopicLogSyncEvent::LiveModeStarted
                | TopicLogSyncEvent::SessionFinished { .. } => {
                    if let Err(err) = persist_local_profile_sync_state(
                        &domain,
                        &data_dir,
                        &profile_id,
                        local_author,
                    )
                    .await
                    {
                        tracing::warn!(
                            local_profile_id = %profile_id,
                            "failed to persist synced local profile state after sync milestone: {err:#}"
                        );
                    }
                }
                TopicLogSyncEvent::Failed { error } => {
                    tracing::warn!(
                        local_profile_id = %profile_id,
                        "LogSync local profile replication failed: {error}"
                    );
                }
                TopicLogSyncEvent::SessionStarted | TopicLogSyncEvent::SyncStarted { .. } => {}
            }
        }
    })
}

async fn persist_contact_cache(
    domain: &FileSharingOperationDomain,
    data_dir: &Path,
    profile_id: &str,
) -> Result<()> {
    let Some(state) = domain.read_profile_state(profile_id).await? else {
        return Ok(());
    };
    persist_contact_profile_state(data_dir, profile_id, &state)?;
    Ok(())
}

async fn persist_local_profile_sync_state(
    domain: &FileSharingOperationDomain,
    data_dir: &Path,
    profile_id: &str,
    local_author: VerifyingKey,
) -> Result<()> {
    let operations = domain
        .operations_for_profile(profile_id)
        .await?
        .into_iter()
        .filter(|entry| entry.author != local_author)
        .map(|entry| {
            let body = Body::from(
                encode_cbor(&entry.operation)
                    .context("failed to encode synced local profile operation body")?,
            );
            Ok(Operation {
                hash: entry.header.hash(),
                header: entry.header,
                body: Some(body),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    write_local_profile_sync_operations(data_dir, operations)?;
    reconcile_local_contacts_projection(data_dir, domain, profile_id).await?;
    Ok(())
}

async fn reconcile_local_contacts_projection(
    data_dir: &Path,
    domain: &FileSharingOperationDomain,
    profile_id: &str,
) -> Result<()> {
    let follows = domain.read_followed_contact_state(profile_id).await?;
    let mut contacts = ContactsStore::load(data_dir)
        .context("failed to load contacts while reconciling local follow projection")?;
    contacts
        .reconcile_followed_contacts(&follows)
        .context("failed to persist contacts projection from local follow operations")?;
    Ok(())
}

async fn load_local_profile_sync_cache(
    domain: &mut FileSharingOperationDomain,
    data_dir: &Path,
) -> Result<()> {
    let operations = load_local_profile_sync_operations(data_dir)
        .context("failed to load cached synced local profile operations")?;
    for operation in operations {
        domain.ingest_remote_operation(operation).await?;
    }
    Ok(())
}

fn persist_contact_profile_state(
    data_dir: &Path,
    profile_id: &str,
    state: &ReducedProfileState,
) -> Result<()> {
    write_contact_cache(data_dir, profile_id, state)
}

fn load_local_profile_sync_operations(data_dir: &Path) -> Result<Vec<Operation<DomainExtensions>>> {
    let store = open_profile_data_store(data_dir)?;
    let Some(persisted): Option<PersistedDomainOperations> =
        load_json_key(&store.pool, LOCAL_PROFILE_SYNC_OPERATIONS_KEY)?
    else {
        return Ok(Vec::new());
    };
    if persisted.version != DOMAIN_OPERATION_CACHE_VERSION {
        anyhow::bail!(
            "unsupported domain operation cache version {}",
            persisted.version
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

fn write_local_profile_sync_operations(
    data_dir: &Path,
    operations: Vec<Operation<DomainExtensions>>,
) -> Result<()> {
    let persisted = PersistedDomainOperations {
        version: DOMAIN_OPERATION_CACHE_VERSION,
        operations: operations
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
            .collect::<Result<Vec<_>>>()?,
    };
    let store = open_profile_data_store(data_dir)?;
    write_json_key(&store.pool, LOCAL_PROFILE_SYNC_OPERATIONS_KEY, &persisted)
}

async fn wait_for_topic_registration(
    address_book: &p2panda_net::AddressBook,
    topic: Topic,
    public_key: VerifyingKey,
    profile_id: &str,
) -> Result<()> {
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                anyhow::bail!("timed out waiting for topic bootstrap registration for {profile_id}");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                let node_infos = address_book
                    .node_infos_by_topics([topic])
                    .await
                    .with_context(|| format!("failed to read topic bootstrap registration for {profile_id}"))?;
                if node_infos.into_iter().any(|info| info.node_id == public_key) {
                    return Ok(());
                }
            }
        }
    }
}

fn normalize_profile_id(profile_id: &str) -> Result<VerifyingKey> {
    profile_id
        .parse()
        .with_context(|| format!("invalid profile ID {profile_id}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use iroh::test_utils::run_relay_server;
    use p2panda_core::test_utils::setup_logging;
    use tempfile::tempdir;

    use super::*;
    use crate::contacts::ContactsStore;
    use crate::node::NodeOptions;
    use crate::persist::ShareRecord;
    use crate::profile::ProfileStore;
    use crate::share::share_directory;

    // Relay-backed sync tests can stall under heavy suite load; keep the timeout generous
    // enough to exercise the real flow without turning intermittent scheduler delays into
    // false negatives.
    const CONTACT_PROFILE_PHASE_TIMEOUT_SECS: u64 = 120;

    #[tokio::test(flavor = "multi_thread")]
    async fn syncs_contact_profile_via_log_sync_with_catch_up_and_live_updates() -> Result<()> {
        setup_logging();
        let test_start = std::time::Instant::now();
        let mut phase_start = test_start;

        let sharer_dir = tempdir()?;
        let follower_dir = tempdir()?;
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
        println!(
            "  relay server started: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );

        let node_options = NodeOptions {
            relay_url: Some(relay_url.clone()),
            mdns_enabled: false,
            insecure_skip_relay_cert_verify: true,
        };
        phase_start = std::time::Instant::now();
        let sharer = AppNode::with_data_dir(sharer_dir.path(), node_options.clone()).await?;
        println!(
            "  sharer node created: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );
        let sharer_profile_id = {
            let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
            sharer_profile.update_display_name("Alice Example")?;
            sharer_profile.profile().profile_id.clone()
        };
        phase_start = std::time::Instant::now();
        let mut sharer_sync = ProfileSyncService::new(&sharer, sharer_profile_id.clone()).await?;
        sharer_sync.refresh_local_profile().await?;
        println!(
            "  sharer sync ready: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );

        phase_start = std::time::Instant::now();
        let follower = AppNode::with_data_dir(follower_dir.path(), node_options.clone()).await?;
        println!(
            "  follower node created: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );
        follower
            .address_book
            .insert_node_info(relay_bootstrap_node_info(
                sharer.node_id(),
                relay_url.clone(),
            ))
            .await?;

        let mut follower_profile = ProfileStore::load_or_create(follower_dir.path())?;
        let follower_profile_id = follower_profile.profile().profile_id.clone();
        follower_profile.follow_contact(sharer_profile_id.clone())?;
        drop(follower_profile);

        let mut follower_contacts = ContactsStore::load(follower_dir.path())?;
        follower_contacts.follow_contact(sharer_profile_id.clone())?;
        follower_contacts.refresh_contact(&sharer_profile_id).ok();
        assert_eq!(
            follower_contacts.get(&sharer_profile_id).unwrap().label(),
            sharer_profile_id.chars().take(8).collect::<String>()
        );

        phase_start = std::time::Instant::now();
        let mut follower_sync =
            ProfileSyncService::new(&follower, follower_profile_id.clone()).await?;
        follower_sync
            .sync_contact_profile(&sharer_profile_id)
            .await?;
        println!(
            "  phase 1 sync_contact_profile: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );

        wait_for_contact_label(follower_dir.path(), &sharer_profile_id, "Alice Example").await?;
        assert!(!follower_dir.path().join("contact-record-cache").exists());
        assert!(!follower_dir.path().join("contacts.json").exists());

        drop(follower_sync);
        drop(follower);
        tokio::time::sleep(Duration::from_millis(250)).await;

        println!("  -- phase 2: reconnect --");
        phase_start = std::time::Instant::now();
        {
            let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
            sharer_profile.update_display_name("Alice Reconnected")?;
        }
        sharer_sync.refresh_local_profile().await?;

        let follower = AppNode::with_data_dir(follower_dir.path(), node_options).await?;
        println!(
            "  follower node recreated: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );
        follower
            .address_book
            .insert_node_info(relay_bootstrap_node_info(sharer.node_id(), relay_url))
            .await?;
        phase_start = std::time::Instant::now();
        let mut follower_sync =
            ProfileSyncService::new(&follower, follower_profile_id.clone()).await?;
        assert_eq!(follower_sync.sync_followed_contacts_from_disk().await?, 1);
        println!(
            "  phase 2 sync_followed_contacts_from_disk: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );
        wait_for_contact_label(follower_dir.path(), &sharer_profile_id, "Alice Reconnected")
            .await?;
        assert!(!follower_dir.path().join("contact-record-cache").exists());
        assert!(!follower_dir
            .path()
            .join("local-profile-sync-cache.json")
            .exists());

        println!("  -- phase 3: live update --");
        phase_start = std::time::Instant::now();
        {
            let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
            sharer_profile.update_display_name("Alice Live")?;
        }
        sharer_sync.refresh_local_profile().await?;
        follower_sync
            .sync_contact_profile(&sharer_profile_id)
            .await?;
        println!(
            "  phase 3 sync_contact_profile: {:.1}s",
            phase_start.elapsed().as_secs_f64()
        );
        wait_for_contact_label(follower_dir.path(), &sharer_profile_id, "Alice Live").await?;

        println!(
            "  total test time: {:.1}s",
            test_start.elapsed().as_secs_f64()
        );
        Ok(())
    }

    #[tokio::test]
    async fn refresh_local_profile_ignores_already_migrated_records() -> Result<()> {
        let dir = tempdir()?;
        let node = AppNode::with_data_dir(
            dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let profile = ProfileStore::load_or_create(dir.path())?;
        let profile_id = profile.profile().profile_id.clone();
        let expected_display_name = profile.profile().display_name.clone();
        drop(profile);

        let mut sync = ProfileSyncService::new(&node, profile_id.clone()).await?;
        sync.refresh_local_profile().await?;

        let reduced = FileSharingOperationDomain::new(sync.store.clone())
            .read_profile_state(&profile_id)
            .await?
            .expect("local profile state to exist");
        assert_eq!(
            reduced.display_name.as_deref(),
            Some(expected_display_name.as_str())
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_start_avoids_legacy_profile_contact_json_files() -> Result<()> {
        let dir = tempdir()?;
        let node = AppNode::with_data_dir(
            dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let profile = ProfileStore::load_or_create(dir.path())?;
        let profile_id = profile.profile().profile_id.clone();
        drop(profile);
        let _contacts = ContactsStore::load(dir.path())?;
        let mut sync = ProfileSyncService::new(&node, profile_id).await?;
        sync.refresh_local_profile().await?;

        assert!(!dir.path().join("profile.json").exists());
        assert!(!dir.path().join("profile-records.json").exists());
        assert!(!dir.path().join("contacts.json").exists());
        assert!(!dir.path().join("contact-record-cache").exists());
        assert!(!dir.path().join("local-profile-sync-cache.json").exists());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_local_profile_rebuilds_contacts_from_follow_operation_history() -> Result<()> {
        let dir = tempdir()?;
        let node = AppNode::with_data_dir(
            dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let mut profile = ProfileStore::load_or_create(dir.path())?;
        let profile_id = profile.profile().profile_id.clone();
        let stale_contact_id = SigningKey::generate().verifying_key().to_string();
        let kept_contact_id = SigningKey::generate().verifying_key().to_string();
        let removed_contact_id = SigningKey::generate().verifying_key().to_string();

        {
            let mut contacts = ContactsStore::load(dir.path())?;
            contacts.follow_contact(stale_contact_id.clone())?;
        }

        profile.follow_contact(kept_contact_id.clone())?;
        profile.follow_contact(removed_contact_id.clone())?;
        profile.unfollow_contact(removed_contact_id.clone())?;
        drop(profile);

        let mut sync = ProfileSyncService::new(&node, profile_id.clone()).await?;
        sync.refresh_local_profile().await?;

        let contacts = ContactsStore::load(dir.path())?;
        let profile_ids = contacts
            .contacts()
            .iter()
            .map(|contact| contact.profile_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(profile_ids, vec![kept_contact_id.as_str()]);

        let reduced = FileSharingOperationDomain::new(sync.store.clone())
            .read_profile_state(&profile_id)
            .await?
            .expect("local profile state to exist");
        assert_eq!(reduced.followed_profile_ids, vec![kept_contact_id]);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn syncs_contact_share_state_via_log_sync_catch_up_and_live_removal() -> Result<()> {
        setup_logging();

        let sharer_dir = tempdir()?;
        let follower_dir = tempdir()?;
        let source_dir = tempdir()?;
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;

        let source_root = source_dir.path().join("shared");
        fs::create_dir_all(&source_root)?;
        fs::write(source_root.join("hello.txt"), b"hello share sync")?;

        let node_options = NodeOptions {
            relay_url: Some(relay_url.clone()),
            mdns_enabled: false,
            insecure_skip_relay_cert_verify: true,
        };
        let sharer = AppNode::with_data_dir(sharer_dir.path(), node_options.clone()).await?;
        let share = share_directory(&sharer, &source_root).await?;
        let sharer_profile_id = {
            let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
            let mut share_record = ShareRecord::from(&share);
            share_record.owner_profile_id = Some(sharer_profile.profile().profile_id.clone());
            sharer_profile.ensure_share_ownership_record(&share_record)?;
            sharer_profile.profile().profile_id.clone()
        };
        let mut sharer_sync = ProfileSyncService::new(&sharer, sharer_profile_id.clone()).await?;
        sharer_sync.refresh_local_profile().await?;

        let follower = AppNode::with_data_dir(follower_dir.path(), node_options.clone()).await?;
        follower
            .address_book
            .insert_node_info(relay_bootstrap_node_info(
                sharer.node_id(),
                relay_url.clone(),
            ))
            .await?;

        let mut follower_profile = ProfileStore::load_or_create(follower_dir.path())?;
        let follower_profile_id = follower_profile.profile().profile_id.clone();
        follower_profile.follow_contact(sharer_profile_id.clone())?;
        drop(follower_profile);

        let mut follower_contacts = ContactsStore::load(follower_dir.path())?;
        follower_contacts.follow_contact(sharer_profile_id.clone())?;

        let mut follower_sync =
            ProfileSyncService::new(&follower, follower_profile_id.clone()).await?;
        follower_sync
            .sync_contact_profile(&sharer_profile_id)
            .await?;
        wait_for_contact_share_count(follower_dir.path(), &sharer_profile_id, 1).await?;

        {
            let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
            let mut share_record = ShareRecord::from(&share);
            share_record.owner_profile_id = Some(sharer_profile.profile().profile_id.clone());
            assert!(sharer_profile.remove_share_ownership_record(&share_record)?);
        }
        sharer_sync.refresh_local_profile().await?;
        wait_for_contact_share_count(follower_dir.path(), &sharer_profile_id, 0).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_profile_devices_converge_contact_graph_via_log_sync() -> Result<()> {
        setup_logging();

        let first_dir = tempdir()?;
        let second_dir = tempdir()?;
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;

        let node_options = NodeOptions {
            relay_url: Some(relay_url.clone()),
            mdns_enabled: false,
            insecure_skip_relay_cert_verify: true,
        };
        let first = AppNode::with_data_dir(first_dir.path(), node_options.clone()).await?;
        let second = AppNode::with_data_dir(second_dir.path(), node_options.clone()).await?;
        first
            .address_book
            .insert_node_info(relay_bootstrap_node_info(
                second.node_id(),
                relay_url.clone(),
            ))
            .await?;
        second
            .address_book
            .insert_node_info(relay_bootstrap_node_info(
                first.node_id(),
                relay_url.clone(),
            ))
            .await?;

        let shared_profile_id = {
            let first_profile = ProfileStore::load_or_create(first_dir.path())?;
            first_profile.profile().profile_id.clone()
        };
        {
            let mut second_profile = ProfileStore::load_or_create(second_dir.path())?;
            second_profile.set_profile_id(shared_profile_id.clone())?;
        }

        let mut first_sync = ProfileSyncService::new(&first, shared_profile_id.clone()).await?;
        let mut second_sync = ProfileSyncService::new(&second, shared_profile_id.clone()).await?;

        wait_for_topic_registration(
            &first_sync.address_book,
            first_sync.local_topic,
            second.node_id(),
            &shared_profile_id,
        )
        .await?;
        wait_for_topic_registration(
            &second_sync.address_book,
            second_sync.local_topic,
            first.node_id(),
            &shared_profile_id,
        )
        .await?;
        first_sync.sync_local_profile_peers().await?;
        second_sync.sync_local_profile_peers().await?;

        let followed_contact_id = SigningKey::generate().verifying_key().to_string();
        {
            let mut first_profile = ProfileStore::load_or_create(first_dir.path())?;
            assert!(first_profile.follow_contact(followed_contact_id.clone())?);
        }
        first_sync.refresh_local_profile().await?;
        first_sync.sync_local_profile_peers().await?;
        second_sync.sync_local_profile_peers().await?;
        wait_for_local_followed_contacts(second_dir.path(), vec![followed_contact_id.clone()])
            .await?;

        {
            let mut second_profile = ProfileStore::load_or_create(second_dir.path())?;
            assert!(second_profile.unfollow_contact(followed_contact_id.clone())?);
        }
        second_sync.refresh_local_profile().await?;
        second_sync.sync_local_profile_peers().await?;
        first_sync.sync_local_profile_peers().await?;
        wait_for_local_followed_contacts(first_dir.path(), Vec::new()).await?;

        Ok(())
    }

    async fn wait_for_contact_label(
        data_dir: &Path,
        profile_id: &str,
        expected_label: &str,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(CONTACT_PROFILE_PHASE_TIMEOUT_SECS),
            async {
                loop {
                    let mut contacts = ContactsStore::load(data_dir)?;
                    contacts.refresh_contact(profile_id).ok();
                    let actual = contacts
                        .get(profile_id)
                        .and_then(|contact| contact.display_name().map(String::from));
                    if actual.as_deref() == Some(expected_label) {
                        println!(
                            "  wait_for_contact_label({expected_label:?}): ok in {:.1}s",
                            start.elapsed().as_secs_f64()
                        );
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        )
        .await
        .context(format!(
            "timed out after {:.1}s waiting for contact label {expected_label:?}",
            start.elapsed().as_secs_f64()
        ))??;
        Ok(())
    }

    async fn wait_for_contact_share_count(
        data_dir: &Path,
        profile_id: &str,
        expected_count: usize,
    ) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(CONTACT_PROFILE_PHASE_TIMEOUT_SECS),
            async {
                loop {
                    let mut contacts = ContactsStore::load(data_dir)?;
                    contacts.refresh_contact(profile_id).ok();
                    if contacts
                        .get(profile_id)
                        .map(|contact| contact.cached_shares.len())
                        == Some(expected_count)
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        )
        .await
        .context("timed out waiting for synced contact share count")??;
        Ok(())
    }

    async fn wait_for_local_followed_contacts(
        data_dir: &Path,
        expected_profile_ids: Vec<String>,
    ) -> Result<()> {
        let mut last_seen = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(CONTACT_PROFILE_PHASE_TIMEOUT_SECS),
            async {
                loop {
                    let contacts = ContactsStore::load(data_dir)?;
                    let actual = contacts
                        .contacts()
                        .iter()
                        .map(|contact| contact.profile_id.clone())
                        .collect::<Vec<_>>();
                    last_seen = actual.clone();
                    if actual == expected_profile_ids {
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        )
        .await
        .with_context(|| {
            format!(
                "timed out waiting for synced local contact projection; last seen = {:?}",
                last_seen
            )
        })??;
        Ok(())
    }

    fn relay_bootstrap_node_info(node_id: VerifyingKey, relay_url: RelayUrl) -> NodeInfo {
        NodeInfo::from(EndpointAddr::new(from_verifying_key(node_id)).with_relay_url(relay_url))
            .bootstrap()
    }
}
