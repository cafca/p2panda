use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use p2panda_core::Hash;
use p2panda_core::PublicKey;
use p2panda_net::addrs::NodeInfo;
use p2panda_net::gossip::GossipHandle;
use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr};
use p2panda_net::{Gossip, TopicId};
use serde::{Deserialize, Serialize};

use crate::contacts::{contact_records_cache_path, ContactsStore};
use crate::node::AppNode;
use crate::profile::profile_records_path;

const PROFILE_SYNC_TOPIC_NAMESPACE: &[u8] = b"p2panda-file-sharing/profile-sync/v1";

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProfileSyncMessage {
    Request {
        requester_profile_id: String,
    },
    Snapshot {
        owner_profile_id: String,
        #[serde(default)]
        target_profile_id: Option<String>,
        #[serde(with = "serde_bytes")]
        records: Vec<u8>,
    },
}

struct ContactProfileSync {
    handle: GossipHandle,
    _task: tokio::task::JoinHandle<()>,
}

pub(crate) struct ProfileSyncService {
    gossip: Gossip,
    local_handle: GossipHandle,
    _local_task: tokio::task::JoinHandle<()>,
    address_book: p2panda_net::AddressBook,
    data_dir: std::path::PathBuf,
    relay_url: Option<p2panda_net::iroh_endpoint::RelayUrl>,
    local_profile_id: String,
    contact_streams: HashMap<String, ContactProfileSync>,
}

impl ProfileSyncService {
    pub(crate) async fn new(node: &AppNode, local_profile_id: impl Into<String>) -> Result<Self> {
        let local_profile_id = local_profile_id.into();
        let topic = profile_sync_topic(&local_profile_id);
        let local_handle = node
            .gossip
            .stream(topic)
            .await
            .context("failed to join local profile sync topic")?;

        let local_task = spawn_local_profile_task(
            local_handle.clone(),
            node.data_dir.clone(),
            local_profile_id.clone(),
        );

        Ok(Self {
            gossip: node.gossip.clone(),
            local_handle,
            _local_task: local_task,
            address_book: node.address_book.clone(),
            data_dir: node.data_dir.clone(),
            relay_url: node.relay_url.clone(),
            local_profile_id,
            contact_streams: HashMap::new(),
        })
    }

    pub(crate) async fn refresh_local_profile(&mut self) -> Result<()> {
        let records = fs::read(profile_records_path(&self.data_dir))
            .context("failed to read local profile records for sync")?;
        publish_snapshot(&self.local_handle, &self.local_profile_id, None, records).await
    }

    pub(crate) async fn sync_followed_contacts_from_disk(&mut self) -> Result<usize> {
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

    pub(crate) async fn sync_contact_profile(&mut self, profile_id: &str) -> Result<bool> {
        if profile_id == self.local_profile_id {
            return Ok(false);
        }

        let public_key = normalize_profile_id(profile_id)?;
        self.seed_contact_bootstrap(public_key).await?;

        let topic = profile_sync_topic(profile_id);
        self.address_book
            .set_topics(public_key, [topic])
            .await
            .with_context(|| format!("failed to register profile sync topic for {profile_id}"))?;

        let mut started = false;
        if !self.contact_streams.contains_key(profile_id) {
            let remote_handle = self
                .join_contact_topic(profile_id)
                .await
                .with_context(|| format!("failed to join profile sync topic for {profile_id}"))?;
            let task = spawn_contact_profile_task(
                remote_handle.clone(),
                self.local_profile_id.clone(),
                profile_id.to_owned(),
                contact_records_cache_path(&self.data_dir, profile_id),
            );
            self.contact_streams.insert(
                profile_id.to_owned(),
                ContactProfileSync {
                    handle: remote_handle,
                    _task: task,
                },
            );
            started = true;
        }

        if let Some(sync) = self.contact_streams.get(profile_id) {
            publish_request(&sync.handle, &self.local_profile_id).await?;
        }

        Ok(started)
    }

    async fn join_contact_topic(&self, profile_id: &str) -> Result<GossipHandle> {
        let topic = profile_sync_topic(profile_id);
        self.gossip
            .stream(topic)
            .await
            .with_context(|| format!("failed to join gossip topic {}", profile_id))
    }

    async fn seed_contact_bootstrap(&self, public_key: PublicKey) -> Result<()> {
        let Some(relay_url) = self.relay_url.clone() else {
            return Ok(());
        };

        let endpoint_addr =
            EndpointAddr::new(from_public_key(public_key)).with_relay_url(relay_url);
        self.address_book
            .insert_node_info(NodeInfo::from(endpoint_addr).bootstrap())
            .await
            .with_context(|| format!("failed to seed bootstrap info for {}", public_key))?;
        Ok(())
    }
}

fn spawn_local_profile_task(
    handle: GossipHandle,
    data_dir: std::path::PathBuf,
    local_profile_id: String,
) -> tokio::task::JoinHandle<()> {
    let mut subscription = handle.subscribe();
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            let Ok(bytes) = message else {
                continue;
            };
            let Ok(message) = decode_message(&bytes) else {
                continue;
            };
            let ProfileSyncMessage::Request {
                requester_profile_id,
            } = message
            else {
                continue;
            };
            if requester_profile_id == local_profile_id {
                continue;
            }

            let records = match fs::read(profile_records_path(&data_dir)) {
                Ok(records) => records,
                Err(err) => {
                    tracing::warn!("failed to read local profile records for sync: {err}");
                    continue;
                }
            };

            if let Err(err) = publish_snapshot(
                &handle,
                &local_profile_id,
                Some(requester_profile_id),
                records,
            )
            .await
            {
                tracing::warn!("failed to publish profile snapshot for sync: {err:#}");
            }
        }
    })
}

fn spawn_contact_profile_task(
    handle: GossipHandle,
    local_profile_id: String,
    remote_profile_id: String,
    cache_path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    let mut subscription = handle.subscribe();
    tokio::spawn(async move {
        let mut request_interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tokio::select! {
                _ = request_interval.tick() => {
                    if let Err(err) = publish_request(&handle, &local_profile_id).await {
                        tracing::warn!(
                            "failed to publish profile sync request for {}: {err:#}",
                            remote_profile_id
                        );
                    }
                }
                maybe_message = subscription.next() => {
                    let Some(message) = maybe_message else {
                        break;
                    };
                    let Ok(bytes) = message else {
                        continue;
                    };
                    let Ok(message) = decode_message(&bytes) else {
                        continue;
                    };
                    let ProfileSyncMessage::Snapshot {
                        owner_profile_id,
                        target_profile_id,
                        records,
                    } = message
                    else {
                        continue;
                    };

                    if owner_profile_id != remote_profile_id {
                        continue;
                    }
                    if let Some(target_profile_id) = target_profile_id.as_deref() {
                        if target_profile_id != local_profile_id {
                            continue;
                        }
                    }

                    if let Err(err) = write_contact_cache(&cache_path, &records) {
                        tracing::warn!(
                            "failed to persist synced profile cache for {}: {err:#}",
                            remote_profile_id
                        );
                    }
                }
            }
        }
    })
}

fn write_contact_cache(path: &Path, records: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create contact cache dir {}", parent.display()))?;
    }
    fs::write(path, records)
        .with_context(|| format!("failed to write contact cache file {}", path.display()))?;
    Ok(())
}

async fn publish_request(handle: &GossipHandle, requester_profile_id: &str) -> Result<()> {
    let bytes = encode_message(&ProfileSyncMessage::Request {
        requester_profile_id: requester_profile_id.to_owned(),
    })?;
    handle
        .publish(bytes)
        .await
        .context("failed to publish profile sync request")?;
    Ok(())
}

async fn publish_snapshot(
    handle: &GossipHandle,
    owner_profile_id: &str,
    target_profile_id: Option<String>,
    records: Vec<u8>,
) -> Result<()> {
    let bytes = encode_message(&ProfileSyncMessage::Snapshot {
        owner_profile_id: owner_profile_id.to_owned(),
        target_profile_id,
        records,
    })?;
    handle
        .publish(bytes)
        .await
        .context("failed to publish profile sync snapshot")?;
    Ok(())
}

fn encode_message(message: &ProfileSyncMessage) -> Result<Vec<u8>> {
    p2panda_core::cbor::encode_cbor(message).context("failed to encode profile sync message")
}

fn decode_message(bytes: &[u8]) -> Result<ProfileSyncMessage> {
    p2panda_core::cbor::decode_cbor(bytes).context("failed to decode profile sync message")
}

fn profile_sync_topic(profile_id: &str) -> TopicId {
    let mut topic_seed =
        Vec::with_capacity(PROFILE_SYNC_TOPIC_NAMESPACE.len() + 1 + profile_id.len());
    topic_seed.extend_from_slice(PROFILE_SYNC_TOPIC_NAMESPACE);
    topic_seed.push(b':');
    topic_seed.extend_from_slice(profile_id.as_bytes());
    Hash::new(&topic_seed).into()
}

fn normalize_profile_id(profile_id: &str) -> Result<PublicKey> {
    profile_id
        .parse()
        .with_context(|| format!("invalid profile ID {profile_id}"))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use iroh::test_utils::run_relay_server;
    use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr, RelayUrl};
    use tempfile::tempdir;
    use tokio::time::{sleep, timeout, Duration};

    use super::*;
    use crate::contacts::ContactsStore;
    use crate::node::NodeOptions;
    use crate::profile::{load_profile_records_from_path, ProfileStore};

    #[ignore = "profile sync over gossip is nondeterministic in the current sandbox"]
    #[tokio::test(flavor = "multi_thread")]
    async fn syncs_contact_profile_and_tracks_display_name_updates() -> Result<()> {
        let sharer_dir = tempdir()?;
        let follower_dir = tempdir()?;
        let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;

        let node_options = NodeOptions {
            relay_url: Some(relay_url.clone()),
            mdns_enabled: false,
            insecure_skip_relay_cert_verify: false,
        };
        let sharer = AppNode::with_data_dir(sharer_dir.path(), node_options.clone()).await?;
        let follower = AppNode::with_data_dir(follower_dir.path(), node_options).await?;
        follower
            .address_book
            .insert_node_info(relay_bootstrap_node_info(sharer.node_id(), relay_url))
            .await?;

        let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
        sharer_profile.update_display_name("Alice Example")?;
        let mut sharer_sync =
            ProfileSyncService::new(&sharer, sharer_profile.profile().profile_id.clone()).await?;
        sharer_sync.refresh_local_profile().await?;

        let follower_profile = ProfileStore::load_or_create(follower_dir.path())?;
        let mut follower_sync =
            ProfileSyncService::new(&follower, follower_profile.profile().profile_id.clone())
                .await?;

        let sharer_profile_id = sharer_profile.profile().profile_id.clone();
        let mut contacts = ContactsStore::load(follower_dir.path())?;
        contacts.follow_contact(sharer_profile_id.clone())?;
        assert_eq!(
            contacts.get(&sharer_profile_id).unwrap().label(),
            sharer_profile_id.chars().take(8).collect::<String>()
        );

        follower_sync
            .sync_contact_profile(&sharer_profile_id)
            .await?;
        timeout(Duration::from_secs(10), async {
            loop {
                let cache_path =
                    contact_records_cache_path(follower_dir.path(), &sharer_profile_id);
                if load_profile_records_from_path(&cache_path).is_ok() {
                    break Ok::<(), anyhow::Error>(());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;

        contacts.refresh_contact(&sharer_profile_id)?;
        assert_eq!(
            contacts.get(&sharer_profile_id).unwrap().display_name(),
            Some("Alice Example")
        );

        sharer_profile.update_display_name("Alice Updated")?;
        sharer_sync.refresh_local_profile().await?;

        timeout(Duration::from_secs(10), async {
            loop {
                contacts.refresh_contact(&sharer_profile_id)?;
                if contacts.get(&sharer_profile_id).unwrap().display_name() == Some("Alice Updated")
                {
                    break Ok::<(), anyhow::Error>(());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;

        assert_eq!(
            contacts.get(&sharer_profile_id).unwrap().label(),
            "Alice Updated"
        );
        Ok(())
    }

    fn relay_bootstrap_node_info(node_id: PublicKey, relay_url: RelayUrl) -> NodeInfo {
        let endpoint_addr = EndpointAddr::new(from_public_key(node_id)).with_relay_url(relay_url);
        NodeInfo::from(endpoint_addr).bootstrap()
    }
}
