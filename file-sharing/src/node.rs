use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use p2panda_blobs::{Blobs, FsStore};
use p2panda_core::identity::SIGNING_KEY_LEN;
use p2panda_core::SigningKey;
use p2panda_core::Topic;
use p2panda_net::iroh_endpoint::RelayUrl;
use p2panda_net::iroh_mdns::MdnsDiscoveryMode;
use p2panda_net::supervisor::SupervisorEvent;
use p2panda_net::{AddressBook, Discovery, Endpoint, Gossip, MdnsDiscovery, Supervisor};
use tokio::sync::broadcast;

const APP_NAME: &str = "p2panda-file-sharing";
const NODE_KEY_FILE: &str = "node.key";
const BLOBS_DIR: &str = "blobs";

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub relay_url: Option<RelayUrl>,
    pub mdns_enabled: bool,
    pub insecure_skip_relay_cert_verify: bool,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            relay_url: None,
            mdns_enabled: true,
            insecure_skip_relay_cert_verify: false,
        }
    }
}

pub struct AppNode {
    pub blobs: Blobs,
    pub gossip: Gossip,
    pub discovery: Discovery,
    pub endpoint: Endpoint,
    pub address_book: AddressBook,
    pub relay_url: Option<RelayUrl>,
    pub data_dir: PathBuf,
    _fs_store: FsStore,
    _mdns: Option<MdnsDiscovery>,
    _supervisor: Supervisor,
}

impl AppNode {
    pub async fn new(opts: NodeOptions) -> Result<Self> {
        let data_dir = app_data_dir()?;
        Self::with_data_dir(data_dir, opts).await
    }

    pub async fn with_data_dir(data_dir: impl Into<PathBuf>, opts: NodeOptions) -> Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir).with_context(|| {
            format!(
                "failed to create app data directory at {}",
                data_dir.display()
            )
        })?;

        let private_key = load_or_create_private_key(&data_dir)?;
        let supervisor = Supervisor::builder().spawn().await?;
        let address_book = AddressBook::builder().spawn_linked(&supervisor).await?;

        let mut endpoint_builder = Endpoint::builder(address_book.clone()).signing_key(private_key);

        if let Some(relay_url) = opts.relay_url.clone() {
            endpoint_builder = endpoint_builder.relay_url(relay_url);
        }

        if opts.insecure_skip_relay_cert_verify {
            endpoint_builder = endpoint_builder.insecure_skip_relay_cert_verify(true);
        }

        let endpoint = endpoint_builder.spawn_linked(&supervisor).await?;

        let mdns = if opts.mdns_enabled {
            Some(
                MdnsDiscovery::builder(address_book.clone(), endpoint.clone())
                    .mode(MdnsDiscoveryMode::Active)
                    .spawn_linked(&supervisor)
                    .await?,
            )
        } else {
            None
        };

        let discovery = Discovery::builder(address_book.clone(), endpoint.clone())
            .spawn_linked(&supervisor)
            .await?;

        let gossip = Gossip::builder(address_book.clone(), endpoint.clone())
            .spawn_linked(&supervisor)
            .await?;

        let blobs_dir = data_dir.join(BLOBS_DIR);
        let fs_store = FsStore::load(&blobs_dir).await.with_context(|| {
            format!(
                "failed to load blob store from {}",
                blobs_dir.as_path().display()
            )
        })?;
        let blobs = Blobs::new(&fs_store, &endpoint, &address_book).await?;

        Ok(Self {
            blobs,
            gossip,
            discovery,
            endpoint,
            address_book,
            relay_url: opts.relay_url,
            data_dir,
            _fs_store: fs_store,
            _mdns: mdns,
            _supervisor: supervisor,
        })
    }

    pub fn node_id(&self) -> p2panda_core::VerifyingKey {
        self.endpoint.node_id()
    }

    pub async fn join_topic(&self, topic: Topic) -> Result<p2panda_net::gossip::GossipHandle> {
        Ok(self.gossip.stream(topic).await?)
    }

    pub async fn supervisor_events(&self) -> Result<broadcast::Receiver<SupervisorEvent>> {
        Ok(self._supervisor.events().await?)
    }
}

pub type FileSharingNode = AppNode;

fn app_data_dir() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("", "", APP_NAME)
        .ok_or_else(|| anyhow::anyhow!("failed to resolve app data directory for {APP_NAME}"))?;
    Ok(project_dirs.data_dir().to_path_buf())
}

fn load_or_create_private_key(data_dir: &Path) -> Result<SigningKey> {
    let key_path = data_dir.join(NODE_KEY_FILE);

    match fs::read(&key_path) {
        Ok(bytes) => private_key_from_bytes(&key_path, &bytes),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let private_key = SigningKey::generate();
            fs::write(&key_path, private_key.as_bytes()).with_context(|| {
                format!("failed to write node private key to {}", key_path.display())
            })?;
            Ok(private_key)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to read node private key from {}",
                key_path.display()
            )
        }),
    }
}

fn private_key_from_bytes(key_path: &Path, bytes: &[u8]) -> Result<SigningKey> {
    let key_bytes: [u8; SIGNING_KEY_LEN] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid private key length in {}: expected {} bytes, got {}",
            key_path.display(),
            SIGNING_KEY_LEN,
            bytes.len()
        )
    })?;

    Ok(SigningKey::from_bytes(&key_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use p2panda_core::Hash;
    use tempfile::tempdir;
    use tokio::time::{sleep, timeout, Duration, Instant};

    #[tokio::test]
    async fn reuses_private_key_for_same_data_dir() -> Result<()> {
        let temp_dir = tempdir()?;

        let node_a = AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()).await?;
        let node_a_id = node_a.node_id();

        assert!(temp_dir.path().join(NODE_KEY_FILE).is_file());
        assert!(temp_dir.path().join(BLOBS_DIR).exists());

        drop(node_a);

        // Endpoint and store shutdown happen asynchronously after the last handle drops.
        // Give teardown a short window before reopening the same data directory.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let node_b = timeout(
            Duration::from_secs(5),
            AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()),
        )
        .await
        .context("timed out reopening the same node data directory")??;

        assert_eq!(node_a_id, node_b.node_id());

        Ok(())
    }

    #[tokio::test]
    async fn supports_multiple_gossip_topics() -> Result<()> {
        let temp_dir = tempdir()?;
        let node = AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()).await?;

        let topic_a: Topic = Hash::digest(b"topic-a").into();
        let topic_b: Topic = Hash::digest(b"topic-b").into();

        let _handle_a = node.join_topic(topic_a).await?;
        let _handle_b = node.join_topic(topic_b).await?;

        Ok(())
    }

    #[tokio::test]
    async fn can_start_with_mdns_disabled() -> Result<()> {
        let temp_dir = tempdir()?;
        let node = AppNode::with_data_dir(
            temp_dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;

        assert!(node._mdns.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn supervised_discovery_restarts_after_failure() -> Result<()> {
        let temp_dir = tempdir()?;
        let node = AppNode::with_data_dir(
            temp_dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let mut events = node.supervisor_events().await?;

        node.discovery.crash_for_test().await?;

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_failure = false;
        let mut saw_restart = false;
        while Instant::now() < deadline && (!saw_failure || !saw_restart) {
            match timeout(Duration::from_millis(250), events.recv()).await {
                Ok(Ok(SupervisorEvent::ChildFailed { label, .. })) if label == "Discovery" => {
                    saw_failure = true;
                }
                Ok(Ok(SupervisorEvent::ChildRestarted { label, .. })) if label == "Discovery" => {
                    saw_restart = true;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) => break,
                Err(_) => {}
            }
        }

        assert!(saw_failure, "expected Discovery failure event");
        assert!(saw_restart, "expected Discovery restart event");

        timeout(Duration::from_secs(5), async {
            loop {
                if node.discovery.metrics().await.is_ok() {
                    return;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("discovery actor did not recover after supervised restart")?;

        Ok(())
    }

    #[tokio::test]
    async fn supervised_gossip_restarts_after_failure() -> Result<()> {
        let temp_dir = tempdir()?;
        let node = AppNode::with_data_dir(
            temp_dir.path(),
            NodeOptions {
                mdns_enabled: false,
                ..Default::default()
            },
        )
        .await?;
        let mut events = node.supervisor_events().await?;

        node.gossip.crash_for_test().await?;

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_failure = false;
        let mut saw_restart = false;
        while Instant::now() < deadline && (!saw_failure || !saw_restart) {
            match timeout(Duration::from_millis(250), events.recv()).await {
                Ok(Ok(SupervisorEvent::ChildFailed { label, .. })) if label == "Gossip" => {
                    saw_failure = true;
                }
                Ok(Ok(SupervisorEvent::ChildRestarted { label, .. })) if label == "Gossip" => {
                    saw_restart = true;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) => break,
                Err(_) => {}
            }
        }

        assert!(saw_failure, "expected Gossip failure event");
        assert!(saw_restart, "expected Gossip restart event");

        let topic: Topic = Hash::digest(b"supervised-gossip-recovery").into();
        timeout(Duration::from_secs(5), async {
            loop {
                if node.join_topic(topic).await.is_ok() {
                    return;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("gossip actor did not recover after supervised restart")?;

        Ok(())
    }
}
