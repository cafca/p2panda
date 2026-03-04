use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use p2panda_blobs::{Blobs, FsStore};
use p2panda_core::identity::PRIVATE_KEY_LEN;
use p2panda_core::PrivateKey;
use p2panda_net::iroh_endpoint::RelayUrl;
use p2panda_net::iroh_mdns::MdnsDiscoveryMode;
use p2panda_net::{AddressBook, Discovery, Endpoint, Gossip, MdnsDiscovery, TopicId};

const APP_NAME: &str = "p2panda-file-sharing";
const NODE_KEY_FILE: &str = "node.key";
const BLOBS_DIR: &str = "blobs";

#[derive(Clone, Debug, Default)]
pub struct NodeOptions {
    pub relay_url: Option<RelayUrl>,
    pub insecure_skip_relay_cert_verify: bool,
}

pub struct AppNode {
    pub blobs: Blobs,
    pub gossip: Gossip,
    pub endpoint: Endpoint,
    pub address_book: AddressBook,
    pub relay_url: Option<RelayUrl>,
    pub data_dir: PathBuf,
    _fs_store: FsStore,
    _mdns: MdnsDiscovery,
    _discovery: Discovery,
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
        let address_book = AddressBook::builder().spawn().await?;

        let mut endpoint_builder = Endpoint::builder(address_book.clone()).private_key(private_key);

        if let Some(relay_url) = opts.relay_url.clone() {
            endpoint_builder = endpoint_builder.relay_url(relay_url);
        }

        if opts.insecure_skip_relay_cert_verify {
            endpoint_builder = endpoint_builder.insecure_skip_relay_cert_verify(true);
        }

        let endpoint = endpoint_builder.spawn().await?;

        let mdns = MdnsDiscovery::builder(address_book.clone(), endpoint.clone())
            .mode(MdnsDiscoveryMode::Active)
            .spawn()
            .await?;

        let discovery = Discovery::builder(address_book.clone(), endpoint.clone())
            .spawn()
            .await?;

        let gossip = Gossip::builder(address_book.clone(), endpoint.clone())
            .spawn()
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
            endpoint,
            address_book,
            relay_url: opts.relay_url,
            data_dir,
            _fs_store: fs_store,
            _mdns: mdns,
            _discovery: discovery,
        })
    }

    pub fn node_id(&self) -> p2panda_core::PublicKey {
        self.endpoint.node_id()
    }

    pub async fn join_topic(&self, topic: TopicId) -> Result<p2panda_net::gossip::GossipHandle> {
        Ok(self.gossip.stream(topic).await?)
    }
}

pub type FileSharingNode = AppNode;

fn app_data_dir() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("", "", APP_NAME)
        .ok_or_else(|| anyhow::anyhow!("failed to resolve app data directory for {APP_NAME}"))?;
    Ok(project_dirs.data_dir().to_path_buf())
}

fn load_or_create_private_key(data_dir: &Path) -> Result<PrivateKey> {
    let key_path = data_dir.join(NODE_KEY_FILE);

    match fs::read(&key_path) {
        Ok(bytes) => private_key_from_bytes(&key_path, &bytes),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let private_key = PrivateKey::new();
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

fn private_key_from_bytes(key_path: &Path, bytes: &[u8]) -> Result<PrivateKey> {
    let key_bytes: [u8; PRIVATE_KEY_LEN] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid private key length in {}: expected {} bytes, got {}",
            key_path.display(),
            PRIVATE_KEY_LEN,
            bytes.len()
        )
    })?;

    Ok(PrivateKey::from_bytes(&key_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use p2panda_core::Hash;
    use tempfile::tempdir;

    #[tokio::test]
    async fn reuses_private_key_for_same_data_dir() -> Result<()> {
        let temp_dir = tempdir()?;

        let node_a = AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()).await?;
        let node_a_id = node_a.node_id();

        assert!(temp_dir.path().join(NODE_KEY_FILE).is_file());
        assert!(temp_dir.path().join(BLOBS_DIR).exists());

        drop(node_a);

        let node_b = AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()).await?;

        assert_eq!(node_a_id, node_b.node_id());

        Ok(())
    }

    #[tokio::test]
    async fn supports_multiple_gossip_topics() -> Result<()> {
        let temp_dir = tempdir()?;
        let node = AppNode::with_data_dir(temp_dir.path(), NodeOptions::default()).await?;

        let topic_a: TopicId = Hash::new(b"topic-a").into();
        let topic_b: TopicId = Hash::new(b"topic-b").into();

        let _handle_a = node.join_topic(topic_a).await?;
        let _handle_b = node.join_topic(topic_b).await?;

        Ok(())
    }
}
