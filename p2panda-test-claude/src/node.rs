use anyhow::Result;
use p2panda_blobs::{Blobs, MemStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::iroh_endpoint::RelayUrl;
use p2panda_net::iroh_mdns::MdnsDiscoveryMode;
use p2panda_net::{AddressBook, Discovery, Endpoint, Gossip, MdnsDiscovery, TopicId};

#[derive(Default)]
pub struct NodeOptions {
    /// Optional relay URL for relay-based peer connections.
    pub relay_url: Option<RelayUrl>,
    /// Use passive mDNS (no active announcements). Useful when `--peer` is given.
    pub passive_mdns: bool,
}

pub struct FileSharingNode {
    pub blobs: Blobs,
    pub gossip: Gossip,
    pub endpoint: Endpoint,
    pub address_book: AddressBook,
    pub topic_id: TopicId,
    // Held alive:
    _mdns: MdnsDiscovery,
    _discovery: Discovery,
}

impl FileSharingNode {
    /// Create a new node subscribed to the given user topic string.
    pub async fn new(topic: &str, opts: NodeOptions) -> Result<Self> {
        let private_key = PrivateKey::new();

        // Derive topic ID deterministically from the user-supplied string.
        let topic_id: TopicId = Hash::new(topic.as_bytes()).into();

        let address_book = AddressBook::builder().spawn().await?;

        let mut endpoint_builder = Endpoint::builder(address_book.clone()).private_key(private_key);

        if let Some(relay_url) = opts.relay_url {
            endpoint_builder = endpoint_builder.relay_url(relay_url);
        }

        let endpoint = endpoint_builder.spawn().await?;

        let mdns_mode = if opts.passive_mdns {
            MdnsDiscoveryMode::Passive
        } else {
            MdnsDiscoveryMode::Active
        };

        let mdns = MdnsDiscovery::builder(address_book.clone(), endpoint.clone())
            .mode(mdns_mode)
            .spawn()
            .await?;

        let discovery = Discovery::builder(address_book.clone(), endpoint.clone())
            .spawn()
            .await?;

        let gossip = Gossip::builder(address_book.clone(), endpoint.clone())
            .spawn()
            .await?;

        let mem_store = MemStore::new();
        let blobs = Blobs::new(&mem_store, &endpoint, &address_book).await?;

        Ok(Self {
            blobs,
            gossip,
            endpoint,
            address_book,
            topic_id,
            _mdns: mdns,
            _discovery: discovery,
        })
    }
}
