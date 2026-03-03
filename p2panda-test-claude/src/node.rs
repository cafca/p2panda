use anyhow::Result;
use p2panda_blobs::{Blobs, MemStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::iroh_mdns::MdnsDiscoveryMode;
use p2panda_net::{AddressBook, Discovery, Endpoint, Gossip, MdnsDiscovery, TopicId};

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
    pub async fn new(topic: &str) -> Result<Self> {
        let private_key = PrivateKey::new();

        // Derive topic ID deterministically from the user-supplied string.
        let topic_id: TopicId = Hash::new(topic.as_bytes()).into();

        let address_book = AddressBook::builder().spawn().await?;

        let endpoint = Endpoint::builder(address_book.clone())
            .private_key(private_key)
            .spawn()
            .await?;

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

        let mem_store = MemStore::new();
        let blobs = Blobs::new(&*mem_store, &endpoint, &address_book).await?;

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
