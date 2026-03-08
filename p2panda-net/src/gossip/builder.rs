// SPDX-License-Identifier: MIT OR Apache-2.0

use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};

use crate::address_book::AddressBook;
use crate::gossip::GossipConfig;
use crate::gossip::actors::GossipManager;
use crate::gossip::actors::GossipManagerArgs;
use crate::gossip::api::{Gossip, GossipError};
use crate::iroh_endpoint::Endpoint;

pub struct Builder {
    address_book: AddressBook,
    endpoint: Endpoint,
    config: Option<GossipConfig>,
}

impl Builder {
    pub fn new(address_book: AddressBook, endpoint: Endpoint) -> Self {
        Self {
            address_book,
            endpoint,
            config: None,
        }
    }

    pub fn config(mut self, config: GossipConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub(crate) fn build_args(self) -> GossipManagerArgs {
        let config = self.config.unwrap_or_default();
        (config, self.address_book, self.endpoint)
    }

    pub async fn spawn(self) -> Result<Gossip, GossipError> {
        let args = self.build_args();
        let my_node_id = args.2.node_id();
        let address_book = args.1.clone();

        let (actor_ref, _) = {
            let thread_pool = ThreadLocalActorSpawner::new();
            GossipManager::spawn(None, args.clone(), thread_pool).await?
        };

        Ok(Gossip::new(Some(actor_ref), my_node_id, address_book, args))
    }
}
