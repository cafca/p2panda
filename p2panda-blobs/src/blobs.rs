// SPDX-License-Identifier: MIT OR Apache-2.0

use std::ops::Deref;

use anyhow::Result;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use p2panda_net::iroh_endpoint::from_public_key;
use p2panda_net::{AddressBook, Endpoint};
use tracing::warn;

use crate::pins::Pins;

/// Blobs service offering storage, retrieval and synchronisation of content-addressed data.
pub struct Blobs {
    store: Store,
    downloader: iroh_blobs::api::downloader::Downloader,
    address_book: AddressBook,
}

impl Deref for Blobs {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl Blobs {
    /// Create a new `Blobs` instance, registering the blob protocol with the given endpoint.
    pub async fn new(store: &Store, endpoint: &Endpoint, address_book: &AddressBook) -> Result<Self> {
        let blobs_proto = BlobsProtocol::new(store, None);
        endpoint
            .accept(iroh_blobs::ALPN, blobs_proto)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let iroh_ep = endpoint.endpoint().await.map_err(|e| anyhow::anyhow!(e))?;
        let downloader = store.downloader(&iroh_ep);

        Ok(Self {
            store: store.clone(),
            downloader,
            address_book: address_book.clone(),
        })
    }

    /// Download a blob from all currently known peers in the address book.
    pub async fn download(&self, hash: Hash) -> Result<()> {
        let node_ids = self.address_book.node_ids().await?;
        if node_ids.is_empty() {
            warn!("no known peers to download blob from");
            anyhow::bail!("no known peers");
        }
        let providers: Vec<_> = node_ids.into_iter().map(from_public_key).collect();
        self.downloader.download(hash, providers).await?;
        Ok(())
    }

    /// Access the pinning API for managing blob GC lifecycle.
    pub fn pins(&self) -> Pins<'_> {
        Pins::new(self.store.tags())
    }

    /// Access the underlying iroh-blobs store directly for advanced use.
    pub fn store(&self) -> &Store {
        &self.store
    }
}
