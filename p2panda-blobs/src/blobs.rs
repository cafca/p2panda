// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::downloader::DownloadProgress;
use iroh_blobs::provider::events::{
    AbortReason, ConnectMode, EventMask, ObserveMode, ProviderMessage, RequestMode, ThrottleMode,
};
use p2panda_net::iroh_endpoint::from_public_key;
use p2panda_net::{AddressBook, Endpoint};
use tracing::warn;

use crate::pins::Pins;

/// Blobs service offering storage, retrieval and synchronisation of content-addressed data.
pub struct Blobs {
    store: Store,
    downloader: iroh_blobs::api::downloader::Downloader,
    address_book: AddressBook,
    blocked_hashes: Arc<RwLock<HashSet<Hash>>>,
    event_task: tokio::task::JoinHandle<()>,
}

impl Deref for Blobs {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl Blobs {
    /// Create a new `Blobs` instance, registering the blob protocol with the given endpoint.
    pub async fn new(
        store: &Store,
        endpoint: &Endpoint,
        address_book: &AddressBook,
    ) -> Result<Self> {
        let blocked_hashes = Arc::new(RwLock::new(HashSet::new()));
        let (events, mut event_rx) = iroh_blobs::provider::events::EventSender::channel(
            256,
            EventMask {
                connected: ConnectMode::Notify,
                get: RequestMode::Intercept,
                get_many: RequestMode::Intercept,
                push: RequestMode::Disabled,
                observe: ObserveMode::Notify,
                throttle: ThrottleMode::None,
            },
        );

        let blocked_hashes_for_events = Arc::clone(&blocked_hashes);
        let event_task = tokio::spawn(async move {
            while let Some(message) = event_rx.recv().await {
                match message {
                    ProviderMessage::GetRequestReceived(msg) => {
                        let blocked = blocked_hashes_for_events
                            .read()
                            .map(|blocked| blocked.contains(&msg.inner.request.hash))
                            .unwrap_or(false);
                        let _ = msg
                            .tx
                            .send(if blocked {
                                Err(AbortReason::Permission)
                            } else {
                                Ok(())
                            })
                            .await;
                    }
                    ProviderMessage::GetManyRequestReceived(msg) => {
                        let blocked = blocked_hashes_for_events
                            .read()
                            .map(|blocked| {
                                msg.inner
                                    .request
                                    .hashes
                                    .iter()
                                    .any(|hash| blocked.contains(hash))
                            })
                            .unwrap_or(false);
                        let _ = msg
                            .tx
                            .send(if blocked {
                                Err(AbortReason::Permission)
                            } else {
                                Ok(())
                            })
                            .await;
                    }
                    ProviderMessage::ClientConnected(msg) => {
                        let _ = msg.tx.send(Ok(())).await;
                    }
                    ProviderMessage::ObserveRequestReceived(msg) => {
                        let _ = msg.tx.send(Ok(())).await;
                    }
                    ProviderMessage::PushRequestReceived(msg) => {
                        let _ = msg.tx.send(Err(AbortReason::Permission)).await;
                    }
                    ProviderMessage::ClientConnectedNotify(_)
                    | ProviderMessage::ConnectionClosed(_)
                    | ProviderMessage::GetRequestReceivedNotify(_)
                    | ProviderMessage::GetManyRequestReceivedNotify(_)
                    | ProviderMessage::PushRequestReceivedNotify(_)
                    | ProviderMessage::ObserveRequestReceivedNotify(_)
                    | ProviderMessage::Throttle(_) => {}
                }
            }
        });

        let blobs_proto = BlobsProtocol::new(store, Some(events));
        endpoint
            .accept_raw(iroh_blobs::ALPN, blobs_proto)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let iroh_ep = endpoint.endpoint().await.map_err(|e| anyhow::anyhow!(e))?;
        let downloader = store.downloader(&iroh_ep);

        Ok(Self {
            store: store.clone(),
            downloader,
            address_book: address_book.clone(),
            blocked_hashes,
            event_task,
        })
    }

    /// Download a blob from all currently known peers in the address book.
    pub async fn download(&self, hash: Hash) -> Result<()> {
        self.download_with_progress(hash).await?.await?;
        Ok(())
    }

    /// Download a blob from all currently known peers in the address book and return the progress handle.
    pub async fn download_with_progress(&self, hash: Hash) -> Result<DownloadProgress> {
        let node_ids = self.address_book.node_ids().await?;
        if node_ids.is_empty() {
            warn!("no known peers to download blob from");
            anyhow::bail!("no known peers");
        }
        let providers: Vec<_> = node_ids.into_iter().map(from_public_key).collect();
        Ok(self.downloader.download(hash, providers))
    }

    /// Block serving the given hashes to remote peers.
    pub fn block_serving_hashes(&self, hashes: impl IntoIterator<Item = Hash>) {
        if let Ok(mut blocked) = self.blocked_hashes.write() {
            blocked.extend(hashes);
        }
    }

    /// Unblock serving the given hashes to remote peers.
    pub fn unblock_serving_hashes(&self, hashes: impl IntoIterator<Item = Hash>) {
        if let Ok(mut blocked) = self.blocked_hashes.write() {
            for hash in hashes {
                blocked.remove(&hash);
            }
        }
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

impl Drop for Blobs {
    fn drop(&mut self) {
        self.event_task.abort();
    }
}
