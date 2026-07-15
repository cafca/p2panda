// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::downloader::DownloadProgress;
use iroh_blobs::provider::events::{
    AbortReason, ConnectMode, EventMask, ObserveMode, ProviderMessage, RequestMode, ThrottleMode,
};
use p2panda_net::utils::{from_verifying_key, to_verifying_key};
use p2panda_net::{AddressBook, Endpoint, NodeId};
use tracing::warn;

use crate::policy::{AllowAll, BlobServePolicy, ServeDecision};

/// Blobs service offering storage, retrieval and synchronisation of content-addressed data.
pub struct Blobs {
    store: Store,
    downloader: iroh_blobs::api::downloader::Downloader,
    address_book: AddressBook,
    event_task: tokio::task::JoinHandle<()>,
}

impl Deref for Blobs {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl Blobs {
    /// Create a new `Blobs` instance serving every hash to every peer.
    ///
    /// Equivalent to [`Blobs::with_policy`] using the [`AllowAll`] policy. Use
    /// [`Blobs::with_policy`] to restrict which peers may connect and which
    /// hashes they may fetch.
    pub async fn new(
        store: &Store,
        endpoint: &Endpoint,
        address_book: &AddressBook,
    ) -> Result<Self> {
        Self::with_policy(store, endpoint, address_book, Arc::new(AllowAll)).await
    }

    /// Create a new `Blobs` instance whose serving behaviour is governed by the
    /// given [`BlobServePolicy`].
    ///
    /// The policy is consulted on every inbound connection ([`on_connect`]) and
    /// every blob request ([`on_request`]); see [`BlobServePolicy`] for the
    /// (non-cryptographic) guarantees these hooks provide.
    ///
    /// [`on_connect`]: BlobServePolicy::on_connect
    /// [`on_request`]: BlobServePolicy::on_request
    pub async fn with_policy(
        store: &Store,
        endpoint: &Endpoint,
        address_book: &AddressBook,
        policy: Arc<dyn BlobServePolicy>,
    ) -> Result<Self> {
        let (events, mut event_rx) = iroh_blobs::provider::events::EventSender::channel(
            256,
            EventMask {
                // Intercept connections and requests so the policy can reject
                // them; pushes are refused outright and observes just notified.
                connected: ConnectMode::Intercept,
                get: RequestMode::Intercept,
                get_many: RequestMode::Intercept,
                push: RequestMode::Disabled,
                observe: ObserveMode::Notify,
                throttle: ThrottleMode::None,
            },
        );

        let event_task = tokio::spawn(async move {
            // Maps an open connection to the peer that opened it. The provider
            // reports the peer identity once on connect, but tags subsequent
            // requests only with the connection id, so we correlate them here:
            // populated on connect, consulted per request, evicted on close.
            let mut connections: HashMap<u64, NodeId> = HashMap::new();

            while let Some(message) = event_rx.recv().await {
                match message {
                    ProviderMessage::ClientConnected(msg) => {
                        // A connection whose peer identity we can't determine
                        // can't be authorised, so reject it.
                        let Some(peer) = msg.inner.endpoint_id.map(to_verifying_key) else {
                            warn!("rejecting blob connection with unknown peer identity");
                            let _ = msg.tx.send(Err(AbortReason::Permission)).await;
                            continue;
                        };
                        let response = match policy.on_connect(peer).await {
                            ServeDecision::Allow => {
                                connections.insert(msg.inner.connection_id, peer);
                                Ok(())
                            }
                            ServeDecision::Deny => Err(AbortReason::Permission),
                        };
                        let _ = msg.tx.send(response).await;
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        connections.remove(&msg.inner.connection_id);
                    }
                    ProviderMessage::GetRequestReceived(msg) => {
                        let response = match connections.get(&msg.inner.connection_id) {
                            Some(&peer) => decision_to_result(
                                policy.on_request(peer, msg.inner.request.hash).await,
                            ),
                            // No authorised peer for this connection: refuse.
                            None => Err(AbortReason::Permission),
                        };
                        let _ = msg.tx.send(response).await;
                    }
                    ProviderMessage::GetManyRequestReceived(msg) => {
                        let response = match connections.get(&msg.inner.connection_id) {
                            Some(&peer) => {
                                // Allow only when every requested hash is allowed.
                                let mut decision = ServeDecision::Allow;
                                for hash in &msg.inner.request.hashes {
                                    if policy.on_request(peer, *hash).await == ServeDecision::Deny {
                                        decision = ServeDecision::Deny;
                                        break;
                                    }
                                }
                                decision_to_result(decision)
                            }
                            None => Err(AbortReason::Permission),
                        };
                        let _ = msg.tx.send(response).await;
                    }
                    ProviderMessage::ObserveRequestReceived(msg) => {
                        let _ = msg.tx.send(Ok(())).await;
                    }
                    ProviderMessage::PushRequestReceived(msg) => {
                        let _ = msg.tx.send(Err(AbortReason::Permission)).await;
                    }
                    ProviderMessage::ClientConnectedNotify(_)
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
        let providers: Vec<_> = node_ids.into_iter().map(from_verifying_key).collect();
        Ok(self.downloader.download(hash, providers))
    }

    /// Access the pinning API for managing blob GC lifecycle.
    pub fn pins(&self) -> crate::pins::Pins<'_> {
        crate::pins::Pins::new(self.store.tags())
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

fn decision_to_result(decision: ServeDecision) -> std::result::Result<(), AbortReason> {
    match decision {
        ServeDecision::Allow => Ok(()),
        ServeDecision::Deny => Err(AbortReason::Permission),
    }
}
