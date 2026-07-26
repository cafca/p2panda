// SPDX-License-Identifier: MIT OR Apache-2.0

//! Application-defined policy for deciding which peers may open blob
//! connections and which hashes they are allowed to fetch.

use std::future::Future;
use std::pin::Pin;

use iroh_blobs::Hash;
use p2panda_net::NodeId;

/// Boxed future returned by the [`BlobServePolicy`] hooks.
///
/// The hooks are called from the blob provider's event loop, so they return a
/// boxed future (keeping the trait object-safe) rather than being `async fn`s.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Outcome of a [`BlobServePolicy`] hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeDecision {
    /// Serve the connection or request.
    Allow,
    /// Refuse it. The remote peer sees a permission error.
    Deny,
}

impl ServeDecision {
    /// Convenience for turning a boolean into a decision (`true` -> `Allow`).
    pub fn allow_if(allowed: bool) -> Self {
        if allowed { Self::Allow } else { Self::Deny }
    }
}

/// Serving-side policy consulted before handing blob data to remote peers.
///
/// Both hooks run on the *serving* node only. They are **not** a
/// confidentiality boundary: a peer that already holds the bytes can re-serve
/// them regardless of what this policy says, and a modified node can ignore the
/// policy entirely. Use them for abuse control, fairness, and "don't serve
/// strangers" style rules; use encryption for confidentiality.
pub trait BlobServePolicy: Send + Sync + 'static {
    /// Called once per inbound connection, before any request on it is served.
    ///
    /// Returning [`ServeDecision::Deny`] rejects the whole connection before a
    /// single byte is transferred. Connections whose peer identity cannot be
    /// determined are rejected without calling this hook.
    fn on_connect(&self, peer: NodeId) -> BoxFuture<'_, ServeDecision>;

    /// Called for every blob request, carrying the peer that opened the
    /// connection the request arrived on.
    ///
    /// Returning [`ServeDecision::Deny`] refuses that request; other requests
    /// on the same connection are unaffected.
    fn on_request(&self, peer: NodeId, hash: Hash) -> BoxFuture<'_, ServeDecision>;
}

/// Default policy: serve everything to every peer.
///
/// This preserves the behaviour of a node with no serving restrictions and is
/// what [`Blobs::new`](crate::Blobs::new) installs when no policy is given.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl BlobServePolicy for AllowAll {
    fn on_connect(&self, _peer: NodeId) -> BoxFuture<'_, ServeDecision> {
        Box::pin(async { ServeDecision::Allow })
    }

    fn on_request(&self, _peer: NodeId, _hash: Hash) -> BoxFuture<'_, ServeDecision> {
        Box::pin(async { ServeDecision::Allow })
    }
}
