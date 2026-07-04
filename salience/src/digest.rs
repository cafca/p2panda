//! Peer digests: the broadcastable artifact and trust-weighted merging.
//!
//! A digest carries decayed pseudo-counts of a peer's *own* observations in
//! a tiered envelope; every section is independently omittable (privacy is
//! an app-level dial). Received digests live in a peer pool kept separate
//! from local observations; their influence surfaces only through the
//! `peer_rate` score component, bounded by trust and blend weight.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::events::Timestamp;
use crate::scopes::Scope;

/// Version of the digest wire schema produced by this crate.
pub const DIGEST_VERSION: u16 = 1;

/// Decayed pseudo-counts for one item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ItemEntry<I> {
    pub id: I,
    /// Attention units, decayed as of `produced_at`.
    pub attention: f64,
    /// Exposure units, decayed as of `produced_at`.
    pub exposure: f64,
}

/// Decayed pseudo-counts for one scope roll-up.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScopeEntry<S> {
    pub scope: Scope<S>,
    pub attention: f64,
    pub exposure: f64,
}

/// The tiered, versioned broadcast envelope. Transport, signing and
/// encryption are the app's job; this crate only defines the data and the
/// merge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Digest<I, S> {
    pub version: u16,
    pub produced_at: Timestamp,
    /// Top-N items by decayed attention. Reveals what the producer viewed.
    pub items: Option<Vec<ItemEntry<I>>>,
    /// Scope roll-ups. Reveals aggregate interests.
    pub scopes: Option<Vec<ScopeEntry<S>>>,
    /// Interest centroids. Sketches the producer's taste.
    pub centroids: Option<Vec<Vec<f32>>>,
}

/// What to include when producing a digest.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct DigestSpec {
    /// Half-life used to decay pseudo-counts to `produced_at`.
    pub half_life_days: f64,
    pub include_items: bool,
    pub include_scopes: bool,
    pub include_centroids: bool,
    pub max_items: usize,
    pub max_scopes: usize,
    /// Items below this decayed exposure are omitted (noise floor).
    pub min_exposure: f64,
}

impl Default for DigestSpec {
    fn default() -> Self {
        Self {
            half_life_days: 30.0,
            include_items: true,
            include_scopes: true,
            include_centroids: false,
            max_items: 256,
            max_scopes: 128,
            min_exposure: 1.0,
        }
    }
}

/// A peer's stored contribution: their latest digest, trust-scaled at query
/// time. A new digest from the same peer replaces this entirely (idempotent,
/// no double counting across re-broadcasts).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize + Ord, S: Serialize + Ord",
    deserialize = "I: Deserialize<'de> + Ord, S: Deserialize<'de> + Ord"
))]
pub struct PeerContribution<I, S> {
    pub trust: f64,
    pub produced_at: Timestamp,
    #[serde(with = "crate::serde_util")]
    pub items: BTreeMap<I, (f64, f64)>,
    #[serde(with = "crate::serde_util")]
    pub scopes: BTreeMap<Scope<S>, (f64, f64)>,
    /// Stored for the app's use (e.g. replication toward a peer's taste);
    /// not used in local scoring in v1.
    pub centroids: Vec<Vec<f32>>,
}

/// Why a digest was rejected by [`Engine::merge_digest`](crate::Engine::merge_digest).
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum MergeError {
    #[error("unsupported digest version {0}, expected {DIGEST_VERSION}")]
    UnsupportedVersion(u16),
    #[error("digest contains a non-finite or negative count")]
    InvalidEntry,
    #[error("trust must be finite, got {0}")]
    InvalidTrust(f64),
}

impl<I: Ord + Clone, S: Ord + Clone> PeerContribution<I, S> {
    /// Validate a received digest and convert it into a stored contribution,
    /// truncating each section to the given caps (largest attention first).
    pub fn from_digest(
        digest: &Digest<I, S>,
        trust: f64,
        max_items: usize,
        max_scopes: usize,
    ) -> Result<Self, MergeError> {
        if digest.version != DIGEST_VERSION {
            return Err(MergeError::UnsupportedVersion(digest.version));
        }
        if !trust.is_finite() {
            return Err(MergeError::InvalidTrust(trust));
        }
        let trust = trust.clamp(0.0, 1.0);

        let valid = |attention: f64, exposure: f64| {
            attention.is_finite() && exposure.is_finite() && attention >= 0.0 && exposure >= 0.0
        };

        let mut items = BTreeMap::new();
        if let Some(entries) = &digest.items {
            for entry in entries {
                if !valid(entry.attention, entry.exposure) {
                    return Err(MergeError::InvalidEntry);
                }
            }
            let mut sorted: Vec<&ItemEntry<I>> = entries.iter().collect();
            sorted.sort_by(|a, b| b.attention.total_cmp(&a.attention));
            for entry in sorted.into_iter().take(max_items) {
                items.insert(entry.id.clone(), (entry.attention, entry.exposure));
            }
        }

        let mut scopes = BTreeMap::new();
        if let Some(entries) = &digest.scopes {
            for entry in entries {
                if !valid(entry.attention, entry.exposure) {
                    return Err(MergeError::InvalidEntry);
                }
            }
            let mut sorted: Vec<&ScopeEntry<S>> = entries.iter().collect();
            sorted.sort_by(|a, b| b.attention.total_cmp(&a.attention));
            for entry in sorted.into_iter().take(max_scopes) {
                scopes.insert(entry.scope.clone(), (entry.attention, entry.exposure));
            }
        }

        Ok(Self {
            trust,
            produced_at: digest.produced_at,
            items,
            scopes,
            centroids: digest.centroids.clone().unwrap_or_default(),
        })
    }
}
