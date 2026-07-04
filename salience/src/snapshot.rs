//! Snapshot/restore: the app persists engine state wherever and whenever it
//! likes; the crate does no I/O.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::digest::PeerContribution;
use crate::events::ItemAttrs;
use crate::ledger::Series;
use crate::predict::CentroidModel;
use crate::scopes::ScopeIndex;

/// Version of the snapshot schema produced by this crate.
pub const SNAPSHOT_VERSION: u16 = 1;

/// Per-item state: attributes plus the bucketed observation series.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ItemState<S> {
    pub attrs: ItemAttrs<S>,
    pub series: Series,
}

impl<S> Default for ItemState<S> {
    fn default() -> Self {
        Self {
            attrs: ItemAttrs::default(),
            series: Series::default(),
        }
    }
}

/// Complete serializable engine state (everything except configuration,
/// which is passed to [`Engine::restore`](crate::Engine::restore)
/// separately so it can evolve independently of persisted data).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize + Ord, S: Serialize + Ord, P: Serialize + Ord",
    deserialize = "I: Deserialize<'de> + Ord, S: Deserialize<'de> + Ord, P: Deserialize<'de> + Ord"
))]
pub struct Snapshot<I, S, P> {
    pub version: u16,
    #[serde(with = "crate::serde_util")]
    pub items: BTreeMap<I, ItemState<S>>,
    pub scopes: ScopeIndex<S>,
    pub global: Series,
    pub centroids: CentroidModel,
    #[serde(with = "crate::serde_util")]
    pub peers: BTreeMap<P, PeerContribution<I, S>>,
}

impl<I, S, P> Default for Snapshot<I, S, P> {
    fn default() -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            items: BTreeMap::new(),
            scopes: ScopeIndex {
                series: BTreeMap::new(),
            },
            global: Series::default(),
            centroids: CentroidModel::default(),
            peers: BTreeMap::new(),
        }
    }
}

/// Why a snapshot could not be restored.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RestoreError {
    #[error("unsupported snapshot version {0}, expected {SNAPSHOT_VERSION}")]
    UnsupportedVersion(u16),
}
