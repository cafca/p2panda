//! Scopes: app-defined grouping keys items belong to (author, content type,
//! topic, ...), and the roll-up index that accumulates attention per scope.
//!
//! Scope roll-ups serve double duty: they are the predictive features of the
//! personal model (layer 2) and they answer replication's coarse-grained
//! questions ("how much attention does topic X convert?") directly via
//! [`Engine::rank_scopes`](crate::Engine::rank_scopes).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::CompactionPolicy;
use crate::ledger::Series;

/// App-defined class of a scope, discriminating e.g. authors from topics.
///
/// Per-class weights in the predictor are configured via
/// [`PredictorConfig`](crate::config::PredictorConfig).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeClass(pub u16);

impl ScopeClass {
    pub const AUTHOR: Self = Self(0);
    pub const KIND: Self = Self(1);
    pub const TOPIC: Self = Self(2);
}

/// A grouping key an item belongs to: a class plus an app-defined key.
///
/// The app maps scopes onto its real concepts (public keys, sync topics,
/// logs, hashtags, ...); the crate never knows those types.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Scope<S> {
    pub class: ScopeClass,
    pub key: S,
}

impl<S> Scope<S> {
    pub fn new(class: ScopeClass, key: S) -> Self {
        Self { class, key }
    }
}

/// Bucketed accumulators per scope, updated on every event of every item
/// belonging to the scope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "S: Serialize + Ord",
    deserialize = "S: Deserialize<'de> + Ord"
))]
pub struct ScopeIndex<S> {
    #[serde(with = "crate::serde_util")]
    pub series: BTreeMap<Scope<S>, Series>,
}

impl<S> Default for ScopeIndex<S> {
    fn default() -> Self {
        Self {
            series: BTreeMap::new(),
        }
    }
}

impl<S: Ord + Clone> ScopeIndex<S> {
    pub fn get(&self, scope: &Scope<S>) -> Option<&Series> {
        self.series.get(scope)
    }

    pub fn record(
        &mut self,
        scope: &Scope<S>,
        day: f64,
        policy: &CompactionPolicy,
        f: impl FnOnce(&mut crate::ledger::Accum),
    ) {
        let series = self.series.entry(scope.clone()).or_default();
        series.record(day, f);
        series.maybe_compact(day, policy);
    }

    pub fn len(&self) -> usize {
        self.series.len()
    }

    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// Evict lowest-mass scopes until at most `max` remain. Mass is the
    /// undecayed event count, a cheap proxy adequate for cap enforcement.
    pub fn evict_to(&mut self, max: usize) {
        if self.series.len() <= max {
            return;
        }
        let mut by_mass: Vec<(f64, Scope<S>)> = self
            .series
            .iter()
            .map(|(scope, series)| (series.event_count(), scope.clone()))
            .collect();
        by_mass.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        let excess = self.series.len() - max;
        for (_, scope) in by_mass.into_iter().take(excess) {
            self.series.remove(&scope);
        }
    }
}
