//! Engine configuration: attention currency, clamps, priors, caps.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::events::InteractionKind;
use crate::scopes::ScopeClass;

/// Converts structured signals (dwell, interactions) into scalar attention
/// units at query time. Signals are stored structured, so these weights stay
/// tunable forever without reprocessing history.
///
/// One attention unit = one second of engaged dwell (by default).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WeightProfile {
    /// Attention units per second of dwell.
    pub units_per_dwell_second: f64,
    /// Attention units per interaction, by kind.
    #[serde(with = "crate::serde_util")]
    pub interaction_weights: BTreeMap<InteractionKind, f64>,
    /// Units for interaction kinds absent from `interaction_weights`.
    pub default_interaction_weight: f64,
}

impl Default for WeightProfile {
    fn default() -> Self {
        let mut interaction_weights = BTreeMap::new();
        interaction_weights.insert(InteractionKind::REACTION, 5.0);
        interaction_weights.insert(InteractionKind::REPLY, 30.0);
        interaction_weights.insert(InteractionKind::SHARE, 60.0);
        interaction_weights.insert(InteractionKind::CLICK, 2.0);
        interaction_weights.insert(InteractionKind::MEDIA_PLAY, 10.0);
        Self {
            units_per_dwell_second: 1.0,
            interaction_weights,
            default_interaction_weight: 5.0,
        }
    }
}

impl WeightProfile {
    pub fn interaction_weight(&self, kind: InteractionKind) -> f64 {
        *self
            .interaction_weights
            .get(&kind)
            .unwrap_or(&self.default_interaction_weight)
    }
}

/// Log-compaction policy: age thresholds (in days) up to which buckets keep
/// daily, weekly and monthly granularity. Older mass merges into a terminal
/// bucket. Coarse buckets exist only where decay weight is small, which is
/// what bounds the compaction error.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Buckets younger than this stay daily.
    pub daily_horizon_days: u32,
    /// Buckets younger than this (but older than daily) merge to 7-day spans.
    pub weekly_horizon_days: u32,
    /// Buckets younger than this (but older than weekly) merge to 28-day
    /// spans. Anything older merges into the terminal bucket.
    pub monthly_horizon_days: u32,
    /// Compact a series lazily once it exceeds this many buckets.
    pub max_live_buckets: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            daily_horizon_days: 7,
            weekly_horizon_days: 56,
            monthly_horizon_days: 365,
            max_live_buckets: 32,
        }
    }
}

/// Configuration of the v1 hierarchical predictor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictorConfig {
    /// Relative weight of each scope class when blending scope posteriors
    /// into an item prior.
    #[serde(with = "crate::serde_util")]
    pub class_weights: BTreeMap<ScopeClass, f64>,
    /// Weight for classes absent from `class_weights`.
    pub default_class_weight: f64,
    /// Pseudo-exposure strength of the global anchor in the attribute pool,
    /// and with which the blended attribute prediction anchors an item's own
    /// posterior. Higher = attribute prediction dominates longer before the
    /// item's own data takes over — and the less uncertain (less explorable)
    /// fresh items look.
    pub prior_strength: f64,
    /// Half-life (days) for the *taste* horizon: scope evidence and the
    /// predictor's global anchor decay with this, independently of the
    /// query's half-life. Taste shifts over weeks, not days; a feed scoring
    /// with a 2-day half-life must not forget what the user likes.
    pub taste_half_life_days: f64,
}

impl Default for PredictorConfig {
    fn default() -> Self {
        let mut class_weights = BTreeMap::new();
        class_weights.insert(ScopeClass::AUTHOR, 1.0);
        class_weights.insert(ScopeClass::KIND, 0.5);
        class_weights.insert(ScopeClass::TOPIC, 1.0);
        Self {
            class_weights,
            default_class_weight: 0.5,
            prior_strength: 5.0,
            taste_half_life_days: 30.0,
        }
    }
}

/// Interest-centroid (taste model) configuration.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CentroidConfig {
    /// Maximum number of interest centroids.
    pub k: usize,
    /// Half-life (days) of a centroid's accumulated mass; taste shifts slowly.
    pub half_life_days: f64,
    /// A converted item's embedding spawns a new centroid (capacity
    /// permitting) when its best cosine similarity is below this threshold;
    /// otherwise it merges into the nearest centroid.
    pub min_merge_similarity: f64,
}

impl Default for CentroidConfig {
    fn default() -> Self {
        Self {
            k: 4,
            half_life_days: 90.0,
            min_merge_similarity: 0.6,
        }
    }
}

/// Engine configuration. All defaults are starting points; everything is
/// app-tunable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineConfig {
    pub weight_profile: WeightProfile,
    /// A single dwell event is clamped to this duration (abandoned open tab
    /// is not four hours of attention).
    pub max_dwell_per_event_ms: u64,
    /// Cap on attention units credited to one item within one day.
    pub max_attention_per_item_per_day: f64,
    /// Prior pseudo-exposure for rate smoothing (in exposure units).
    pub prior_strength: f64,
    /// Prior mean rate used to anchor the global rate when the engine has
    /// seen little data (attention units per exposure unit).
    pub baseline_rate: f64,
    pub predictor: PredictorConfig,
    pub centroids: CentroidConfig,
    pub compaction: CompactionPolicy,
    /// Maximum tracked items; lowest long-half-life-attention items are
    /// evicted beyond this.
    pub max_items: usize,
    /// Half-life (days) of the eviction retention score.
    pub retention_half_life_days: f64,
    /// Maximum tracked scopes.
    pub max_scopes: usize,
    /// Maximum stored peer digests.
    pub max_peers: usize,
    /// Per-peer caps on stored digest entries.
    pub max_peer_items: usize,
    pub max_peer_scopes: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            weight_profile: WeightProfile::default(),
            max_dwell_per_event_ms: 120_000,
            max_attention_per_item_per_day: 600.0,
            prior_strength: 10.0,
            baseline_rate: 1.0,
            predictor: PredictorConfig::default(),
            centroids: CentroidConfig::default(),
            compaction: CompactionPolicy::default(),
            max_items: 100_000,
            retention_half_life_days: 30.0,
            max_scopes: 100_000,
            max_peers: 1_000,
            max_peer_items: 10_000,
            max_peer_scopes: 1_000,
        }
    }
}
