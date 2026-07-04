//! Query types: scoring profiles, blend weights, explainable scores.

use serde::{Deserialize, Serialize};

use crate::scopes::Scope;

/// Exploration strategy for ranking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Exploration {
    /// Rank by posterior means; fully deterministic.
    #[default]
    Off,
    /// Rank by one sample from each rate posterior, giving uncertain (new)
    /// items a principled chance at exposure. Requires
    /// [`Engine::rank_with_rng`](crate::Engine::rank_with_rng).
    ThompsonSample,
}

/// Blend weights over the score components. They need not sum to one; only
/// their ratios matter for ordering.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Blend {
    /// Weight of the item's own observed conversion rate (layer 1, local).
    pub item_rate: f64,
    /// Weight of the personal predicted rate (layer 2).
    pub predicted_rate: f64,
    /// Weight of the trust-weighted peer-reported rate.
    pub peer_rate: f64,
    /// Weight of embedding-centroid affinity. The affinity (in `[0, 1]`) is
    /// scaled by the global mean rate to keep it commensurate with the rate
    /// components.
    pub affinity: f64,
}

/// How to score: time horizon, blend, exploration.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoringProfile {
    /// Decay half-life in days, applied to attention and exposure alike.
    pub half_life_days: f64,
    pub weights: Blend,
    pub exploration: Exploration,
    /// Override of the engine's prior strength for smoothing; `None` uses
    /// the engine config.
    pub prior_strength: Option<f64>,
}

impl ScoringProfile {
    /// Home feed: short horizon, prediction-heavy — "what would I spend
    /// attention on right now?"
    pub fn feed() -> Self {
        Self {
            half_life_days: 2.0,
            weights: Blend {
                item_rate: 0.25,
                predicted_rate: 0.5,
                peer_rate: 0.15,
                affinity: 0.1,
            },
            exploration: Exploration::Off,
            prior_strength: None,
        }
    }

    /// Search re-ranking: medium horizon; relevance stays outside the crate
    /// (combine via [`Engine::rank_boosted`](crate::Engine::rank_boosted)).
    pub fn search() -> Self {
        Self {
            half_life_days: 7.0,
            weights: Blend {
                item_rate: 0.3,
                predicted_rate: 0.5,
                peer_rate: 0.1,
                affinity: 0.1,
            },
            exploration: Exploration::Off,
            prior_strength: None,
        }
    }

    /// Replication: long horizon, ledger- and peer-heavy — "what does the
    /// network demonstrably care about?"
    pub fn replication() -> Self {
        Self {
            half_life_days: 30.0,
            weights: Blend {
                item_rate: 0.5,
                predicted_rate: 0.15,
                peer_rate: 0.35,
                affinity: 0.0,
            },
            exploration: Exploration::Off,
            prior_strength: None,
        }
    }
}

/// An explainable score: the blended total plus every component that went
/// into it. Rankings must be debuggable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Score {
    /// Blended total used for ordering.
    pub total: f64,
    /// Smoothed observed conversion rate of the item itself (local ledger).
    pub item_rate: f64,
    /// Personal predicted rate (attribute back-off + own data).
    pub predicted_rate: f64,
    /// Trust-weighted peer-reported rate.
    pub peer_rate: f64,
    /// Embedding-centroid affinity in `[0, 1]`, `0` when unavailable.
    pub affinity: f64,
    /// Combined posterior standard deviation of the rate components,
    /// weighted by the blend.
    pub uncertainty: f64,
    /// Decayed effective exposure behind the estimate (local plus
    /// trust-weighted peer exposure) — how much evidence there is.
    pub exposure_mass: f64,
}

/// A ranked item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ranked<I> {
    pub id: I,
    pub score: Score,
}

/// A ranked scope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RankedScope<S> {
    pub scope: Scope<S>,
    pub score: Score,
}
