//! # salience
//!
//! Attention estimation for ranking and replication in social applications.
//!
//! Attention is a finite resource users spend on content. Every time the app
//! shows an item it grants an *opportunity* (a prominence-weighted exposure —
//! the app's bet); the user converts some of it into actual attention,
//! measurable through its byproducts: dwell time and interactions. The
//! central quantity is the **conversion rate** — attention spent per exposure
//! granted — which removes position bias and makes ranking self-correcting.
//!
//! The engine maintains two layers, blended per consumer via
//! [`ScoringProfile`]:
//!
//! 1. an **item ledger** of observed exposure/attention (local + peer
//!    reported) — identifies, doesn't generalize;
//! 2. a **personal predictive model** over item attributes (author, content
//!    type, topics, optional embedding) — generalizes to unseen items.
//!
//! The crate is pure: no I/O, no async, no clock, no ML runtime. Timestamps
//! are explicit, randomness is caller-supplied, persistence is
//! snapshot/restore, transport of [`Digest`]s is the app's job.
//!
//! ```
//! use salience::{
//!     Engine, EngineConfig, Event, InteractionKind, ItemAttrs, Scope, ScopeClass,
//!     ScoringProfile,
//! };
//!
//! let mut engine: Engine<&str, &str, &str> = Engine::new(EngineConfig::default());
//! let now = 1_700_000_000_000;
//!
//! engine.upsert_item("post-1", ItemAttrs::new(vec![Scope::new(ScopeClass::AUTHOR, "alice")]));
//! engine.record(Event::Exposure { item: "post-1", weight: 1.0, at: now }).unwrap();
//! engine.record(Event::Dwell { item: "post-1", millis: 12_000, at: now }).unwrap();
//! engine.record(Event::Interaction { item: "post-1", kind: InteractionKind::REPLY, at: now }).unwrap();
//!
//! let ranked = engine.rank(&["post-1"], &ScoringProfile::feed(), now);
//! assert!(ranked[0].score.total > 0.0);
//! ```

pub mod config;
pub mod digest;
pub mod events;
pub mod ledger;
pub mod predict;
pub mod rank;
pub mod scopes;
pub(crate) mod serde_util;
pub mod snapshot;
pub mod stats;

use std::collections::BTreeMap;

pub use config::{CentroidConfig, CompactionPolicy, EngineConfig, PredictorConfig, WeightProfile};
pub use digest::{Digest, DigestSpec, ItemEntry, MergeError, PeerContribution, ScopeEntry};
pub use events::{day_of, Event, InteractionKind, ItemAttrs, Timestamp};
pub use predict::{CentroidModel, HierarchicalPredictor, Predictor, ScopeEvidence};
pub use rank::{Blend, Exploration, Ranked, RankedScope, Score, ScoringProfile};
pub use scopes::{Scope, ScopeClass, ScopeIndex};
pub use snapshot::{ItemState, RestoreError, Snapshot, SNAPSHOT_VERSION};
pub use stats::{decay_weight, Prior, PseudoCountEstimator, RateEstimator, RatePosterior};

use ledger::Series;
use rand::Rng;
use stats::RatePosterior as Posterior;

/// Why an event was rejected by [`Engine::record`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RecordError {
    #[error("exposure weight must be finite and positive, got {0}")]
    InvalidExposureWeight(f64),
}

/// The attention engine: owned, in-memory, synchronous.
///
/// Generic over the item id `I`, scope key `S` and peer id `P` (all
/// app-defined), plus the pluggable [`Predictor`] and [`RateEstimator`].
#[derive(Clone, Debug)]
pub struct Engine<I, S, P, PR = HierarchicalPredictor, E = PseudoCountEstimator> {
    config: EngineConfig,
    items: BTreeMap<I, ItemState<S>>,
    scopes: ScopeIndex<S>,
    global: Series,
    centroids: CentroidModel,
    peers: BTreeMap<P, PeerContribution<I, S>>,
    predictor: PR,
    estimator: E,
    /// Latest event day seen; reference point for eviction decay.
    last_day: f64,
}

/// Rate posteriors backing one item's score, before blending.
struct ItemPosteriors {
    item: Posterior,
    predicted: Posterior,
    peer: Posterior,
    affinity: f64,
    exposure_mass: f64,
}

impl<I, S, P, PR, E> Engine<I, S, P, PR, E>
where
    I: Clone + Ord,
    S: Clone + Ord,
    P: Clone + Ord,
    PR: Predictor,
    E: RateEstimator,
{
    pub fn new(config: EngineConfig) -> Self
    where
        PR: Default,
        E: Default,
    {
        Self::with_parts(config, PR::default(), E::default())
    }

    /// Construct with explicit predictor and estimator implementations.
    pub fn with_parts(config: EngineConfig, predictor: PR, estimator: E) -> Self {
        Self {
            config,
            items: BTreeMap::new(),
            scopes: ScopeIndex {
                series: BTreeMap::new(),
            },
            global: Series::default(),
            centroids: CentroidModel::default(),
            peers: BTreeMap::new(),
            predictor,
            estimator,
            last_day: 0.0,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Number of currently tracked items.
    pub fn tracked_items(&self) -> usize {
        self.items.len()
    }

    /// The taste model (interest centroids) learned so far.
    pub fn centroids(&self) -> &CentroidModel {
        &self.centroids
    }

    /// Register or update an item's attributes. Attributes affect scope
    /// roll-ups only for events recorded after this call.
    pub fn upsert_item(&mut self, id: I, attrs: ItemAttrs<S>) {
        self.items.entry(id).or_default().attrs = attrs;
        self.maybe_evict_items();
    }

    /// Record an attention observation. Items unknown at this point are
    /// auto-created with empty attributes (attributes may arrive later via
    /// [`Engine::upsert_item`]).
    ///
    /// Dwell events are clamped to `max_dwell_per_event_ms`, and attention
    /// credited to one item within one day is capped by
    /// `max_attention_per_item_per_day` (excess is dropped).
    pub fn record(&mut self, event: Event<I>) -> Result<(), RecordError> {
        let day = day_of(event.at());
        self.last_day = self.last_day.max(day);
        let profile = self.config.weight_profile.clone();
        let policy = self.config.compaction;

        match event {
            Event::Exposure {
                item,
                weight,
                at: _,
            } => {
                if !weight.is_finite() || weight <= 0.0 {
                    return Err(RecordError::InvalidExposureWeight(weight));
                }
                self.apply(&item, day, &policy, |accum| accum.exposure += weight);
            }
            Event::Dwell {
                item,
                millis,
                at: _,
            } => {
                let millis = millis.min(self.config.max_dwell_per_event_ms) as f64;
                let added_units = millis / 1000.0 * profile.units_per_dwell_second;
                let state = self.items.entry(item.clone()).or_default();
                let remaining = self.config.max_attention_per_item_per_day
                    - state.series.units_on_day(day, &profile);
                if remaining <= 0.0 || added_units <= 0.0 {
                    return Ok(());
                }
                let scale = (remaining / added_units).min(1.0);
                let millis = millis * scale;
                self.apply(&item, day, &policy, |accum| accum.dwell_ms += millis);
                self.observe_taste(&item, added_units * scale, day);
            }
            Event::Interaction { item, kind, at: _ } => {
                let weight = profile.interaction_weight(kind);
                let state = self.items.entry(item.clone()).or_default();
                let remaining = self.config.max_attention_per_item_per_day
                    - state.series.units_on_day(day, &profile);
                if weight > remaining {
                    return Ok(());
                }
                self.apply(&item, day, &policy, |accum| {
                    *accum.interactions.entry(kind).or_insert(0.0) += 1.0;
                });
                self.observe_taste(&item, weight, day);
            }
        }
        Ok(())
    }

    /// Apply one recording closure to the item's series, each of its scopes,
    /// and the global aggregate.
    fn apply(
        &mut self,
        item: &I,
        day: f64,
        policy: &CompactionPolicy,
        f: impl Fn(&mut ledger::Accum),
    ) {
        let state = self.items.entry(item.clone()).or_default();
        state.series.record(day, &f);
        state.series.maybe_compact(day, policy);
        let item_scopes = state.attrs.scopes.clone();
        for scope in &item_scopes {
            self.scopes.record(scope, day, policy, &f);
        }
        self.global.record(day, &f);
        self.global.maybe_compact(day, policy);
        self.scopes.evict_to(self.config.max_scopes);
        self.maybe_evict_items();
    }

    /// Fold converted attention into the taste model when the item carries
    /// an embedding.
    fn observe_taste(&mut self, item: &I, mass: f64, day: f64) {
        let Some(state) = self.items.get(item) else {
            return;
        };
        let Some(embedding) = state.attrs.embedding.clone() else {
            return;
        };
        self.centroids
            .observe(&embedding, mass, day, &self.config.centroids);
    }

    fn maybe_evict_items(&mut self) {
        if self.items.len() <= self.config.max_items {
            return;
        }
        let excess = self.items.len() - self.config.max_items;
        let profile = &self.config.weight_profile;
        let half_life = self.config.retention_half_life_days;
        let now = self.last_day;
        let mut scored: Vec<(f64, I)> = self
            .items
            .iter()
            .map(|(id, state)| {
                let (attention, _) = state.series.decayed(now, half_life, profile);
                (attention, id.clone())
            })
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        for (_, id) in scored.into_iter().take(excess) {
            self.items.remove(&id);
        }
    }

    /// Compact every series against `now`. Compaction also happens lazily
    /// during recording; calling this explicitly is optional housekeeping
    /// (e.g. before [`Engine::snapshot`]).
    pub fn compact(&mut self, now: Timestamp) {
        let day = day_of(now);
        let policy = self.config.compaction;
        for state in self.items.values_mut() {
            state.series.compact(day, &policy);
        }
        for series in self.scopes.series.values_mut() {
            series.compact(day, &policy);
        }
        self.global.compact(day, &policy);
    }

    // ------------------------------------------------------------------
    // Scoring
    // ------------------------------------------------------------------

    fn prior_strength(&self, profile: &ScoringProfile) -> f64 {
        profile.prior_strength.unwrap_or(self.config.prior_strength)
    }

    fn global_posterior(&self, now_day: f64, profile: &ScoringProfile) -> Posterior {
        let (attention, exposure) =
            self.global
                .decayed(now_day, profile.half_life_days, &self.config.weight_profile);
        self.estimator.estimate(
            attention,
            exposure,
            Prior {
                rate: self.config.baseline_rate,
                strength: self.prior_strength(profile),
            },
        )
    }

    /// Trust-weighted peer pseudo-counts for one item, decayed by digest age.
    fn peer_item_counts(&self, id: &I, now_day: f64, half_life_days: f64) -> (f64, f64) {
        let mut attention = 0.0;
        let mut exposure = 0.0;
        for contribution in self.peers.values() {
            if let Some((a, e)) = contribution.items.get(id) {
                let age = (now_day - day_of(contribution.produced_at)).max(0.0);
                let w = contribution.trust * decay_weight(age, half_life_days);
                attention += a * w;
                exposure += e * w;
            }
        }
        (attention, exposure)
    }

    fn peer_scope_counts(&self, scope: &Scope<S>, now_day: f64, half_life_days: f64) -> (f64, f64) {
        let mut attention = 0.0;
        let mut exposure = 0.0;
        for contribution in self.peers.values() {
            if let Some((a, e)) = contribution.scopes.get(scope) {
                let age = (now_day - day_of(contribution.produced_at)).max(0.0);
                let w = contribution.trust * decay_weight(age, half_life_days);
                attention += a * w;
                exposure += e * w;
            }
        }
        (attention, exposure)
    }

    fn item_posteriors(
        &self,
        id: &I,
        profile: &ScoringProfile,
        now_day: f64,
        global: &Posterior,
    ) -> ItemPosteriors {
        let wp = &self.config.weight_profile;
        let ps = self.prior_strength(profile);
        let hl = profile.half_life_days;
        let global_prior = Prior {
            rate: global.mean(),
            strength: ps,
        };

        let state = self.items.get(id);
        let (attention, exposure) = state
            .map(|s| s.series.decayed(now_day, hl, wp))
            .unwrap_or((0.0, 0.0));

        let item = self.estimator.estimate(attention, exposure, global_prior);

        // Decayed scope evidence for the item's attributes feeds the
        // predictor. Taste has its own, slower horizon: the feed may score
        // with a 2-day half-life, but what the user likes must not be
        // forgotten at that pace.
        let taste_hl = self.config.predictor.taste_half_life_days;
        let item_scopes: &[Scope<S>] = state.map(|s| s.attrs.scopes.as_slice()).unwrap_or(&[]);
        let mut evidence = Vec::with_capacity(item_scopes.len());
        for scope in item_scopes {
            let (s_att, s_exp) = self
                .scopes
                .get(scope)
                .map(|series| series.decayed(now_day, taste_hl, wp))
                .unwrap_or((0.0, 0.0));
            evidence.push(predict::ScopeEvidence {
                class: scope.class,
                attention: s_att,
                exposure: s_exp,
            });
        }
        let (g_att, g_exp) = self.global.decayed(now_day, taste_hl, wp);
        let global_taste = self.estimator.estimate(
            g_att,
            g_exp,
            Prior {
                rate: self.config.baseline_rate,
                strength: ps,
            },
        );
        let attr_prior = self
            .predictor
            .prior(&evidence, &global_taste, &self.config.predictor);
        let predicted = self.estimator.estimate(attention, exposure, attr_prior);

        let (p_att, p_exp) = self.peer_item_counts(id, now_day, hl);
        let peer = self.estimator.estimate(p_att, p_exp, global_prior);

        let affinity = state
            .and_then(|s| s.attrs.embedding.as_deref())
            .and_then(|e| self.centroids.affinity(e))
            .unwrap_or(0.0);

        ItemPosteriors {
            item,
            predicted,
            peer,
            affinity,
            exposure_mass: exposure + p_exp,
        }
    }

    fn blend(
        profile: &ScoringProfile,
        posteriors: &ItemPosteriors,
        global_mean: f64,
        item_rate: f64,
        predicted_rate: f64,
        peer_rate: f64,
    ) -> Score {
        let w = &profile.weights;
        let uncertainty = ((w.item_rate * posteriors.item.std()).powi(2)
            + (w.predicted_rate * posteriors.predicted.std()).powi(2)
            + (w.peer_rate * posteriors.peer.std()).powi(2))
        .sqrt();
        let total = w.item_rate * item_rate
            + w.predicted_rate * predicted_rate
            + w.peer_rate * peer_rate
            + w.affinity * posteriors.affinity * global_mean;
        Score {
            total,
            item_rate,
            predicted_rate,
            peer_rate,
            affinity: posteriors.affinity,
            uncertainty,
            exposure_mass: posteriors.exposure_mass,
        }
    }

    /// Score a single item (posterior means; deterministic).
    pub fn score(&self, id: &I, profile: &ScoringProfile, now: Timestamp) -> Score {
        let now_day = day_of(now);
        let global = self.global_posterior(now_day, profile);
        let posteriors = self.item_posteriors(id, profile, now_day, &global);
        Self::blend(
            profile,
            &posteriors,
            global.mean(),
            posteriors.item.mean(),
            posteriors.predicted.mean(),
            posteriors.peer.mean(),
        )
    }

    /// Rank candidates by blended score, descending; ties break on item id.
    /// Deterministic: uses posterior means regardless of
    /// `profile.exploration` (see [`Engine::rank_with_rng`]).
    pub fn rank(
        &self,
        candidates: &[I],
        profile: &ScoringProfile,
        now: Timestamp,
    ) -> Vec<Ranked<I>> {
        let mut ranked: Vec<Ranked<I>> = candidates
            .iter()
            .map(|id| Ranked {
                id: id.clone(),
                score: self.score(id, profile, now),
            })
            .collect();
        Self::sort(&mut ranked);
        ranked
    }

    /// Rank with exploration: when `profile.exploration` is
    /// [`Exploration::ThompsonSample`], each rate component is one draw from
    /// its posterior, giving uncertain items a principled chance to surface.
    pub fn rank_with_rng<R: Rng + ?Sized>(
        &self,
        candidates: &[I],
        profile: &ScoringProfile,
        now: Timestamp,
        rng: &mut R,
    ) -> Vec<Ranked<I>> {
        if profile.exploration == Exploration::Off {
            return self.rank(candidates, profile, now);
        }
        let now_day = day_of(now);
        let global = self.global_posterior(now_day, profile);
        let mut ranked: Vec<Ranked<I>> = candidates
            .iter()
            .map(|id| {
                let posteriors = self.item_posteriors(id, profile, now_day, &global);
                let score = Self::blend(
                    profile,
                    &posteriors,
                    global.mean(),
                    posteriors.item.sample(rng),
                    posteriors.predicted.sample(rng),
                    posteriors.peer.sample(rng),
                );
                Ranked {
                    id: id.clone(),
                    score,
                }
            })
            .collect();
        Self::sort(&mut ranked);
        ranked
    }

    /// Rank with an external per-candidate boost multiplied into the total —
    /// the search integration point (boost = relevance).
    pub fn rank_boosted(
        &self,
        candidates: &[(I, f64)],
        profile: &ScoringProfile,
        now: Timestamp,
    ) -> Vec<Ranked<I>> {
        let mut ranked: Vec<Ranked<I>> = candidates
            .iter()
            .map(|(id, boost)| {
                let mut score = self.score(id, profile, now);
                score.total *= boost;
                Ranked {
                    id: id.clone(),
                    score,
                }
            })
            .collect();
        Self::sort(&mut ranked);
        ranked
    }

    /// Rank scopes (topics, authors, logs...) by their rolled-up conversion
    /// rate — replication's coarse-grained signal. Only the `item_rate`
    /// (here: the scope's own observed rate) and `peer_rate` blend weights
    /// apply.
    pub fn rank_scopes(
        &self,
        scopes: &[Scope<S>],
        profile: &ScoringProfile,
        now: Timestamp,
    ) -> Vec<RankedScope<S>> {
        let now_day = day_of(now);
        let wp = &self.config.weight_profile;
        let hl = profile.half_life_days;
        let global = self.global_posterior(now_day, profile);
        let global_prior = Prior {
            rate: global.mean(),
            strength: self.prior_strength(profile),
        };
        let w = &profile.weights;
        let mut ranked: Vec<RankedScope<S>> = scopes
            .iter()
            .map(|scope| {
                let (attention, exposure) = self
                    .scopes
                    .get(scope)
                    .map(|series| series.decayed(now_day, hl, wp))
                    .unwrap_or((0.0, 0.0));
                let local = self.estimator.estimate(attention, exposure, global_prior);
                let (p_att, p_exp) = self.peer_scope_counts(scope, now_day, hl);
                let peer = self.estimator.estimate(p_att, p_exp, global_prior);
                let uncertainty = ((w.item_rate * local.std()).powi(2)
                    + (w.peer_rate * peer.std()).powi(2))
                .sqrt();
                RankedScope {
                    scope: scope.clone(),
                    score: Score {
                        total: w.item_rate * local.mean() + w.peer_rate * peer.mean(),
                        item_rate: local.mean(),
                        predicted_rate: 0.0,
                        peer_rate: peer.mean(),
                        affinity: 0.0,
                        uncertainty,
                        exposure_mass: exposure + p_exp,
                    },
                }
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.score
                .total
                .total_cmp(&a.score.total)
                .then_with(|| a.scope.cmp(&b.scope))
        });
        ranked
    }

    fn sort(ranked: &mut [Ranked<I>]) {
        ranked.sort_by(|a, b| {
            b.score
                .total
                .total_cmp(&a.score.total)
                .then_with(|| a.id.cmp(&b.id))
        });
    }

    // ------------------------------------------------------------------
    // Digests
    // ------------------------------------------------------------------

    /// Produce the broadcastable digest of *local, first-hand* observations.
    /// Received peer data is never re-exported, preventing gossip
    /// amplification loops.
    pub fn digest(&self, spec: &DigestSpec, now: Timestamp) -> Digest<I, S> {
        let now_day = day_of(now);
        let wp = &self.config.weight_profile;

        let items = spec.include_items.then(|| {
            let mut entries: Vec<ItemEntry<I>> = self
                .items
                .iter()
                .filter_map(|(id, state)| {
                    let (attention, exposure) =
                        state.series.decayed(now_day, spec.half_life_days, wp);
                    (exposure >= spec.min_exposure).then(|| ItemEntry {
                        id: id.clone(),
                        attention,
                        exposure,
                    })
                })
                .collect();
            entries.sort_by(|a, b| b.attention.total_cmp(&a.attention));
            entries.truncate(spec.max_items);
            entries
        });

        let scopes = spec.include_scopes.then(|| {
            let mut entries: Vec<ScopeEntry<S>> = self
                .scopes
                .series
                .iter()
                .filter_map(|(scope, series)| {
                    let (attention, exposure) = series.decayed(now_day, spec.half_life_days, wp);
                    (exposure >= spec.min_exposure).then(|| ScopeEntry {
                        scope: scope.clone(),
                        attention,
                        exposure,
                    })
                })
                .collect();
            entries.sort_by(|a, b| b.attention.total_cmp(&a.attention));
            entries.truncate(spec.max_scopes);
            entries
        });

        let centroids = spec.include_centroids.then(|| {
            self.centroids
                .centroids
                .iter()
                .map(|c| c.vector.clone())
                .collect()
        });

        Digest {
            version: digest::DIGEST_VERSION,
            produced_at: now,
            items,
            scopes,
            centroids,
        }
    }

    /// Store a peer's digest, scaled by `trust ∈ [0, 1]` (app policy: social
    /// distance, explicit lists...). A new digest from the same peer
    /// **replaces** their previous contribution — merging is idempotent per
    /// peer and order-insensitive across peers.
    pub fn merge_digest(
        &mut self,
        peer: P,
        digest: &Digest<I, S>,
        trust: f64,
    ) -> Result<(), MergeError> {
        let contribution = PeerContribution::from_digest(
            digest,
            trust,
            self.config.max_peer_items,
            self.config.max_peer_scopes,
        )?;
        self.peers.insert(peer, contribution);
        self.maybe_evict_peers();
        Ok(())
    }

    /// Remove a peer's contribution entirely.
    pub fn remove_peer(&mut self, peer: &P) {
        self.peers.remove(peer);
    }

    fn maybe_evict_peers(&mut self) {
        if self.peers.len() <= self.config.max_peers {
            return;
        }
        let excess = self.peers.len() - self.config.max_peers;
        let mut scored: Vec<(f64, Timestamp, P)> = self
            .peers
            .iter()
            .map(|(peer, c)| (c.trust, c.produced_at, peer.clone()))
            .collect();
        // Lowest trust first, then oldest digest, then key order.
        scored.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        for (_, _, peer) in scored.into_iter().take(excess) {
            self.peers.remove(&peer);
        }
    }

    // ------------------------------------------------------------------
    // Persistence
    // ------------------------------------------------------------------

    /// Serializable copy of the complete engine state (excluding config).
    pub fn snapshot(&self) -> Snapshot<I, S, P> {
        Snapshot {
            version: SNAPSHOT_VERSION,
            items: self.items.clone(),
            scopes: self.scopes.clone(),
            global: self.global.clone(),
            centroids: self.centroids.clone(),
            peers: self.peers.clone(),
        }
    }

    /// Rebuild an engine from a snapshot. Config is supplied separately so
    /// it can evolve independently of persisted data.
    pub fn restore(snapshot: Snapshot<I, S, P>, config: EngineConfig) -> Result<Self, RestoreError>
    where
        PR: Default,
        E: Default,
    {
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(RestoreError::UnsupportedVersion(snapshot.version));
        }
        let last_day = snapshot
            .items
            .values()
            .flat_map(|s| s.series.buckets.iter())
            .map(|b| b.rep_day)
            .fold(0.0, f64::max);
        Ok(Self {
            config,
            items: snapshot.items,
            scopes: snapshot.scopes,
            global: snapshot.global,
            centroids: snapshot.centroids,
            peers: snapshot.peers,
            predictor: PR::default(),
            estimator: E::default(),
            last_day,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestEngine = Engine<&'static str, &'static str, &'static str>;

    const DAY_MS: u64 = 86_400_000;

    fn engine() -> TestEngine {
        Engine::new(EngineConfig::default())
    }

    fn author(name: &'static str) -> Scope<&'static str> {
        Scope::new(ScopeClass::AUTHOR, name)
    }

    fn expose(e: &mut TestEngine, item: &'static str, weight: f64, at: Timestamp) {
        e.record(Event::Exposure { item, weight, at }).unwrap();
    }

    fn dwell(e: &mut TestEngine, item: &'static str, secs: u64, at: Timestamp) {
        e.record(Event::Dwell {
            item,
            millis: secs * 1000,
            at,
        })
        .unwrap();
    }

    #[test]
    fn conversion_beats_accumulation() {
        // The core position-bias assertion: an over-exposed mediocre item
        // must not outrank an under-exposed good one, even though its total
        // attention is far larger.
        let mut e = engine();
        let now = 100 * DAY_MS;
        // Mediocre M: 100 exposures, 300s total attention (rate 3).
        for i in 0..100 {
            expose(&mut e, "M", 1.0, now + i);
            dwell(&mut e, "M", 3, now + i);
        }
        // Good G: 5 exposures, 75s total attention (rate 15).
        for i in 0..5 {
            expose(&mut e, "G", 1.0, now + i);
            dwell(&mut e, "G", 15, now + i);
        }
        let profile = ScoringProfile {
            weights: Blend {
                item_rate: 1.0,
                predicted_rate: 0.0,
                peer_rate: 0.0,
                affinity: 0.0,
            },
            ..ScoringProfile::replication()
        };
        let ranked = e.rank(&["M", "G"], &profile, now + DAY_MS);
        assert_eq!(ranked[0].id, "G");
        assert!(ranked[0].score.item_rate > ranked[1].score.item_rate);
    }

    #[test]
    fn unseen_items_are_predicted_from_scopes() {
        let mut e = engine();
        let now = 100 * DAY_MS;
        // The user converts heavily on alice, poorly on bob.
        for i in 0..50u64 {
            let (a, b): (&'static str, &'static str) = ("a-old", "b-old");
            e.upsert_item(a, ItemAttrs::new(vec![author("alice")]));
            e.upsert_item(b, ItemAttrs::new(vec![author("bob")]));
            expose(&mut e, a, 1.0, now + i * 1000);
            dwell(&mut e, a, 20, now + i * 1000);
            expose(&mut e, b, 1.0, now + i * 1000);
            dwell(&mut e, b, 1, now + i * 1000);
        }
        // Two brand-new posts, no events at all.
        e.upsert_item("a-new", ItemAttrs::new(vec![author("alice")]));
        e.upsert_item("b-new", ItemAttrs::new(vec![author("bob")]));
        let ranked = e.rank(&["b-new", "a-new"], &ScoringProfile::feed(), now + DAY_MS);
        assert_eq!(ranked[0].id, "a-new");
        assert!(ranked[0].score.predicted_rate > ranked[1].score.predicted_rate);
    }

    #[test]
    fn dwell_clamp_and_daily_cap_apply() {
        let mut e = engine();
        let now = 100 * DAY_MS;
        expose(&mut e, "x", 1.0, now);
        // A 10-hour "dwell" is clamped to 120s.
        dwell(&mut e, "x", 36_000, now);
        let score_day = day_of(now);
        let units = e.items["x"]
            .series
            .units_on_day(score_day, &e.config.weight_profile);
        assert!((units - 120.0).abs() < 1e-9);
        // Repeated clamped dwells stop at the daily cap (600 units).
        for i in 0..10 {
            dwell(&mut e, "x", 120, now + i + 1);
        }
        let units = e.items["x"]
            .series
            .units_on_day(score_day, &e.config.weight_profile);
        assert!((units - 600.0).abs() < 1e-9, "units {units}");
    }

    #[test]
    fn invalid_exposure_weight_is_rejected() {
        let mut e = engine();
        for weight in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
            assert!(e
                .record(Event::Exposure {
                    item: "x",
                    weight,
                    at: 0,
                })
                .is_err());
        }
    }

    #[test]
    fn rank_is_deterministic_with_stable_tie_break() {
        let e = engine();
        let ranked = e.rank(&["b", "a", "c"], &ScoringProfile::feed(), DAY_MS);
        // All-unknown items score identically; order falls back to id.
        assert_eq!(
            ranked.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn digest_merge_is_idempotent_per_peer() {
        let mut source = engine();
        let now = 100 * DAY_MS;
        expose(&mut source, "hot", 10.0, now);
        dwell(&mut source, "hot", 100, now);
        let digest = source.digest(&DigestSpec::default(), now);

        let mut e = engine();
        e.merge_digest("peer1", &digest, 1.0).unwrap();
        let once = e.score(&"hot", &ScoringProfile::replication(), now);
        e.merge_digest("peer1", &digest, 1.0).unwrap();
        e.merge_digest("peer1", &digest, 1.0).unwrap();
        let thrice = e.score(&"hot", &ScoringProfile::replication(), now);
        assert_eq!(once, thrice);
    }

    #[test]
    fn digest_merge_is_order_insensitive_across_peers() {
        let now = 100 * DAY_MS;
        let mut s1 = engine();
        expose(&mut s1, "hot", 5.0, now);
        dwell(&mut s1, "hot", 50, now);
        let d1 = s1.digest(&DigestSpec::default(), now);
        let mut s2 = engine();
        expose(&mut s2, "hot", 2.0, now);
        dwell(&mut s2, "hot", 30, now);
        let d2 = s2.digest(&DigestSpec::default(), now);

        let mut ab = engine();
        ab.merge_digest("p1", &d1, 0.8).unwrap();
        ab.merge_digest("p2", &d2, 0.3).unwrap();
        let mut ba = engine();
        ba.merge_digest("p2", &d2, 0.3).unwrap();
        ba.merge_digest("p1", &d1, 0.8).unwrap();
        let profile = ScoringProfile::replication();
        assert_eq!(
            ab.score(&"hot", &profile, now),
            ba.score(&"hot", &profile, now)
        );
    }

    #[test]
    fn peer_influence_scales_with_trust_and_is_bounded() {
        let now = 100 * DAY_MS;
        let mut source = engine();
        // A peer reporting an extreme rate.
        expose(&mut source, "spam", 10.0, now);
        for i in 0..50 {
            dwell(&mut source, "spam", 100, now + i);
        }
        let digest = source.digest(&DigestSpec::default(), now);

        let profile = ScoringProfile::replication();
        let mut trusted = engine();
        trusted.merge_digest("p", &digest, 1.0).unwrap();
        let mut distrusted = engine();
        distrusted.merge_digest("p", &digest, 0.1).unwrap();
        let none = engine();

        let t = trusted.score(&"spam", &profile, now).peer_rate;
        let d = distrusted.score(&"spam", &profile, now).peer_rate;
        let n = none.score(&"spam", &profile, now).peer_rate;
        assert!(t > d, "trust 1.0 ({t}) should outweigh trust 0.1 ({d})");
        assert!(d > n);
        // Peer data must never leak into first-hand digests.
        let re_export = trusted.digest(&DigestSpec::default(), now);
        assert!(re_export.items.unwrap().is_empty());
    }

    #[test]
    fn digest_rejects_bad_input() {
        let mut e = engine();
        let bad_version = Digest::<&str, &str> {
            version: 99,
            produced_at: 0,
            items: None,
            scopes: None,
            centroids: None,
        };
        assert_eq!(
            e.merge_digest("p", &bad_version, 1.0),
            Err(MergeError::UnsupportedVersion(99))
        );
        let bad_entry = Digest::<&str, &str> {
            version: digest::DIGEST_VERSION,
            produced_at: 0,
            items: Some(vec![ItemEntry {
                id: "x",
                attention: f64::NAN,
                exposure: 1.0,
            }]),
            scopes: None,
            centroids: None,
        };
        assert_eq!(
            e.merge_digest("p", &bad_entry, 1.0),
            Err(MergeError::InvalidEntry)
        );
        assert!(matches!(
            e.merge_digest("p", &e.clone().digest(&DigestSpec::default(), 0), f64::NAN),
            Err(MergeError::InvalidTrust(_))
        ));
    }

    #[test]
    fn snapshot_roundtrip_preserves_scores() {
        // String keys: deserialization must not borrow from the JSON buffer.
        type OwnedEngine = Engine<String, String, String>;
        let mut e: OwnedEngine = Engine::new(EngineConfig::default());
        let now = 100 * DAY_MS;
        let x = "x".to_string();
        e.upsert_item(
            x.clone(),
            ItemAttrs::new(vec![Scope::new(ScopeClass::AUTHOR, "alice".to_string())])
                .with_embedding(vec![1.0, 0.0]),
        );
        e.record(Event::Exposure {
            item: x.clone(),
            weight: 2.0,
            at: now,
        })
        .unwrap();
        e.record(Event::Dwell {
            item: x.clone(),
            millis: 30_000,
            at: now,
        })
        .unwrap();
        e.record(Event::Interaction {
            item: x.clone(),
            kind: InteractionKind::SHARE,
            at: now,
        })
        .unwrap();
        let mut source: OwnedEngine = Engine::new(EngineConfig::default());
        source
            .record(Event::Exposure {
                item: x.clone(),
                weight: 1.0,
                at: now,
            })
            .unwrap();
        source
            .record(Event::Dwell {
                item: x.clone(),
                millis: 10_000,
                at: now,
            })
            .unwrap();
        e.merge_digest(
            "p".to_string(),
            &source.digest(&DigestSpec::default(), now),
            0.5,
        )
        .unwrap();

        let json = serde_json::to_string(&e.snapshot()).unwrap();
        let snapshot: Snapshot<String, String, String> = serde_json::from_str(&json).unwrap();
        let restored = OwnedEngine::restore(snapshot, EngineConfig::default()).unwrap();

        for profile in [
            ScoringProfile::feed(),
            ScoringProfile::search(),
            ScoringProfile::replication(),
        ] {
            assert_eq!(
                e.score(&x, &profile, now + DAY_MS),
                restored.score(&x, &profile, now + DAY_MS)
            );
        }
    }

    #[test]
    fn restore_rejects_unknown_version() {
        let snapshot = Snapshot::<&str, &str, &str> {
            version: 42,
            ..Default::default()
        };
        assert_eq!(
            TestEngine::restore(snapshot, EngineConfig::default()).unwrap_err(),
            RestoreError::UnsupportedVersion(42)
        );
    }

    #[test]
    fn eviction_removes_lowest_retention_items() {
        let config = EngineConfig {
            max_items: 3,
            ..Default::default()
        };
        let mut e: TestEngine = Engine::new(config);
        let now = 100 * DAY_MS;
        let ids: [&'static str; 4] = ["a", "b", "c", "d"];
        for (i, id) in ids.iter().enumerate() {
            expose(&mut e, id, 1.0, now);
            dwell(&mut e, id, (i as u64 + 1) * 10, now);
        }
        assert_eq!(e.tracked_items(), 3);
        // "a" had the least attention and must be the one evicted.
        assert!(!e.items.contains_key("a"));
        for id in ["b", "c", "d"] {
            assert!(e.items.contains_key(id));
        }
    }

    #[test]
    fn thompson_sampling_varies_order_for_uncertain_items() {
        use rand::SeedableRng;
        let mut e = engine();
        let now = 100 * DAY_MS;
        // One established item and one nearly-unseen item.
        for i in 0..200 {
            expose(&mut e, "old", 1.0, now + i);
            dwell(&mut e, "old", 5, now + i);
        }
        expose(&mut e, "new", 1.0, now);
        let profile = ScoringProfile {
            exploration: Exploration::ThompsonSample,
            ..ScoringProfile::feed()
        };
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(11);
        let mut new_on_top = 0;
        for _ in 0..200 {
            let ranked = e.rank_with_rng(&["old", "new"], &profile, now + DAY_MS, &mut rng);
            if ranked[0].id == "new" {
                new_on_top += 1;
            }
        }
        // The uncertain item surfaces sometimes but not always.
        assert!(new_on_top > 0, "exploration never surfaced the new item");
        assert!(new_on_top < 200, "established item never won");
        // With exploration off the order is stable.
        let deterministic = e.rank(&["old", "new"], &ScoringProfile::feed(), now + DAY_MS);
        assert_eq!(deterministic[0].id, "old");
    }

    #[test]
    fn rank_boosted_multiplies_relevance() {
        let mut e = engine();
        let now = 100 * DAY_MS;
        for i in 0..20 {
            expose(&mut e, "engaging", 1.0, now + i);
            dwell(&mut e, "engaging", 20, now + i);
            expose(&mut e, "boring", 1.0, now + i);
            dwell(&mut e, "boring", 2, now + i);
        }
        let profile = ScoringProfile::search();
        // Without boost the engaging item wins...
        let plain = e.rank_boosted(&[("engaging", 1.0), ("boring", 1.0)], &profile, now);
        assert_eq!(plain[0].id, "engaging");
        // ...but a strong relevance boost lets the boring one overtake.
        let boosted = e.rank_boosted(&[("engaging", 1.0), ("boring", 100.0)], &profile, now);
        assert_eq!(boosted[0].id, "boring");
    }

    #[test]
    fn rank_scopes_orders_by_rolled_up_conversion() {
        let mut e = engine();
        let now = 100 * DAY_MS;
        e.upsert_item(
            "t1",
            ItemAttrs::new(vec![Scope::new(ScopeClass::TOPIC, "cats")]),
        );
        e.upsert_item(
            "t2",
            ItemAttrs::new(vec![Scope::new(ScopeClass::TOPIC, "tax")]),
        );
        for i in 0..30 {
            expose(&mut e, "t1", 1.0, now + i);
            dwell(&mut e, "t1", 15, now + i);
            expose(&mut e, "t2", 1.0, now + i);
            dwell(&mut e, "t2", 2, now + i);
        }
        let scopes = [
            Scope::new(ScopeClass::TOPIC, "tax"),
            Scope::new(ScopeClass::TOPIC, "cats"),
        ];
        let ranked = e.rank_scopes(&scopes, &ScoringProfile::replication(), now + DAY_MS);
        assert_eq!(ranked[0].scope.key, "cats");
        assert!(ranked[0].score.exposure_mass > 0.0);
    }

    #[test]
    fn digest_respects_spec_sections_and_caps() {
        let mut e = engine();
        let now = 100 * DAY_MS;
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            e.upsert_item(*id, ItemAttrs::new(vec![author("alice")]));
            expose(&mut e, id, 2.0, now);
            dwell(&mut e, id, (i as u64 + 1) * 10, now);
        }
        let spec = DigestSpec {
            max_items: 2,
            include_centroids: false,
            include_scopes: false,
            ..DigestSpec::default()
        };
        let digest = e.digest(&spec, now);
        let items = digest.items.unwrap();
        assert_eq!(items.len(), 2);
        // Top-N by attention: "c" then "b".
        assert_eq!(items[0].id, "c");
        assert_eq!(items[1].id, "b");
        assert!(digest.scopes.is_none());
        assert!(digest.centroids.is_none());
    }
}
