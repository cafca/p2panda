//! Layer 2: the personal predictive model.
//!
//! v1 combines hierarchical rate back-off (scope posteriors, precision- and
//! class-weighted, shrunk toward the global rate) with an optional
//! embedding-centroid taste model. Both are deliberately training-free.

use serde::{Deserialize, Serialize};

use crate::config::{CentroidConfig, PredictorConfig};
use crate::scopes::ScopeClass;
use crate::stats::{decay_weight, Prior, RatePosterior};

/// Decayed observations of one scope an item belongs to.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScopeEvidence {
    pub class: ScopeClass,
    /// Decayed attention units observed in this scope.
    pub attention: f64,
    /// Decayed exposure observed in this scope.
    pub exposure: f64,
}

/// Blends an item's scope-level evidence into a prior for the item's own
/// rate posterior. Implementations must be pure.
pub trait Predictor {
    fn prior(
        &self,
        evidence: &[ScopeEvidence],
        global: &RatePosterior,
        config: &PredictorConfig,
    ) -> Prior;
}

/// v1 predictor: class-weighted pooling of scope pseudo-counts, anchored by
/// the global rate at fixed strength.
///
/// The pooled rate is `(global_rate · strength + Σ w_c · attention_s) /
/// (strength + Σ w_c · exposure_s)`. Each scope's influence grows with its
/// own evidence (exposure), while the global anchor stays constant — a scope
/// the user has converted on a lot speaks loudly; a barely-seen scope
/// regresses to the global rate. Deliberately *not* precision-weighted
/// against the global posterior: the global aggregate is backed by all data
/// and would otherwise drown every scope signal.
///
/// Fully interpretable: an unseen item's predicted rate is a weighted average
/// of "how the user converts on this author / this content type / these
/// topics", falling back to the global rate when attributes carry no data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HierarchicalPredictor;

impl Predictor for HierarchicalPredictor {
    fn prior(
        &self,
        evidence: &[ScopeEvidence],
        global: &RatePosterior,
        config: &PredictorConfig,
    ) -> Prior {
        let anchor = config.prior_strength.max(0.0);
        let mut attention = global.mean() * anchor;
        let mut exposure = anchor;
        for ev in evidence {
            let class_weight = *config
                .class_weights
                .get(&ev.class)
                .unwrap_or(&config.default_class_weight);
            attention += class_weight * ev.attention;
            exposure += class_weight * ev.exposure;
        }
        let rate = if exposure > 0.0 {
            attention / exposure
        } else {
            global.mean()
        };
        Prior {
            rate,
            strength: config.prior_strength,
        }
    }
}

/// One interest centroid: a direction in embedding space with accumulated,
/// slowly decaying attention mass.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Centroid {
    pub vector: Vec<f32>,
    pub mass: f64,
    /// Fractional day of the last mass update (for lazy decay).
    pub rep_day: f64,
}

/// The taste model: up to K attention-weighted centroids over the embeddings
/// of items the user converted on. Updates are online nearest-centroid EMA —
/// no training loop.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CentroidModel {
    pub centroids: Vec<Centroid>,
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b.iter()) {
        dot += *x as f64 * *y as f64;
        na += *x as f64 * *x as f64;
        nb += *y as f64 * *y as f64;
    }
    if na <= 0.0 || nb <= 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

fn normalize(v: &mut [f32]) {
    let norm: f64 = v.iter().map(|x| *x as f64 * *x as f64).sum::<f64>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x = (*x as f64 / norm) as f32;
        }
    }
}

impl CentroidModel {
    /// Fold one converted item's embedding into the taste model with the
    /// given attention mass at fractional day `day`.
    pub fn observe(&mut self, embedding: &[f32], mass: f64, day: f64, config: &CentroidConfig) {
        if embedding.is_empty() || mass <= 0.0 || config.k == 0 {
            return;
        }
        // Lazily decay all centroid masses to `day`.
        for c in &mut self.centroids {
            let age = (day - c.rep_day).max(0.0);
            c.mass *= decay_weight(age, config.half_life_days);
            c.rep_day = day;
        }

        let best = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, cosine(embedding, &c.vector)))
            .max_by(|a, b| a.1.total_cmp(&b.1));

        match best {
            Some((idx, sim))
                if sim >= config.min_merge_similarity || self.centroids.len() >= config.k =>
            {
                let c = &mut self.centroids[idx];
                let lr = mass / (mass + c.mass.max(1e-9));
                for (cv, ev) in c.vector.iter_mut().zip(embedding.iter()) {
                    *cv = (*cv as f64 * (1.0 - lr) + *ev as f64 * lr) as f32;
                }
                normalize(&mut c.vector);
                c.mass += mass;
            }
            _ => {
                let mut vector = embedding.to_vec();
                normalize(&mut vector);
                self.centroids.push(Centroid {
                    vector,
                    mass,
                    rep_day: day,
                });
            }
        }
    }

    /// Affinity of an embedding to the taste model in `[0, 1]`: the best
    /// cosine similarity across centroids, clamped at zero. `None` when the
    /// model has no centroids yet.
    pub fn affinity(&self, embedding: &[f32]) -> Option<f64> {
        if self.centroids.is_empty() || embedding.is_empty() {
            return None;
        }
        let best = self
            .centroids
            .iter()
            .map(|c| cosine(embedding, &c.vector))
            .fold(f64::MIN, f64::max);
        Some(best.clamp(0.0, 1.0))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn posterior(mean: f64, exposure: f64) -> RatePosterior {
        RatePosterior {
            alpha: mean * exposure,
            beta: exposure,
        }
    }

    fn author_evidence(rate: f64, exposure: f64) -> ScopeEvidence {
        ScopeEvidence {
            class: ScopeClass::AUTHOR,
            attention: rate * exposure,
            exposure,
        }
    }

    #[test]
    fn unseen_item_prior_follows_strong_scope_signal() {
        let predictor = HierarchicalPredictor;
        let config = PredictorConfig::default();
        let global = posterior(1.0, 50.0);
        // The user converts 5x baseline on this author, with plenty of data.
        let prior = predictor.prior(&[author_evidence(5.0, 500.0)], &global, &config);
        assert!(prior.rate > 3.0, "prior rate {}", prior.rate);
    }

    #[test]
    fn weak_scope_signal_stays_near_global() {
        let predictor = HierarchicalPredictor;
        let config = PredictorConfig::default();
        let global = posterior(1.0, 500.0);
        // A single noisy observation on this author: raw rate 50 over 0.5
        // exposure must stay bounded near the global rate, far from 50.
        let prior = predictor.prior(&[author_evidence(50.0, 0.5)], &global, &config);
        assert!(prior.rate < 10.0, "prior rate {}", prior.rate);
    }

    #[test]
    fn scope_influence_grows_with_evidence_not_global_volume() {
        // The global aggregate having vastly more data must not drown a
        // well-evidenced scope signal (the precision-weighting failure mode).
        let predictor = HierarchicalPredictor;
        let config = PredictorConfig::default();
        let global = posterior(10.0, 1_000_000.0);
        let prior = predictor.prior(&[author_evidence(1.0, 500.0)], &global, &config);
        assert!(prior.rate < 2.0, "prior rate {}", prior.rate);
    }

    #[test]
    fn class_weights_modulate_influence() {
        let predictor = HierarchicalPredictor;
        let config = PredictorConfig {
            class_weights: BTreeMap::new(),
            default_class_weight: 0.0,
            ..Default::default()
        };
        let global = posterior(1.0, 50.0);
        // With zero class weight the scope signal is ignored entirely.
        let prior = predictor.prior(&[author_evidence(5.0, 500.0)], &global, &config);
        assert!((prior.rate - 1.0).abs() < 1e-9);
    }

    #[test]
    fn no_scopes_falls_back_to_global() {
        let predictor = HierarchicalPredictor;
        let config = PredictorConfig::default();
        let global = posterior(2.5, 100.0);
        let prior = predictor.prior(&[], &global, &config);
        assert!((prior.rate - 2.5).abs() < 1e-9);
    }

    #[test]
    fn centroids_learn_and_score_taste() {
        let config = CentroidConfig::default();
        let mut model = CentroidModel::default();
        // Two distinct interests.
        model.observe(&[1.0, 0.0, 0.0], 30.0, 0.0, &config);
        model.observe(&[0.9, 0.1, 0.0], 30.0, 1.0, &config);
        model.observe(&[0.0, 0.0, 1.0], 30.0, 2.0, &config);
        assert_eq!(model.centroids.len(), 2);
        let close = model.affinity(&[1.0, 0.05, 0.0]).unwrap();
        let far = model.affinity(&[0.0, 1.0, 0.0]).unwrap();
        assert!(close > 0.9);
        assert!(far < 0.3);
    }

    #[test]
    fn centroid_count_is_capped_at_k() {
        let config = CentroidConfig {
            k: 2,
            ..Default::default()
        };
        let mut model = CentroidModel::default();
        model.observe(&[1.0, 0.0, 0.0, 0.0], 10.0, 0.0, &config);
        model.observe(&[0.0, 1.0, 0.0, 0.0], 10.0, 0.0, &config);
        model.observe(&[0.0, 0.0, 1.0, 0.0], 10.0, 0.0, &config);
        model.observe(&[0.0, 0.0, 0.0, 1.0], 10.0, 0.0, &config);
        assert_eq!(model.centroids.len(), 2);
    }

    #[test]
    fn no_affinity_without_centroids() {
        let model = CentroidModel::default();
        assert!(model.affinity(&[1.0, 0.0]).is_none());
    }
}
