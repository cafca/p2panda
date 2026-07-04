//! Rate estimation: Bayesian-smoothed conversion rates with uncertainty.
//!
//! Everything scores through a smoothed rate: `(attention + prior_rate ·
//! prior_strength) / (exposure + prior_strength)`, interpreted as the mean of
//! a Gamma posterior over the rate. Uncertainty shrinks with effective
//! exposure and powers Thompson-sampling exploration.

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Exponential decay weight for mass of the given age.
pub fn decay_weight(age_days: f64, half_life_days: f64) -> f64 {
    if !half_life_days.is_finite() {
        return 1.0;
    }
    if half_life_days <= 0.0 {
        return if age_days <= 0.0 { 1.0 } else { 0.0 };
    }
    0.5_f64.powf(age_days.max(0.0) / half_life_days)
}

/// Prior belief about a conversion rate: a mean rate and a strength in
/// exposure units (how much observed exposure it takes to outweigh it).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Prior {
    pub rate: f64,
    pub strength: f64,
}

/// Gamma posterior over a conversion rate, parameterized by shape `alpha`
/// (pseudo-attention) and rate `beta` (pseudo-exposure).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RatePosterior {
    pub alpha: f64,
    pub beta: f64,
}

impl RatePosterior {
    /// Posterior mean rate (attention units per exposure unit).
    pub fn mean(&self) -> f64 {
        if self.beta <= 0.0 {
            return 0.0;
        }
        self.alpha / self.beta
    }

    /// Posterior variance.
    pub fn var(&self) -> f64 {
        if self.beta <= 0.0 {
            return 0.0;
        }
        self.alpha / (self.beta * self.beta)
    }

    /// Posterior standard deviation.
    pub fn std(&self) -> f64 {
        self.var().sqrt()
    }

    /// Draw one rate from the posterior (Thompson sampling).
    pub fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> f64 {
        if self.alpha <= 0.0 || self.beta <= 0.0 {
            return 0.0;
        }
        gamma_sample(self.alpha, rng) / self.beta
    }
}

/// Turns decayed pseudo-counts plus a prior into a rate posterior.
///
/// v1 ships [`PseudoCountEstimator`]. An inverse-propensity-scoring
/// implementation is the reserved upgrade path; it must not require ledger
/// changes.
pub trait RateEstimator {
    fn estimate(&self, attention: f64, exposure: f64, prior: Prior) -> RatePosterior;
}

/// Pseudo-count smoothing: the prior contributes `rate · strength` attention
/// and `strength` exposure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PseudoCountEstimator;

impl RateEstimator for PseudoCountEstimator {
    fn estimate(&self, attention: f64, exposure: f64, prior: Prior) -> RatePosterior {
        RatePosterior {
            alpha: (prior.rate * prior.strength + attention).max(0.0),
            beta: (prior.strength + exposure).max(f64::MIN_POSITIVE),
        }
    }
}

/// Standard normal sample via Box–Muller.
fn normal_sample<R: Rng + ?Sized>(rng: &mut R) -> f64 {
    // 1 - U keeps the argument of ln strictly positive (U ∈ [0, 1)).
    let u1: f64 = 1.0 - rng.random::<f64>();
    let u2: f64 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// Gamma(shape, 1) sample via Marsaglia–Tsang, with the boost trick for
/// shape < 1.
fn gamma_sample<R: Rng + ?Sized>(shape: f64, rng: &mut R) -> f64 {
    if shape <= 0.0 {
        return 0.0;
    }
    if shape < 1.0 {
        let u: f64 = rng.random::<f64>().max(f64::MIN_POSITIVE);
        return gamma_sample(shape + 1.0, rng) * u.powf(1.0 / shape);
    }
    let d = shape - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();
    loop {
        let x = normal_sample(rng);
        let v = (1.0 + c * x).powi(3);
        if v <= 0.0 {
            continue;
        }
        let u: f64 = rng.random();
        if u < 1.0 - 0.0331 * x.powi(4) {
            return d * v;
        }
        if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
            return d * v;
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;

    #[test]
    fn smoothing_formula_matches_spec() {
        let est = PseudoCountEstimator;
        let post = est.estimate(
            30.0,
            10.0,
            Prior {
                rate: 1.0,
                strength: 10.0,
            },
        );
        // (30 + 1.0 * 10) / (10 + 10) = 2.0
        assert!((post.mean() - 2.0).abs() < 1e-12);
    }

    #[test]
    fn low_exposure_items_regress_to_prior() {
        let est = PseudoCountEstimator;
        let prior = Prior {
            rate: 1.0,
            strength: 10.0,
        };
        // One impression with a huge dwell: raw rate 120, smoothed far lower.
        let noisy = est.estimate(120.0, 1.0, prior);
        assert!(noisy.mean() < 12.0);
        // The same rate observed over lots of exposure survives smoothing.
        let solid = est.estimate(12_000.0, 100.0, prior);
        assert!(solid.mean() > 100.0);
    }

    #[test]
    fn uncertainty_shrinks_with_exposure() {
        let est = PseudoCountEstimator;
        let prior = Prior {
            rate: 1.0,
            strength: 10.0,
        };
        let a = est.estimate(2.0, 2.0, prior);
        let b = est.estimate(200.0, 200.0, prior);
        assert!(b.std() < a.std());
    }

    #[test]
    fn gamma_sampler_matches_moments() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        for shape in [0.5, 1.0, 4.2, 20.0] {
            let n = 20_000;
            let mean: f64 = (0..n).map(|_| gamma_sample(shape, &mut rng)).sum::<f64>() / n as f64;
            assert!(
                (mean - shape).abs() / shape < 0.05,
                "shape {shape}: mean {mean}"
            );
        }
    }

    #[test]
    fn posterior_sampling_concentrates() {
        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let post = RatePosterior {
            alpha: 400.0,
            beta: 100.0,
        };
        let n = 5_000;
        let mean: f64 = (0..n).map(|_| post.sample(&mut rng)).sum::<f64>() / n as f64;
        assert!((mean - 4.0).abs() < 0.05);
    }

    #[test]
    fn decay_weight_basics() {
        assert_eq!(decay_weight(0.0, 7.0), 1.0);
        assert!((decay_weight(7.0, 7.0) - 0.5).abs() < 1e-12);
        assert_eq!(decay_weight(5.0, f64::INFINITY), 1.0);
        assert_eq!(decay_weight(-3.0, 7.0), 1.0);
    }
}
