//! Time-bucketed accumulators with log-compaction and query-time decay.
//!
//! Buckets store plain sums for their period — nothing decayed is ever
//! stored. Exponential decay with a per-query half-life is applied when
//! reading, to both attention (numerator) and exposure (denominator), so one
//! dataset serves consumers with different time horizons.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::{CompactionPolicy, WeightProfile};
use crate::events::InteractionKind;
use crate::stats::decay_weight;

/// Span marker for the terminal bucket holding all mass older than the
/// monthly horizon.
const TERMINAL_SPAN: u32 = u32::MAX;

/// Plain sums of signals within one bucket period.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Accum {
    /// Prominence-weighted exposure (opportunity granted).
    pub exposure: f64,
    /// Dwell milliseconds (attention byproduct).
    pub dwell_ms: f64,
    /// Interaction counts by kind.
    #[serde(with = "crate::serde_util")]
    pub interactions: BTreeMap<InteractionKind, f64>,
    /// Number of recorded events, used to weight the representative day when
    /// buckets merge.
    pub events: f64,
}

impl Accum {
    pub fn merge(&mut self, other: &Accum) {
        self.exposure += other.exposure;
        self.dwell_ms += other.dwell_ms;
        for (kind, count) in &other.interactions {
            *self.interactions.entry(*kind).or_insert(0.0) += count;
        }
        self.events += other.events;
    }

    /// Convert the structured signals into scalar attention units.
    pub fn attention_units(&self, profile: &WeightProfile) -> f64 {
        let mut units = self.dwell_ms / 1000.0 * profile.units_per_dwell_second;
        for (kind, count) in &self.interactions {
            units += count * profile.interaction_weight(*kind);
        }
        units
    }
}

/// One time bucket: a period plus the sums recorded within it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    /// First day (whole days since epoch) covered by this bucket.
    pub start_day: u32,
    /// Days covered; `u32::MAX` marks the terminal bucket.
    pub span_days: u32,
    /// Event-weighted mean day of the mass inside, in fractional days.
    /// Decay is computed from this, which keeps compaction error small.
    pub rep_day: f64,
    pub accum: Accum,
}

impl Bucket {
    fn contains(&self, day: u32) -> bool {
        if self.span_days == TERMINAL_SPAN {
            return day >= self.start_day;
        }
        day >= self.start_day && day < self.start_day + self.span_days
    }

    fn merge(&mut self, other: &Bucket) {
        let total = self.accum.events + other.accum.events;
        self.rep_day = if total > 0.0 {
            (self.rep_day * self.accum.events + other.rep_day * other.accum.events) / total
        } else {
            self.rep_day.min(other.rep_day)
        };
        self.start_day = self.start_day.min(other.start_day);
        if self.span_days != TERMINAL_SPAN && other.span_days != TERMINAL_SPAN {
            let self_end = self.start_day + self.span_days;
            let other_end = other.start_day + other.span_days;
            self.span_days = self_end.max(other_end) - self.start_day;
        } else {
            self.span_days = TERMINAL_SPAN;
        }
        self.accum.merge(&other.accum);
    }
}

/// A bucketed accumulator series for one item, one scope, or the global
/// aggregate. Buckets are kept sorted by `start_day`, newest last.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Series {
    pub buckets: Vec<Bucket>,
}

impl Series {
    /// Record an event at fractional day `day`, applying `f` to the bucket's
    /// accumulator. Out-of-order events land in whichever bucket covers their
    /// day.
    pub fn record(&mut self, day: f64, f: impl FnOnce(&mut Accum)) {
        let whole_day = day.max(0.0).floor() as u32;
        // Newest buckets are the common case; search from the back.
        if let Some(idx) = self.buckets.iter().rposition(|b| b.contains(whole_day)) {
            let bucket = &mut self.buckets[idx];
            let events = bucket.accum.events;
            bucket.rep_day = (bucket.rep_day * events + day) / (events + 1.0);
            bucket.accum.events += 1.0;
            f(&mut bucket.accum);
            return;
        }
        let mut accum = Accum {
            events: 1.0,
            ..Default::default()
        };
        f(&mut accum);
        let bucket = Bucket {
            start_day: whole_day,
            span_days: 1,
            rep_day: day,
            accum,
        };
        let pos = self
            .buckets
            .partition_point(|b| b.start_day <= bucket.start_day);
        self.buckets.insert(pos, bucket);
    }

    /// Compact if the series has grown beyond the policy's live-bucket cap.
    pub fn maybe_compact(&mut self, now_day: f64, policy: &CompactionPolicy) {
        if self.buckets.len() > policy.max_live_buckets {
            self.compact(now_day, policy);
        }
    }

    /// Merge buckets into coarser spans according to their age: daily within
    /// the daily horizon, then 7-day, then 28-day spans, then one terminal
    /// bucket. Spans are aligned to an absolute epoch grid so repeated
    /// compaction is stable.
    pub fn compact(&mut self, now_day: f64, policy: &CompactionPolicy) {
        let now = now_day.max(0.0).floor() as u32;
        let mut merged: BTreeMap<(u32, u32), Bucket> = BTreeMap::new();
        for bucket in self.buckets.drain(..) {
            let age = now.saturating_sub(bucket.rep_day.max(0.0) as u32);
            let target_span = if age <= policy.daily_horizon_days {
                1
            } else if age <= policy.weekly_horizon_days {
                7
            } else if age <= policy.monthly_horizon_days {
                28
            } else {
                TERMINAL_SPAN
            };
            let key = if target_span == TERMINAL_SPAN {
                (0, TERMINAL_SPAN)
            } else {
                (
                    bucket.start_day - bucket.start_day % target_span,
                    target_span,
                )
            };
            match merged.get_mut(&key) {
                Some(existing) => existing.merge(&bucket),
                None => {
                    let mut b = bucket;
                    if b.span_days != TERMINAL_SPAN {
                        // Snap to the grid window; widen to the target span
                        // unless the bucket already covers more.
                        b.span_days = (key.0 + target_span.max(b.span_days))
                            .saturating_sub(key.0.min(b.start_day))
                            .max(target_span);
                        b.start_day = key.0.min(b.start_day);
                    } else {
                        b.start_day = 0;
                    }
                    merged.insert(key, b);
                }
            }
        }
        self.buckets = merged.into_values().collect();
        self.buckets.sort_by_key(|b| b.start_day);
    }

    /// Decayed (attention units, exposure) as of fractional day `now_day`,
    /// with the given half-life in days. Both sides decay with the same
    /// weight, so the rate is recency-weighted while staying a true ratio.
    pub fn decayed(
        &self,
        now_day: f64,
        half_life_days: f64,
        profile: &WeightProfile,
    ) -> (f64, f64) {
        let mut attention = 0.0;
        let mut exposure = 0.0;
        for bucket in &self.buckets {
            let age = (now_day - bucket.rep_day).max(0.0);
            let w = decay_weight(age, half_life_days);
            attention += bucket.accum.attention_units(profile) * w;
            exposure += bucket.accum.exposure * w;
        }
        (attention, exposure)
    }

    /// Undecayed attention units in the daily bucket covering `day`, used to
    /// enforce the per-item daily attention cap.
    pub fn units_on_day(&self, day: f64, profile: &WeightProfile) -> f64 {
        let whole_day = day.max(0.0).floor() as u32;
        self.buckets
            .iter()
            .rev()
            .find(|b| b.contains(whole_day))
            .map(|b| b.accum.attention_units(profile))
            .unwrap_or(0.0)
    }

    /// Total number of recorded events (undecayed).
    pub fn event_count(&self) -> f64 {
        self.buckets.iter().map(|b| b.accum.events).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wp() -> WeightProfile {
        WeightProfile::default()
    }

    #[test]
    fn records_into_daily_buckets() {
        let mut s = Series::default();
        s.record(100.5, |a| a.exposure += 1.0);
        s.record(100.7, |a| a.dwell_ms += 5_000.0);
        s.record(101.2, |a| a.exposure += 2.0);
        assert_eq!(s.buckets.len(), 2);
        assert_eq!(s.buckets[0].start_day, 100);
        assert_eq!(s.buckets[0].accum.exposure, 1.0);
        assert_eq!(s.buckets[0].accum.dwell_ms, 5_000.0);
        assert_eq!(s.buckets[1].accum.exposure, 2.0);
        // rep_day is the event-weighted mean.
        assert!((s.buckets[0].rep_day - 100.6).abs() < 1e-9);
    }

    #[test]
    fn out_of_order_events_find_their_bucket() {
        let mut s = Series::default();
        s.record(200.0, |a| a.exposure += 1.0);
        s.record(100.0, |a| a.exposure += 1.0);
        assert_eq!(s.buckets.len(), 2);
        assert!(s.buckets[0].start_day < s.buckets[1].start_day);
        s.record(100.9, |a| a.exposure += 1.0);
        assert_eq!(s.buckets.len(), 2);
        assert_eq!(s.buckets[0].accum.exposure, 2.0);
    }

    #[test]
    fn decay_full_weight_when_fresh_and_half_at_half_life() {
        let mut s = Series::default();
        s.record(100.0, |a| a.dwell_ms += 10_000.0);
        let (att_now, _) = s.decayed(100.0, 2.0, &wp());
        assert!((att_now - 10.0).abs() < 1e-9);
        let (att_later, _) = s.decayed(102.0, 2.0, &wp());
        assert!((att_later - 5.0).abs() < 1e-9);
    }

    #[test]
    fn decayed_is_monotonically_non_increasing_in_time() {
        let mut s = Series::default();
        for d in 0..40 {
            s.record(d as f64 + 0.3, |a| {
                a.exposure += 1.0;
                a.dwell_ms += 3_000.0;
            });
        }
        for half_life in [1.0, 7.0, 30.0] {
            let mut last = f64::INFINITY;
            for t in 40..80 {
                let (att, exp) = s.decayed(t as f64, half_life, &wp());
                assert!(att <= last + 1e-12);
                assert!(exp >= 0.0);
                last = att;
            }
        }
    }

    #[test]
    fn compaction_bounds_bucket_count_and_preserves_totals() {
        let mut s = Series::default();
        for d in 0..400 {
            s.record(d as f64, |a| {
                a.exposure += 1.0;
                a.dwell_ms += 1_000.0;
            });
        }
        let before_events = s.event_count();
        let policy = CompactionPolicy::default();
        s.compact(400.0, &policy);
        assert!(s.buckets.len() <= 30, "got {} buckets", s.buckets.len());
        assert_eq!(s.event_count(), before_events);
        let (att, exp) = s.decayed(400.0, f64::INFINITY, &wp());
        assert!((att - 400.0).abs() < 1e-6);
        assert!((exp - 400.0).abs() < 1e-6);
    }

    #[test]
    fn compaction_error_is_bounded() {
        // Compare decayed scores before and after compaction across
        // half-lives; rep_day weighting keeps the error small.
        let mut s = Series::default();
        for d in 0..365 {
            let mass = 1.0 + (d % 13) as f64;
            s.record(d as f64 + 0.5, |a| {
                a.exposure += mass;
                a.dwell_ms += mass * 2_000.0;
            });
        }
        let now = 366.0;
        let policy = CompactionPolicy::default();
        for half_life in [1.0, 2.0, 7.0, 30.0, 90.0] {
            let (att_before, exp_before) = s.decayed(now, half_life, &wp());
            let mut compacted = s.clone();
            compacted.compact(now, &policy);
            let (att_after, exp_after) = compacted.decayed(now, half_life, &wp());
            let rel = |a: f64, b: f64| (a - b).abs() / a.abs().max(1e-9);
            assert!(
                rel(att_before, att_after) < 0.05,
                "attention error {} at half-life {half_life}",
                rel(att_before, att_after)
            );
            assert!(
                rel(exp_before, exp_after) < 0.05,
                "exposure error {} at half-life {half_life}",
                rel(exp_before, exp_after)
            );
        }
    }

    #[test]
    fn repeated_compaction_is_stable() {
        let mut s = Series::default();
        for d in 0..365 {
            s.record(d as f64, |a| a.exposure += 1.0);
        }
        let policy = CompactionPolicy::default();
        s.compact(365.0, &policy);
        let once = s.clone();
        s.compact(365.0, &policy);
        assert_eq!(once, s);
    }
}
