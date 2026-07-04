# salience — attention estimation for ranking and replication

**Status:** draft for review · v1 specification
**Crate:** `salience` (standalone, not a workspace member of p2panda)
**License:** MIT OR Apache-2.0

## 1. Purpose

`salience` estimates the attention that content items will receive, based on
records of past attention spending. It is a pure, synchronous Rust library with
no I/O, no async, and no ML runtime. It is designed to serve three consumers in
a peer-to-peer social media application (to be built on p2panda, but the crate
is fully independent of it):

1. **Home feed** — rank candidate items by the attention the local user is
   predicted to spend on them.
2. **Search** — re-rank relevance-matched results by predicted attention.
3. **Replication** — prioritize what to fetch, keep and offer in a p2p network,
   at the granularities the network actually decides on (per item / per scope).

## 2. Conceptual model

Attention is a finite resource that users spend on content. We cannot measure
the spending decision directly; we measure its byproducts — dwell time and
interactions — and we measure the **opportunity** the app granted.

Every time the app shows an item it makes a bet, granting the item a slice of
the user's attention budget (an **exposure**, weighted by prominence). The user
then converts some fraction of that opportunity into actual attention. The
central quantity of the crate is the **conversion rate**:

```
attention_rate = attention spent / exposure granted
```

Ranking on the rate rather than on accumulated attention removes position bias:
an item pinned at the top of a feed collects dwell regardless of quality, but
its *rate* only rises if users convert. When the app bets on an item and users
do not convert, the rate falls and the bet is withdrawn — the system is
self-correcting instead of self-reinforcing.

### 2.1 Two layers, blended per consumer

- **Layer 1 — item ledger (observed).** Per-item accumulated exposure and
  attention, locally observed plus reported by peers. Answers "how much do
  people demonstrably care about this item?"
- **Layer 2 — personal predictive model.** Learned from the local user's own
  spending, keyed by item *attributes* (author, content type, topics, optional
  semantic embedding). Answers "how much attention would I likely spend on this
  item, even if nobody has seen it yet?"

Item data identifies but does not generalize; attribute data generalizes but
does not identify. Each consumer blends the two layers with its own weights
(see §8): feed leans on layer 2, replication leans on layer 1, search uses
layer 2 as a boost on external relevance.

## 3. Inputs

All ingest is via typed events with **explicit timestamps** (`u64` UNIX
milliseconds). The engine never reads the clock or any global RNG.

```
Exposure    { item, weight: f64, at }   // app's bet; 1.0 = one standard impression
Dwell       { item, millis: u64, at }   // time visibly spent on the item
Interaction { item, kind, at }          // reaction, reply, share, click, media-play, …
```

- **Exposure weight** is supplied by the app and encodes prominence (e.g.
  `visible_area × visible_time`, normalized so a typical feed-slot impression
  ≈ 1.0). Sending a constant `1.0` degrades gracefully to flat impression
  counting. The estimator sits behind a trait so an inverse-propensity-scoring
  implementation can replace it later without changing the ledger (§10).
- **Dwell clamping:** each dwell event is clamped to a configurable
  `max_dwell_per_event` (default 120 s), and per-item daily attention is capped
  by `max_attention_per_item_per_day`, so an abandoned open tab is not four
  hours of attention.
- **InteractionKind** is an app-defined newtype (`InteractionKind(u16)`); the
  crate ships conventional constants (`REACTION`, `REPLY`, `SHARE`, `CLICK`,
  `MEDIA_PLAY`) but any value is accepted.

### 3.1 Attention currency

Raw signals are stored structured (never collapsed at ingest). A configurable
`WeightProfile` converts a signal vector into scalar **attention units** at
query time (dwell seconds × 1.0 as the base; per-`InteractionKind` weights,
e.g. reaction ≈ 5, reply ≈ 30, share ≈ 60 units). Weights therefore remain
tunable forever without reprocessing history.

### 3.2 Item attributes

Items are registered with their attributes before/alongside events:

```
ItemAttrs {
    scopes:    Vec<Scope<S>>,        // author, content type, topics, … (§6)
    embedding: Option<Vec<f32>>,     // optional semantic vector, opaque to the crate
}
```

The embedding, if present, is produced by the app (e.g. a pretrained sentence
or image model). The crate never runs or depends on an ML model; it only
consumes vectors.

## 4. Storage model: bucketed accumulators with query-time decay

Per tracked item (and per scope, §6) the ledger keeps a small set of
**time buckets**. Each bucket stores plain sums for its period: exposure,
dwell millis, interaction counts per kind. Nothing decayed is ever stored.

- **Log-compaction:** recent activity in daily buckets; as buckets age they are
  merged into weekly, then monthly buckets. Per-item storage is bounded at
  ~10 buckets (configurable horizon, default ≈ 12 months, older mass merges
  into a terminal bucket).
- **Query-time exponential decay:** every score is computed as

  ```
  decayed(x) = Σ over buckets  x_bucket · 0.5 ^ (bucket_age / half_life)
  ```

  applied to **both** numerator (attention units) and denominator (exposure).
  The half-life is a parameter of the query, not of storage — the feed can
  score with a 2-day half-life while replication uses 30 days over the same
  data. Compaction error is small precisely where decay weight is small.

## 5. Statistics: smoothed rates with uncertainty

A raw ratio is noise for low-exposure items (one impression + one long dwell =
"100 % conversion"). All rates are **Bayesian-smoothed** toward a prior:

```
smoothed_rate = (decayed_attention + prior_rate · prior_strength)
              / (decayed_exposure  + prior_strength)
```

interpreted as a Gamma posterior over the rate, so every estimate carries
**uncertainty** (variance shrinking with effective exposure). Uncertainty is
queryable and powers exploration (§8). `prior_rate` defaults to the engine's
own decayed global rate; `prior_strength` (in exposure units) is configurable.

The estimator is behind a trait:

```rust
trait RateEstimator {
    fn estimate(&self, attention: f64, exposure: f64, prior: Prior) -> RatePosterior;
}
```

v1 ships the pseudo-count/Gamma implementation; an IPW-corrected estimator is
an explicit future upgrade path and must not require ledger changes.

## 6. Scopes: attribute keys and roll-ups

A **scope** is an app-defined grouping key an item belongs to:

```rust
struct Scope<S> { class: ScopeClass, key: S }   // ScopeClass(u16): AUTHOR, KIND, TOPIC, …
```

`S` is generic (`Clone + Eq + Ord + Hash + Serialize + Deserialize`). The app
maps scopes onto its real concepts — for a p2panda app: authors (public keys),
topics (sync topics), logs — but the crate never knows those types.

Every ingested event also updates the bucketed accumulators of each scope the
item belongs to. This yields **roll-up rates per scope**, which serve double
duty:

- as the predictive features of layer 2 (§7), and
- as direct answers to replication's coarse-grained questions ("how much
  attention does topic X convert?") via `rank_scopes` (§8).

This matches p2panda's replication granularities: blob fetch/eviction decisions
use per-item scores; topic subscription and sync prioritization use scope
scores; the app defines the mapping.

## 7. Predictor (layer 2)

Behind a trait, so implementations can be swapped:

```rust
trait Predictor {
    fn prior(&self, evidence: &[ScopeEvidence], global: &RatePosterior,
             config: &PredictorConfig) -> Prior;
}
```

v1 implementation — **hierarchical rate back-off + optional embedding
affinity**:

1. **Hierarchical back-off via evidence-weighted pooling.** An unseen item's
   prior rate pools its scopes' decayed pseudo-counts, weighted per
   `ScopeClass`, anchored by the global rate at fixed strength:
   `(global_rate · strength + Σ w_c · attention_s) / (strength + Σ w_c ·
   exposure_s)`. A scope's influence grows with its own evidence; barely-seen
   scopes regress to the global rate. (Deliberately *not* precision-weighted
   against the global posterior — the global aggregate is backed by all data
   and would drown every scope signal.) Scope evidence and the global anchor
   decay with a dedicated **taste half-life** (default 30 days), independent
   of the query's half-life: a feed scoring with a 2-day horizon must not
   forget what the user likes at that pace. As the item accrues its own
   exposure, its own posterior takes over — the same smoothing formula of §5,
   with the attribute blend as `prior_rate`. Fully interpretable: "ranked up
   because you convert 4× baseline on this author."
2. **Interest centroids (optional).** The engine maintains K (default 4)
   attention-weighted, slowly-decaying centroids over the embeddings of items
   the user converted on (online nearest-centroid EMA update; no training
   loop). An item with an embedding gets an affinity in [0, 1] (max cosine
   similarity across centroids, rescaled), entering the prediction as one more
   weighted component. Items or engines without embeddings skip this entirely.

Explicitly **out of scope**: behavioural collaborative-filtering embeddings
(matrix factorization / two-tower models). They require training on a global
interaction matrix that no p2p node has, and cannot be sourced pretrained.

## 8. Query API

Batch-first ranking over a profile:

```rust
engine.rank(&[I], &ScoringProfile, now) -> Vec<Ranked<I>>
engine.rank_scopes(&[Scope<S>], &ScoringProfile, now) -> Vec<RankedScope<S>>

struct ScoringProfile {
    half_life:    Duration,
    weights:      Blend { item_rate, predicted_rate, peer_rate, affinity },
    exploration:  Exploration,        // Off | ThompsonSample
    prior_strength: f64,
    // …
}
```

- **Presets as starting points, not policy:** `ScoringProfile::feed()`,
  `::search()`, `::replication()` encode recommended defaults (feed: short
  half-life, predictor-heavy; replication: long half-life, ledger- and
  peer-heavy); the app owns the tuning.
- **Scores are explainable:** `Score` exposes its components (item rate,
  predicted rate, peer rate, affinity, uncertainty, effective exposure), not
  just a float — rankings must be debuggable.
- **Exploration:** with `ThompsonSample`, ranking sorts by a sample from each
  item's rate posterior instead of its mean, giving uncertain (new) items a
  principled chance at exposure. Sampling APIs take `&mut impl rand::Rng`;
  `Off` involves no randomness. Posterior sampling alone cannot escape an
  informative-but-wrong prior once a feedback loop has starved an item class
  of exposure — apps should pair it with an exploration floor (e.g. reserve a
  feed slot for a random/low-evidence candidate), which is the app's half of
  the explore/exploit deal (validated in the simulation test).
- **Search integration:** relevance stays outside the crate; the app combines
  our score with its relevance score (optionally via a per-candidate boost
  multiplier parameter on `rank`).
- **Determinism:** with `Exploration::Off`, output ordering is fully
  deterministic; ties break on item id.

## 9. Peer digests: broadcast, receive, merge

The broadcastable artifact is a versioned, serde-serializable, **tiered**
envelope; every section is independently omittable (privacy is an app-level
dial — item digests reveal what you viewed, scope digests reveal aggregate
interests, centroids sketch your taste):

```rust
struct Digest<I, S> {          // v1: DigestV1
    produced_at: u64,
    items:     Option<Vec<ItemEntry<I>>>,    // top-N by decayed attention: id, attention, exposure
    scopes:    Option<Vec<ScopeEntry<S>>>,   // roll-up pseudo-counts per scope
    centroids: Option<Vec<Vec<f32>>>,
}
```

- **Produce:** `engine.digest(&DigestSpec, now)` — decayed pseudo-counts
  relative to `produced_at`, top-N caps per section.
- **Consume:** `engine.merge_digest(peer_id, digest, trust)` with
  `trust ∈ [0.0, 1.0]` supplied by the app (social distance, explicit lists —
  app policy, never crate policy). Merging adds the digest's pseudo-counts,
  scaled by trust, into a **peer pool kept separate from local observations**;
  peer influence surfaces only through the `peer_rate` score component, bounded
  by its blend weight and by trust.
- **Idempotent per peer:** a new digest from the same `peer_id` **replaces**
  that peer's previous contribution (no double counting across re-broadcasts).
- **First-hand only:** nodes broadcast digests of their *own* observations.
  Received peer data is never re-exported in one's own digest, preventing
  gossip amplification loops. (Apps may forward whole signed digests if they
  choose; that is transport, outside this crate.)
- Transport, signing, and encryption are the app's/p2panda's job; the crate
  only defines the data and the merge.

## 10. Engine, state, persistence

One owned, in-memory, synchronous engine:

```rust
struct Engine<I, S, P> { … }   // I: ItemId, S: scope key, P: peer id — all generic

engine.upsert_item(id, ItemAttrs)
engine.record(Event)                       // -> Result<_, RecordError>
engine.rank(…) / rank_scopes(…)            // §8
engine.digest(…) / merge_digest(…)         // §9
engine.snapshot() -> Snapshot<I, S, P>     // serde; app persists wherever/whenever
Engine::restore(Snapshot, EngineConfig)
```

- **Bounded memory by construction:** log-compacted buckets (§4), a cap on
  tracked items (default 100 000) with eviction of the lowest
  long-half-life-attention items, caps on scopes and on stored peer digests.
- **No async, no storage traits in v1.** The engine is a working set, not a
  database; the app snapshots it for persistence. Methods take plain `&self` /
  `&mut self` so a store-backed or actor-style shell can wrap it later.

## 11. Conventions and dependencies

| Concern        | Decision                                                    |
|----------------|-------------------------------------------------------------|
| Name           | `salience` (crates.io availability checked 2026-07)         |
| Time           | explicit `u64` UNIX ms everywhere; engine never reads clock |
| Randomness     | caller-supplied `&mut impl rand::Rng`; only for exploration |
| Serialization  | `serde` derives, format-agnostic (app picks CBOR/JSON/…)    |
| Dependencies   | `serde`, `thiserror`, `rand` — no async, no ML runtimes     |
| `no_std`       | not in v1; design does not preclude it                      |
| Numerics       | `f64` for rates/accumulators, `f32` for embeddings          |

## 12. Module layout

```
salience/
  src/
    lib.rs        // public API, docs
    config.rs     // EngineConfig, WeightProfile, priors, caps
    events.rs     // Event, InteractionKind, ItemAttrs, Scope, ScopeClass
    ledger.rs     // time buckets, log-compaction, accumulators
    stats.rs      // RateEstimator trait, Gamma-posterior impl, decay math
    scopes.rs     // scope roll-up index
    predict.rs    // Predictor trait, hierarchical back-off, centroids
    rank.rs       // ScoringProfile, Blend, Score, rank/rank_scopes, Thompson
    digest.rs     // Digest, DigestSpec, trust-weighted merge, peer pool
    snapshot.rs   // Snapshot, restore
  tests/
    simulation.rs // §13 end-to-end model validation
```

## 13. Validation

- **Unit tests** per module.
- **Property tests:**
  - digest merge is idempotent per peer and order-insensitive across peers;
  - decayed scores are monotonically non-increasing as events age (no new
    events → no rank inversions from time alone);
  - bucket compaction error is bounded (score before vs. after compaction
    within tolerance for any half-life ≥ 1 day);
  - snapshot → restore → identical scores;
  - eviction never removes an item scoring above any retained item (under the
    eviction profile).
- **Simulation test (the model test):** synthetic users with known ground-truth
  preferences (per author/topic rates) generate exposure/dwell/interaction
  traces through a simulated feed **with position bias**. Assert:
  1. recovered ranking correlates with ground-truth preference ordering
     (Kendall τ above threshold);
  2. an over-exposed mediocre item does **not** outrank under-exposed good ones
     (position-bias correction works);
  3. Thompson sampling grants new items exposure and converges;
  4. trust-weighted peer digests shift scores proportionally to trust and are
     bounded by it.

## 14. Non-goals (v1)

- Transport, gossip, signing, encryption of digests (p2panda's job).
- Storage I/O or async APIs.
- Running/bundling ML models (embeddings arrive as opaque vectors).
- Inverse propensity scoring (upgrade path reserved via `RateEstimator`).
- Behavioural collaborative-filtering embeddings.
- Session modeling of dwell (apps pre-segment; the crate only clamps).

## 15. Decision log (interview summary)

| # | Decision |
|---|----------|
| 1 | Pure standalone crate; generic types; p2panda integration lives in the app |
| 2 | Two layers — observed item ledger + personal predictive model — blended per consumer |
| 3 | Structured event ingest, scalar attention units at query time via WeightProfile; dwell clamped |
| 4 | Conversion-rate model: attention relative to prominence-weighted exposure; Bayesian smoothing; estimator modular for later IPW |
| 5 | Predictor = hierarchical scope-rate back-off + optional embedding-centroid affinity; no CF, no training |
| 6 | Tiered digest (items / scope roll-ups / centroids), trust-weighted pseudo-count merge; granularity matches p2panda (blob=item, topic&log=scope) |
| 7 | Owned in-memory engine, serde snapshot/restore, bounded by caps + eviction |
| 8 | Batch `rank()` over `ScoringProfile` with presets; explainable Score; optional Thompson sampling |
| 9 | Name `salience`; explicit timestamps; serde-agnostic; deps: serde/thiserror/rand |
| 10| v1 = full implementation + property tests + position-bias simulation |
