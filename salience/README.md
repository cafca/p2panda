# salience

Attention estimation for ranking and replication in social applications.

`salience` models attention as the finite resource it is: every time an app
shows a content item it grants an *opportunity* (a prominence-weighted
exposure — the app's bet), and the user converts some of it into actual
attention, measurable through dwell time and interactions. The engine
estimates **conversion rates** — attention spent per opportunity granted —
which removes position bias and makes ranking self-correcting: content that
gets promoted but not converted sinks.

Built to serve three consumers of one dataset:

- **Home feed** — rank candidates by the attention the local user is
  predicted to spend on them (works for brand-new, never-seen items via
  attribute back-off: author, content type, topics, optional embeddings).
- **Search** — re-rank relevance-matched results by predicted attention.
- **Replication** — prioritize what to fetch, keep and offer in a p2p
  network, per item (blobs) and per scope (topics, authors, logs), including
  signals merged from peers' broadcast digests, weighted by app-supplied
  trust.

The crate is pure Rust: no I/O, no async, no clock, no ML runtime, no
network. Timestamps are explicit, randomness is caller-supplied, persistence
is snapshot/restore, and transport of peer digests is the app's job. It pairs
naturally with [p2panda](https://p2panda.org) but depends on nothing from it.

```rust
use salience::{Engine, EngineConfig, Event, ItemAttrs, Scope, ScopeClass, ScoringProfile};

let mut engine: Engine<&str, &str, &str> = Engine::new(EngineConfig::default());
let now = 1_700_000_000_000; // UNIX ms, always supplied by the caller

engine.upsert_item("post-1", ItemAttrs::new(vec![Scope::new(ScopeClass::AUTHOR, "alice")]));
engine.record(Event::Exposure { item: "post-1", weight: 1.0, at: now }).unwrap();
engine.record(Event::Dwell { item: "post-1", millis: 12_000, at: now }).unwrap();

let ranked = engine.rank(&["post-1"], &ScoringProfile::feed(), now);
```

See [SPEC.md](SPEC.md) for the full design: the two-layer model (observed
item ledger + personal predictive model), Bayesian smoothing with query-time
decay over log-compacted time buckets, tiered trust-weighted peer digests,
and the validation approach (property tests plus a position-biased feed
simulation).

## Status

v1 — under active development, APIs not yet stable.

## License

Licensed under either of [Apache License, Version 2.0](../LICENSES/Apache-2.0.txt)
or [MIT license](../LICENSES/MIT.txt) at your option.
