//! End-to-end model validation (SPEC §13): synthetic users with known
//! ground-truth preferences generate exposure/attention traces through a
//! simulated feed **with position bias**; the engine must recover the
//! preference ordering for brand-new, never-seen items.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use salience::{
    DigestSpec, Engine, EngineConfig, Event, Exploration, InteractionKind, ItemAttrs, Scope,
    ScopeClass, ScoringProfile, Timestamp,
};

type SimEngine = Engine<u64, String, String>;

const T0: Timestamp = 1_700_000_000_000;
const HOUR_MS: u64 = 3_600_000;

/// Ground-truth expected dwell seconds per standard exposure, per author.
const AUTHORS: [(&str, f64); 3] = [("alice", 20.0), ("bob", 6.0), ("carol", 1.0)];

/// Prominence weight by feed position: the app's bet, larger for higher
/// slots. This is the position bias the engine must correct for.
const POSITION_WEIGHT: [f64; 6] = [1.5, 1.2, 1.0, 0.8, 0.6, 0.5];

fn author_scope(name: &str) -> Scope<String> {
    Scope::new(ScopeClass::AUTHOR, name.to_string())
}

fn publish(engine: &mut SimEngine, id: u64, author: &str) {
    engine.upsert_item(id, ItemAttrs::new(vec![author_scope(author)]));
}

/// Simulate a user session: the engine ranks candidates, the app grants
/// position-weighted exposure to the top slots, and the simulated user
/// converts according to their ground-truth author preference.
fn run_feed_simulation(engine: &mut SimEngine, rounds: u64, rng: &mut ChaCha8Rng) {
    let mut next_id = 0u64;
    let mut published: Vec<(u64, &str, f64)> = Vec::new();

    for round in 0..rounds {
        let now = T0 + round * HOUR_MS;
        // Each author publishes one new post per round.
        for (author, truth) in AUTHORS {
            published.push((next_id, author, truth));
            publish(engine, next_id, author);
            next_id += 1;
        }
        // Candidates: the last 8 rounds' posts. The feed ranks with Thompson
        // sampling — without exploration a greedy loop would starve
        // low-ranked authors of exposure and never pin their rate down.
        let window = published.len().saturating_sub(8 * AUTHORS.len());
        let candidates: Vec<u64> = published[window..].iter().map(|(id, _, _)| *id).collect();
        let profile = ScoringProfile {
            exploration: Exploration::ThompsonSample,
            ..ScoringProfile::feed()
        };
        let ranked = engine.rank_with_rng(&candidates, &profile, now, rng);

        // The app fills all but the last slot from the ranking and reserves
        // the last slot for a uniformly random candidate — the exploration
        // floor that keeps a feedback loop from starving unranked authors of
        // opportunity (SPEC §8: exploration grants opportunities to
        // uncertain items; the slot policy is the app's half of that deal).
        let exploit = POSITION_WEIGHT.len() - 1;
        let mut shown: Vec<u64> = ranked.iter().take(exploit).map(|r| r.id).collect();
        let rest: Vec<u64> = candidates
            .iter()
            .copied()
            .filter(|id| !shown.contains(id))
            .collect();
        if !rest.is_empty() {
            shown.push(rest[rng.random_range(0..rest.len())]);
        }

        for (position, item) in shown.into_iter().enumerate() {
            let weight = POSITION_WEIGHT[position];
            engine
                .record(Event::Exposure {
                    item,
                    weight,
                    at: now,
                })
                .unwrap();
            // The user converts opportunity into attention according to
            // their true preference (dwell scales with prominence; the
            // conversion *rate* stays the author's truth).
            let truth = published[item as usize].2;
            let noise = 0.7 + 0.6 * rng.random::<f64>();
            let dwell_ms = (truth * weight * noise * 1000.0) as u64;
            if dwell_ms > 0 {
                engine
                    .record(Event::Dwell {
                        item,
                        millis: dwell_ms,
                        at: now,
                    })
                    .unwrap();
            }
            // Preference also drives occasional reactions.
            if rng.random::<f64>() < truth / 60.0 {
                engine
                    .record(Event::Interaction {
                        item,
                        kind: InteractionKind::REACTION,
                        at: now,
                    })
                    .unwrap();
            }
        }
    }
}

#[test]
fn engine_recovers_ground_truth_preferences_for_unseen_items() {
    for seed in [42, 7, 1234, 99, 2026] {
        let mut engine: SimEngine = Engine::new(EngineConfig::default());
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        run_feed_simulation(&mut engine, 240, &mut rng);

        // Three brand-new posts, one per author, zero recorded events.
        let now = T0 + 241 * HOUR_MS;
        let fresh_base = 1_000_000u64;
        for (i, (author, _)) in AUTHORS.iter().enumerate() {
            publish(&mut engine, fresh_base + i as u64, author);
        }
        let fresh: Vec<u64> = (0..AUTHORS.len() as u64).map(|i| fresh_base + i).collect();
        let ranked = engine.rank(&fresh, &ScoringProfile::feed(), now);

        // Recovered ordering must match ground truth: alice > bob > carol.
        let order: Vec<u64> = ranked.iter().map(|r| r.id).collect();
        assert_eq!(
            order,
            vec![fresh_base, fresh_base + 1, fresh_base + 2],
            "seed {seed}: expected alice > bob > carol, got {ranked:#?}"
        );

        // The predicted rates must separate clearly, not just tie-break.
        let alice = ranked[0].score.predicted_rate;
        let carol = ranked[2].score.predicted_rate;
        assert!(
            alice > 2.0 * carol,
            "seed {seed}: alice {alice} should clearly exceed carol {carol}"
        );
    }
}

#[test]
fn scope_ranking_reflects_conversion_not_volume() {
    for seed in [43, 8, 4321] {
        let mut engine: SimEngine = Engine::new(EngineConfig::default());
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        run_feed_simulation(&mut engine, 120, &mut rng);

        let now = T0 + 121 * HOUR_MS;
        let scopes: Vec<Scope<String>> = AUTHORS.iter().map(|(a, _)| author_scope(a)).collect();
        let ranked = engine.rank_scopes(&scopes, &ScoringProfile::replication(), now);
        let order: Vec<&str> = ranked.iter().map(|r| r.scope.key.as_str()).collect();
        assert_eq!(order, vec!["alice", "bob", "carol"], "seed {seed}");
    }
}

#[test]
fn peer_digests_carry_taste_across_engines() {
    // A peer whose feed is all about "dave" broadcasts their digest; the
    // local engine (which has never seen dave) picks up the signal for
    // replication decisions.
    let mut peer: SimEngine = Engine::new(EngineConfig::default());
    let now = T0;
    for i in 0..40u64 {
        publish(&mut peer, i, "dave");
        for rep in 0..3u64 {
            let at = now + i * HOUR_MS + rep;
            peer.record(Event::Exposure {
                item: i,
                weight: 1.0,
                at,
            })
            .unwrap();
            peer.record(Event::Dwell {
                item: i,
                millis: 15_000,
                at,
            })
            .unwrap();
        }
    }
    let digest = peer.digest(&DigestSpec::default(), now + 40 * HOUR_MS);

    let mut local: SimEngine = Engine::new(EngineConfig::default());
    local
        .merge_digest("peer-1".to_string(), &digest, 0.9)
        .unwrap();

    let query_at = now + 41 * HOUR_MS;
    let scopes = vec![author_scope("dave"), author_scope("nobody")];
    let ranked = local.rank_scopes(&scopes, &ScoringProfile::replication(), query_at);
    assert_eq!(ranked[0].scope.key, "dave");
    assert!(ranked[0].score.peer_rate > ranked[1].score.peer_rate);

    // Item-level: the peer's hottest items are now replication candidates.
    let hot_items: Vec<u64> = (0..40).collect();
    let ranked_items = local.rank(&hot_items, &ScoringProfile::replication(), query_at);
    assert!(ranked_items[0].score.peer_rate > 0.0);
    assert!(ranked_items[0].score.exposure_mass > 0.0);
}
