//! Problem instance generation and predefined benchmark fixtures.
//!
//! Replicates Simon Marlow's `bench` generator from Chapter 4 of
//! "Parallel and Concurrent Programming in Haskell", along with standard
//! deterministic test cases.

use crate::domain::{Person, Talk};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::index::sample;

/// Generates a synthetic conference benchmark instance matching Marlow's `bench`.
///
/// # Arguments
/// * `n_slots` - Number of time slots (e.g. 4)
/// * `n_tracks` - Number of concurrent tracks per slot (e.g. 2)
/// * `n_persons` - Number of conference attendees (e.g. 100)
/// * `talks_per_person` - Number of talks each attendee wishes to attend (e.g. 3)
/// * `seed` - Seed for reproducible pseudo-random generation (e.g. 1001)
pub fn generate_conference(
    n_slots: usize,
    n_tracks: usize,
    n_persons: usize,
    talks_per_person: usize,
    seed: u64,
) -> (Vec<Person>, Vec<Talk>) {
    let total_talks = n_slots * n_tracks;
    let talks: Vec<Talk> = (0..total_talks).map(Talk).collect();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut people = Vec::with_capacity(n_persons);

    let k = talks_per_person.min(total_talks);

    for i in 0..n_persons {
        let indices = sample(&mut rng, total_talks, k);
        let attendee_talks: Vec<Talk> = indices.into_iter().map(|idx| talks[idx]).collect();

        people.push(Person {
            name: format!("P{}", i + 1),
            talks: attendee_talks,
        });
    }

    (people, talks)
}

/// The standard 4-talks test case from Marlow's `timetable1.hs`:
/// - 4 talks: 0, 1, 2, 3
/// - 2 slots, 2 tracks
/// - Attendees:
///   - P: [0, 1]  (0 and 1 clash)
///   - Q: [1, 2]  (1 and 2 clash)
///   - R: [2, 3]  (2 and 3 clash)
///
/// Has exactly 8 valid timetables.
pub fn marlow_small_test() -> (Vec<Person>, Vec<Talk>, usize, usize) {
    let talks = vec![Talk(0), Talk(1), Talk(2), Talk(3)];
    let people = vec![
        Person {
            name: "P".into(),
            talks: vec![Talk(0), Talk(1)],
        },
        Person {
            name: "Q".into(),
            talks: vec![Talk(1), Talk(2)],
        },
        Person {
            name: "R".into(),
            talks: vec![Talk(2), Talk(3)],
        },
    ];
    let tracks = 2;
    let slots = 2;
    (people, talks, tracks, slots)
}

/// An impossible instance where a clique of talks clashes with each other.
/// If `clique_size > slots`, it cannot be scheduled because at least two talks
/// from the clique must share a slot, violating the clash constraint.
pub fn impossible_clique_test(
    n_slots: usize,
    n_tracks: usize,
) -> (Vec<Person>, Vec<Talk>, usize, usize) {
    let total_talks = n_slots * n_tracks;
    let talks: Vec<Talk> = (0..total_talks).map(Talk).collect();

    // A single attendee interested in (n_slots + 1) talks creates an impossible conflict
    let clique_size = (n_slots + 1).min(total_talks);
    let people = vec![Person {
        name: "ImpossibleAttendee".into(),
        talks: talks[..clique_size].to_vec(),
    }];

    (people, talks, n_tracks, n_slots)
}
