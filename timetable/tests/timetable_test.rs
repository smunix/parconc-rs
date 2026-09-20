use std::collections::HashSet;
use timetable::bitmask_solver::{
    count_bitmask_parallel, solve_bitmask_parallel, solve_bitmask_sequential,
};
use timetable::domain::{Talk, TimeTable, compute_clashes, validate_timetable};
use timetable::generator::{generate_conference, impossible_clique_test, marlow_small_test};
use timetable::solver::{solve_parallel, solve_parallel_naive, solve_sequential};

/// Normalizes a timetable for set comparison (slots ordered, tracks within slot ordered).
fn normalize_timetable(tt: &TimeTable) -> Vec<Vec<usize>> {
    let mut sorted_slots: Vec<Vec<usize>> = tt
        .iter()
        .map(|slot| {
            let mut s: Vec<usize> = slot.iter().map(|t| t.0).collect();
            s.sort_unstable();
            s
        })
        .collect();
    sorted_slots.sort();
    sorted_slots
}

#[test]
fn test_marlow_small_test_eight_solutions() {
    let (people, talks, tracks, slots) = marlow_small_test();
    let clashes = compute_clashes(&people);

    let seq_sol = solve_sequential(&people, &talks, tracks, slots);
    let naive_sol = solve_parallel_naive(&people, &talks, tracks, slots);
    let par_sol = solve_parallel(&people, &talks, tracks, slots, 2);
    let bseq_sol = solve_bitmask_sequential(&people, &talks, tracks, slots);
    let bpar_sol = solve_bitmask_parallel(&people, &talks, tracks, slots, 2);
    let bcount = count_bitmask_parallel(&people, &talks, tracks, slots, 2);

    assert_eq!(seq_sol.len(), 8, "Expected exactly 8 solutions");
    assert_eq!(naive_sol.len(), 8);
    assert_eq!(par_sol.len(), 8);
    assert_eq!(bseq_sol.len(), 8);
    assert_eq!(bpar_sol.len(), 8);
    assert_eq!(bcount, 8);

    // Validate all solutions
    for sol in &seq_sol {
        validate_timetable(sol, slots, tracks, talks.len(), &clashes).expect("Valid timetable");
    }
    for sol in &bpar_sol {
        validate_timetable(sol, slots, tracks, talks.len(), &clashes).expect("Valid timetable");
    }

    // Check solution set equality between standard solver and bitmask solver
    let seq_set: HashSet<Vec<Vec<usize>>> = seq_sol.iter().map(normalize_timetable).collect();
    let bpar_set: HashSet<Vec<Vec<usize>>> = bpar_sol.iter().map(normalize_timetable).collect();
    assert_eq!(seq_set, bpar_set);
}

#[test]
fn test_depth_cutoff_invariance() {
    let (people, talks, tracks, slots) = marlow_small_test();

    // Depths from 0 (pure sequential fallback) to 4 (deep parallel)
    for depth in 0..=4 {
        let par_sol = solve_parallel(&people, &talks, tracks, slots, depth);
        let bpar_sol = solve_bitmask_parallel(&people, &talks, tracks, slots, depth);
        assert_eq!(par_sol.len(), 8, "Failed at depth {depth}");
        assert_eq!(bpar_sol.len(), 8, "Failed at depth {depth}");
    }
}

#[test]
fn test_impossible_clique_configuration() {
    let (people, talks, tracks, slots) = impossible_clique_test(3, 2);
    let seq_sol = solve_sequential(&people, &talks, tracks, slots);
    let bpar_sol = solve_bitmask_parallel(&people, &talks, tracks, slots, 2);
    let bcount = count_bitmask_parallel(&people, &talks, tracks, slots, 2);

    assert_eq!(seq_sol.len(), 0, "Impossible clique must yield 0 solutions");
    assert_eq!(bpar_sol.len(), 0);
    assert_eq!(bcount, 0);
}

#[test]
fn test_synthetic_conference_validation_and_equivalence() {
    // 3 slots, 2 tracks = 6 talks, 8 attendees
    let slots = 3;
    let tracks = 2;
    let (people, talks) = generate_conference(slots, tracks, 8, 2, 42);
    let clashes = compute_clashes(&people);

    let seq_sol = solve_sequential(&people, &talks, tracks, slots);
    let par_sol = solve_parallel(&people, &talks, tracks, slots, 2);
    let bseq_sol = solve_bitmask_sequential(&people, &talks, tracks, slots);
    let bpar_sol = solve_bitmask_parallel(&people, &talks, tracks, slots, 2);
    let bcount = count_bitmask_parallel(&people, &talks, tracks, slots, 2);

    assert_eq!(seq_sol.len(), par_sol.len());
    assert_eq!(seq_sol.len(), bseq_sol.len());
    assert_eq!(seq_sol.len(), bpar_sol.len());
    assert_eq!(seq_sol.len(), bcount);

    // Verify constraints on every single solution
    for sol in &bpar_sol {
        validate_timetable(sol, slots, tracks, talks.len(), &clashes)
            .expect("Every solution must satisfy timetable constraints");
    }

    let seq_set: HashSet<Vec<Vec<usize>>> = seq_sol.iter().map(normalize_timetable).collect();
    let bpar_set: HashSet<Vec<Vec<usize>>> = bpar_sol.iter().map(normalize_timetable).collect();
    assert_eq!(seq_set, bpar_set);
}

#[test]
fn test_validation_catches_invalid_timetable() {
    let (_people, talks, tracks, slots) = marlow_small_test();
    let mut clashes = std::collections::HashMap::new();
    let mut set0 = HashSet::new();
    set0.insert(Talk(1));
    clashes.insert(Talk(0), set0);

    // Case 1: duplicate talk in same slot
    let invalid_dup = vec![vec![Talk(0), Talk(0)], vec![Talk(2), Talk(3)]];
    assert!(validate_timetable(&invalid_dup, slots, tracks, talks.len(), &clashes).is_err());

    // Case 2: clash within same slot (0 and 1 clash)
    let invalid_clash = vec![vec![Talk(0), Talk(1)], vec![Talk(2), Talk(3)]];
    assert!(validate_timetable(&invalid_clash, slots, tracks, talks.len(), &clashes).is_err());

    // Case 3: wrong slot count
    let invalid_slots = vec![vec![Talk(0), Talk(2)]];
    assert!(validate_timetable(&invalid_slots, slots, tracks, talks.len(), &clashes).is_err());

    // Case 4: wrong track count in slot
    let invalid_tracks = vec![vec![Talk(0), Talk(2), Talk(3)], vec![Talk(1)]];
    assert!(validate_timetable(&invalid_tracks, slots, tracks, talks.len(), &clashes).is_err());
}
