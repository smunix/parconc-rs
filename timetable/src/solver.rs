//! Timetable solver implementations connecting domain models to search skeletons.

use crate::{
    domain::{Person, Talk, TimeTable, compute_clashes},
    skeleton::{par_search, par_search_naive, seq_search},
};
use std::collections::HashSet;

/// Intermediate state during timetable backtracking search.
#[derive(Clone, Debug)]
pub struct PartialState {
    pub slot_no: usize,
    pub track_no: usize,
    pub slots: Vec<Vec<Talk>>,
    pub current_slot: Vec<Talk>,
    pub slot_candidates: Vec<Talk>,
    pub remaining_talks: Vec<Talk>,
}

/// Helper function implementing `selects` from Marlow's `timetable.hs`:
/// For a list `[x0, x1, ...]`, returns pairs `(xi, remaining_without_xi)`.
fn selects(items: &[Talk]) -> Vec<(Talk, Vec<Talk>)> {
    let mut result = Vec::with_capacity(items.len());
    for (i, &item) in items.iter().enumerate() {
        let mut rest = Vec::with_capacity(items.len().saturating_sub(1));
        rest.extend_from_slice(&items[..i]);
        rest.extend_from_slice(&items[i + 1..]);
        result.push((item, rest));
    }
    result
}

/// Creates the initial state and closures for the search skeletons.
fn make_search_closures(
    people: &[Person],
    all_talks: &[Talk],
    max_tracks: usize,
    max_slots: usize,
) -> (
    PartialState,
    impl Fn(&PartialState) -> Option<TimeTable> + Sync,
    impl Fn(PartialState) -> Vec<PartialState> + Sync,
) {
    let clashes = compute_clashes(people);

    let initial = PartialState {
        slot_no: 0,
        track_no: 0,
        slots: Vec::new(),
        current_slot: Vec::new(),
        slot_candidates: all_talks.to_vec(),
        remaining_talks: all_talks.to_vec(),
    };

    let is_finished = move |st: &PartialState| -> Option<TimeTable> {
        if st.slot_no == max_slots {
            // Marlow reverses or preserves slots: return completed timetable slots
            Some(st.slots.clone())
        } else {
            None
        }
    };

    let refine = move |st: PartialState| -> Vec<PartialState> {
        if st.track_no == max_tracks {
            // Current slot is full; commit it and advance to the next time slot
            let mut next_slots = st.slots;
            next_slots.push(st.current_slot);
            let next_candidates = st.remaining_talks.clone();
            vec![PartialState {
                slot_no: st.slot_no + 1,
                track_no: 0,
                slots: next_slots,
                current_slot: Vec::new(),
                slot_candidates: next_candidates,
                remaining_talks: st.remaining_talks,
            }]
        } else {
            // Select a candidate talk for the current track
            let empty_set = HashSet::new();
            let mut branches = Vec::new();

            for (talk, rest_candidates) in selects(&st.slot_candidates) {
                let talk_clashes = clashes.get(&talk).unwrap_or(&empty_set);

                let next_slot_candidates: Vec<Talk> = rest_candidates
                    .into_iter()
                    .filter(|t| !talk_clashes.contains(t))
                    .collect();

                let next_remaining: Vec<Talk> = st
                    .remaining_talks
                    .iter()
                    .copied()
                    .filter(|&t| t != talk)
                    .collect();

                let mut next_slot = st.current_slot.clone();
                next_slot.push(talk);

                branches.push(PartialState {
                    slot_no: st.slot_no,
                    track_no: st.track_no + 1,
                    slots: st.slots.clone(),
                    current_slot: next_slot,
                    slot_candidates: next_slot_candidates,
                    remaining_talks: next_remaining,
                });
            }

            branches
        }
    };

    (initial, is_finished, refine)
}

/// Solves the timetable problem using pure sequential depth-first search (`timetable1.hs`).
pub fn solve_sequential(
    people: &[Person],
    all_talks: &[Talk],
    max_tracks: usize,
    max_slots: usize,
) -> Vec<TimeTable> {
    let (initial, is_finished, refine) =
        make_search_closures(people, all_talks, max_tracks, max_slots);
    seq_search(initial, &is_finished, &refine)
}

/// Solves the timetable problem using unbounded naive parallel search (`timetable2.hs`).
pub fn solve_parallel_naive(
    people: &[Person],
    all_talks: &[Talk],
    max_tracks: usize,
    max_slots: usize,
) -> Vec<TimeTable> {
    let (initial, is_finished, refine) =
        make_search_closures(people, all_talks, max_tracks, max_slots);
    par_search_naive(initial, &is_finished, &refine)
}

/// Solves the timetable problem using depth-limited parallel search (`timetable3.hs`).
pub fn solve_parallel(
    people: &[Person],
    all_talks: &[Talk],
    max_tracks: usize,
    max_slots: usize,
    max_depth: usize,
) -> Vec<TimeTable> {
    let (initial, is_finished, refine) =
        make_search_closures(people, all_talks, max_tracks, max_slots);
    par_search(0, max_depth, initial, &is_finished, &refine)
}
