//! Domain types and validation for the conference timetable problem.
//!
//! Based on Chapter 4 of Marlow's "Parallel and Concurrent Programming in Haskell".
//! Represents:
//! - Talks (identified by numeric IDs)
//! - Attendees / People (each interested in a subset of talks)
//! - Conflict / Clash Graph (pairs of talks with overlapping attendee interest)
//! - Timetables (slot-by-slot allocations of parallel tracks)

use std::collections::{HashMap, HashSet};

/// A talk at the conference, identified by an integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Talk(pub usize);

/// An attendee with their list of talks they wish to attend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    pub talks: Vec<Talk>,
}

/// A conference timetable: `slots[slot_index]` contains the list of talks
/// running concurrently across parallel tracks during that slot.
pub type TimeTable = Vec<Vec<Talk>>;

/// Precomputes the clash lookup table: `Talk -> HashSet<Talk>`.
///
/// Two talks clash if there is at least one person who wants to attend both.
/// Clashing talks cannot be scheduled in the same time slot across different tracks.
pub fn compute_clashes(people: &[Person]) -> HashMap<Talk, HashSet<Talk>> {
    let mut clashes: HashMap<Talk, HashSet<Talk>> = HashMap::new();
    for person in people {
        for &t1 in &person.talks {
            for &t2 in &person.talks {
                if t1 != t2 {
                    clashes.entry(t1).or_default().insert(t2);
                }
            }
        }
    }
    clashes
}

/// Validates that a timetable satisfies all conference scheduling rules:
/// 1. Exactly `max_slots` time slots.
/// 2. Exactly `max_tracks` talks per time slot.
/// 3. Every talk appears exactly once.
/// 4. No two talks in the same slot clash.
pub fn validate_timetable(
    timetable: &TimeTable,
    max_slots: usize,
    max_tracks: usize,
    total_talks: usize,
    clashes: &HashMap<Talk, HashSet<Talk>>,
) -> Result<(), String> {
    if timetable.len() != max_slots {
        return Err(format!(
            "Expected {max_slots} slots, but timetable has {}",
            timetable.len()
        ));
    }

    let mut seen_talks = HashSet::new();

    for (slot_idx, slot) in timetable.iter().enumerate() {
        if slot.len() != max_tracks {
            return Err(format!(
                "Slot {slot_idx} has {} talks, expected {max_tracks}",
                slot.len()
            ));
        }

        // Check for clashes within this slot
        for (i, &t1) in slot.iter().enumerate() {
            if !seen_talks.insert(t1) {
                return Err(format!("Duplicate talk {:?} found in slot {slot_idx}", t1));
            }

            for &t2 in &slot[i + 1..] {
                if let Some(conflicts) = clashes.get(&t1)
                    && conflicts.contains(&t2)
                {
                    return Err(format!(
                        "Clash detected in slot {slot_idx}: Talk {:?} and Talk {:?} clash!",
                        t1, t2
                    ));
                }
            }
        }
    }

    if seen_talks.len() != total_talks {
        return Err(format!(
            "Expected {total_talks} distinct talks scheduled, but only saw {}",
            seen_talks.len()
        ));
    }

    Ok(())
}
