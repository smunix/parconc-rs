//! Parallel Conference Timetable Solver
//!
//! Based on Chapter 4 ("A Conference Timetable") of Simon Marlow's
//! *Parallel and Concurrent Programming in Haskell*.
//!
//! # Architecture
//! - [`domain`]: Core types (`Talk`, `Person`, `TimeTable`), clash detection, and schedule validation.
//! - [`skeleton`]: Higher-order search abstractions (`seq_search`, `par_search_naive`, `par_search`).
//! - [`solver`]: Standard backtracking search implementation bridging domain and skeletons.
//! - [`bitmask_solver`]: High-performance bitmask solver representing conflict graphs and candidate sets as 64-bit integers.
//! - [`generator`]: Synthetic conference instance generator matching Marlow's `bench`.

pub mod bitmask_solver;
pub mod domain;
pub mod generator;
pub mod skeleton;
pub mod solver;

pub use domain::{Person, Talk, TimeTable, compute_clashes, validate_timetable};
pub use generator::generate_conference;
pub use solver::{solve_parallel, solve_parallel_naive, solve_sequential};
