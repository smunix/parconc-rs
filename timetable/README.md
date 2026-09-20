# Parallel Conference Timetable Solver

An efficient, multi-threaded Rust implementation of the combinatorial conference timetable scheduling problem and parallel search skeletons from **Chapter 4 ("A Conference Timetable")** of Simon Marlow's book *Parallel and Concurrent Programming in Haskell*.

---

## 1. Problem Formulation

Given:
- `N` talks to be scheduled across `S` time slots and `T` concurrent tracks (`N = S × T`).
- A collection of conference attendees (people), where each attendee wishes to attend a specific subset of talks.

**Constraints**:
1. Every talk must be scheduled exactly once in some slot and track.
2. Each time slot has exactly `T` parallel tracks.
3. Two talks **clash** (conflict) if at least one attendee wants to attend both.
4. **No clashing talks may be scheduled in the same time slot** across different tracks.

Finding a valid timetable is equivalent to partitioning an undirected conflict graph into `S` independent sets of size `T` each.

---

## 2. Parallel Search Skeletons

Following Marlow's abstractions, this crate separates the problem-specific logic (`domain`, `solver`) from generic combinatorial backtracking skeletons (`skeleton`):

```rust
pub fn search<P, S, F, R>(state: P, is_finished: &F, refine: &R) -> Vec<S>
```

### The Three Search Skeletons:

1. **`seq_search` (`timetable1.hs`)**:
   - Pure sequential depth-first search.
   - Explores branches recursively in a single thread.
   - Serves as the correctness and performance baseline ($1.00\times$).

2. **`par_search_naive` (`timetable2.hs`)**:
   - Unbounded divide-and-conquer parallelism.
   - Spawns tasks in parallel at *every* node of the search tree using Rayon fork-join (`into_par_iter()`).
   - **The Granularity Problem**: Near the leaves of the search tree, tasks become minuscule (sub-microsecond), causing thread pool scheduling overhead and queue synchronization to degrade performance.

3. **`par_search` with Depth Cutoff (`timetable3.hs`)**:
   - Bridges coarse-grained parallel distribution with fine-grained local evaluation.
   - Evaluates search branches in parallel across threads down to `max_depth` (e.g. depth 3 or 4).
   - Once `depth >= max_depth`, execution shifts seamlessly to high-speed sequential DFS (`seq_search`) on the local worker thread without any synchronization overhead.

---

## 3. High-Performance Bitmask Solver (`bitmask_solver`)

In addition to vector-based filtering, this crate implements an ultra-fast bitmask solver (`BitmaskProblem` / `BitmaskState`) for conferences with up to 64 talks (`N <= 64`):

- **Bitwise Clash Elimination**: Subsets of talks and attendee conflict graphs are represented as 64-bit integers (`u64`). Filtering candidate talks conflicting with talk `t` is reduced to a single CPU instruction:
  ```rust
  next_candidates = candidates & !(1 << t) & !clashes[t];
  ```
- **Hardware-Accelerated Traversal**: Iterating over valid candidate talks uses hardware bit manipulation (`tzcnt` / `trailing_zeros` and `blsr` / `x & (x - 1)`).
- **Zero Heap Allocations**: When computing total solution counts (`count_bitmask_parallel`), search states remain on the CPU register file and thread stack with zero memory allocation.

---

## 4. Architecture & Project Layout

```
timetable/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs              # Library exports
│   ├── domain.rs           # Talk, Person, TimeTable, compute_clashes, validate_timetable
│   ├── skeleton.rs         # seq_search, par_search_naive, par_search (depth-limited)
│   ├── solver.rs           # Backtracking state & refine closures
│   ├── bitmask_solver.rs   # 64-bit ALU-accelerated solver & counter
│   ├── generator.rs        # Marlow's bench generator & test fixtures
│   └── main.rs             # CLI benchmarking & verification tool
└── tests/
    └── timetable_test.rs   # Test suite verifying validity & equivalence
```

---

## 5. Usage & CLI Benchmark

### Running Tests
```bash
cargo test -p timetable
```

### Running the CLI Benchmark
```bash
# Run and compare all solvers on an 8-talk conference (4 slots, 2 tracks)
cargo run --release -p timetable -- --slots 4 --tracks 2 --persons 10 --talks-per-person 3 --depth 3 --mode all
```

### CLI Options
- `-s, --slots <N>`: Number of time slots (default: 4).
- `-t, --tracks <N>`: Number of parallel tracks per slot (default: 2).
- `-p, --persons <N>`: Number of attendees (default: 10).
- `-c, --talks-per-person <N>`: Talks per attendee (default: 3).
- `-d, --depth <N>`: Depth cutoff for parallel search (default: 3).
- `-m, --mode <MODE>`: Solver to run:
  - `all`: Compare all solvers and display speedup summary.
  - `verify`: Run all solvers and verify mathematical equivalence and constraint validity.
  - `seq`: Pure sequential DFS (`timetable1.hs`).
  - `par-naive`: Unbounded naive parallel search (`timetable2.hs`).
  - `par`: Depth-limited parallel search (`timetable3.hs`).
  - `bitmask-seq`: Sequential bitmask solver.
  - `bitmask-par`: Parallel depth-limited bitmask solver.
  - `bitmask-count`: Fast parallel bitmask counter (no allocations).
- `--seed <U64>`: Random seed for reproducible synthetic generation (default: 1001).

---

## 6. Verification & Constraint Invariants

Every generated solution is mathematically validated by `validate_timetable`:
- [x] Exactly `S` slots and `T` tracks per slot.
- [x] Every talk scheduled exactly once across the conference.
- [x] Zero clashes between any pair of talks sharing the same time slot.
- [x] Exact solution count equivalence across sequential, naive parallel, depth-limited parallel, and bitmask solvers.
