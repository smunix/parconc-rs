use clap::{Parser, ValueEnum};
use std::time::Instant;
use timetable::bitmask_solver::{
    count_bitmask_parallel, solve_bitmask_parallel, solve_bitmask_sequential,
};
use timetable::domain::{TimeTable, compute_clashes, validate_timetable};
use timetable::generator::generate_conference;
use timetable::solver::{solve_parallel, solve_parallel_naive, solve_sequential};

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum SolverMode {
    /// Pure sequential DFS (timetable1.hs)
    Seq,
    /// Unbounded naive parallel search (timetable2.hs)
    ParNaive,
    /// Depth-limited parallel search (timetable3.hs)
    Par,
    /// Sequential bitmask solver
    BitmaskSeq,
    /// Parallel depth-limited bitmask solver
    BitmaskPar,
    /// Fast parallel bitmask counter (no allocations)
    BitmaskCount,
    /// Run and compare all solvers
    All,
    /// Verify all solvers produce valid, identical solution sets
    Verify,
}

#[derive(Parser, Debug)]
#[command(
    name = "timetable",
    about = "Parallel Conference Timetable Solver (Marlow Chapter 4)"
)]
struct Args {
    /// Number of time slots
    #[arg(short = 's', long, default_value_t = 4)]
    slots: usize,

    /// Number of parallel tracks per slot
    #[arg(short = 't', long, default_value_t = 2)]
    tracks: usize,

    /// Number of conference attendees
    #[arg(short = 'p', long, default_value_t = 10)]
    persons: usize,

    /// Number of talks each attendee wishes to attend
    #[arg(short = 'c', long, default_value_t = 3)]
    talks_per_person: usize,

    /// Parallel search depth cutoff (for par & bitmask-par)
    #[arg(short = 'd', long, default_value_t = 3)]
    depth: usize,

    /// Pseudo-random generator seed
    #[arg(long, default_value_t = 1001)]
    seed: u64,

    /// Solver mode
    #[arg(short = 'm', long, value_enum, default_value_t = SolverMode::All)]
    mode: SolverMode,
}

fn print_timetable_sample(timetable: &TimeTable) {
    for (slot_idx, slot) in timetable.iter().enumerate() {
        let talks_str: Vec<String> = slot.iter().map(|t| format!("Talk {}", t.0)).collect();
        println!("  Slot {slot_idx}: [{}]", talks_str.join(", "));
    }
}

fn main() {
    let args = Args::parse();
    let total_talks = args.slots * args.tracks;

    println!("===============================================================");
    println!("        PARALLEL CONFERENCE TIMETABLE BENCHMARK");
    println!("===============================================================");
    println!("Configuration:");
    println!("  Time slots:       {}", args.slots);
    println!("  Parallel tracks:  {}", args.tracks);
    println!("  Total talks:      {total_talks}");
    println!("  Attendees:        {}", args.persons);
    println!("  Talks/attendee:   {}", args.talks_per_person);
    println!("  Cutoff depth:     {}", args.depth);
    println!("  Random seed:      {}", args.seed);
    println!("  Rayon threads:    {}", rayon::current_num_threads());
    println!("---------------------------------------------------------------");

    let (people, talks) = generate_conference(
        args.slots,
        args.tracks,
        args.persons,
        args.talks_per_person,
        args.seed,
    );

    let clashes = compute_clashes(&people);
    let total_clashes: usize = clashes.values().map(|c| c.len()).sum::<usize>() / 2;
    println!("Clash graph generated: {total_clashes} conflicting talk pairs.");
    println!("---------------------------------------------------------------");

    let run_timed =
        |name: &str, f: Box<dyn FnOnce() -> (usize, Option<TimeTable>)>| -> (usize, f64) {
            let start = Instant::now();
            let (count, sample) = f();
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "{:<22} -> {:>8} solutions in {:>9.4}s",
                name, count, elapsed
            );
            if let Some(ref s) = sample {
                println!("  First solution sample:");
                print_timetable_sample(s);
            }
            (count, elapsed)
        };

    match args.mode {
        SolverMode::Seq => {
            run_timed(
                "Sequential (timetable1)",
                Box::new(|| {
                    let res = solve_sequential(&people, &talks, args.tracks, args.slots);
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );
        }
        SolverMode::ParNaive => {
            run_timed(
                "Par Naive (timetable2)",
                Box::new(|| {
                    let res = solve_parallel_naive(&people, &talks, args.tracks, args.slots);
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );
        }
        SolverMode::Par => {
            run_timed(
                "Par Depth-Cutoff (timetable3)",
                Box::new(|| {
                    let res = solve_parallel(&people, &talks, args.tracks, args.slots, args.depth);
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );
        }
        SolverMode::BitmaskSeq => {
            run_timed(
                "Bitmask Sequential",
                Box::new(|| {
                    let res = solve_bitmask_sequential(&people, &talks, args.tracks, args.slots);
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );
        }
        SolverMode::BitmaskPar => {
            run_timed(
                "Bitmask Par Depth-Cutoff",
                Box::new(|| {
                    let res = solve_bitmask_parallel(
                        &people,
                        &talks,
                        args.tracks,
                        args.slots,
                        args.depth,
                    );
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );
        }
        SolverMode::BitmaskCount => {
            run_timed(
                "Bitmask Par Count",
                Box::new(|| {
                    let count = count_bitmask_parallel(
                        &people,
                        &talks,
                        args.tracks,
                        args.slots,
                        args.depth,
                    );
                    (count, None)
                }),
            );
        }
        SolverMode::All => {
            println!("Running performance comparison across all implementations...\n");

            let (seq_count, seq_time) = run_timed(
                "1. Sequential DFS",
                Box::new(|| {
                    let res = solve_sequential(&people, &talks, args.tracks, args.slots);
                    let sample = res.first().cloned();
                    (res.len(), sample)
                }),
            );

            let (naive_count, naive_time) = run_timed(
                "2. Par Naive (no cutoff)",
                Box::new(|| {
                    let res = solve_parallel_naive(&people, &talks, args.tracks, args.slots);
                    (res.len(), None)
                }),
            );

            let (par_count, par_time) = run_timed(
                &format!("3. Par Depth-Cutoff (d={})", args.depth),
                Box::new(|| {
                    let res = solve_parallel(&people, &talks, args.tracks, args.slots, args.depth);
                    (res.len(), None)
                }),
            );

            let (bseq_count, bseq_time) = run_timed(
                "4. Bitmask Sequential",
                Box::new(|| {
                    let res = solve_bitmask_sequential(&people, &talks, args.tracks, args.slots);
                    (res.len(), None)
                }),
            );

            let (bpar_count, bpar_time) = run_timed(
                &format!("5. Bitmask Par (d={})", args.depth),
                Box::new(|| {
                    let res = solve_bitmask_parallel(
                        &people,
                        &talks,
                        args.tracks,
                        args.slots,
                        args.depth,
                    );
                    (res.len(), None)
                }),
            );

            let (bcount_count, bcount_time) = run_timed(
                &format!("6. Bitmask Count-Only (d={})", args.depth),
                Box::new(|| {
                    let count = count_bitmask_parallel(
                        &people,
                        &talks,
                        args.tracks,
                        args.slots,
                        args.depth,
                    );
                    (count, None)
                }),
            );

            println!("\n---------------------------------------------------------------");
            println!("Summary & Speedup (relative to Sequential DFS):");
            println!(
                "  Sequential DFS:        {:>9.4}s (baseline: 1.00x)",
                seq_time
            );
            println!(
                "  Par Naive (unbounded): {:>9.4}s ({:.2}x)",
                naive_time,
                seq_time / naive_time.max(1e-9)
            );
            println!(
                "  Par Depth-Cutoff:      {:>9.4}s ({:.2}x)",
                par_time,
                seq_time / par_time.max(1e-9)
            );
            println!(
                "  Bitmask Sequential:    {:>9.4}s ({:.2}x)",
                bseq_time,
                seq_time / bseq_time.max(1e-9)
            );
            println!(
                "  Bitmask Parallel:      {:>9.4}s ({:.2}x)",
                bpar_time,
                seq_time / bpar_time.max(1e-9)
            );
            println!(
                "  Bitmask Count-Only:    {:>9.4}s ({:.2}x)",
                bcount_time,
                seq_time / bcount_time.max(1e-9)
            );
            println!("---------------------------------------------------------------");

            assert_eq!(seq_count, naive_count);
            assert_eq!(seq_count, par_count);
            assert_eq!(seq_count, bseq_count);
            assert_eq!(seq_count, bpar_count);
            assert_eq!(seq_count, bcount_count);
            println!("ALL SOLVERS FOUND EXACTLY THE SAME NUMBER OF SOLUTIONS: {seq_count}");
        }
        SolverMode::Verify => {
            println!("Running verification of constraint satisfaction and solver equivalence...");

            let seq_sol = solve_sequential(&people, &talks, args.tracks, args.slots);
            let par_sol = solve_parallel(&people, &talks, args.tracks, args.slots, args.depth);
            let bseq_sol = solve_bitmask_sequential(&people, &talks, args.tracks, args.slots);
            let bpar_sol =
                solve_bitmask_parallel(&people, &talks, args.tracks, args.slots, args.depth);

            println!("Validating every sequential solution...");
            for (i, sol) in seq_sol.iter().enumerate() {
                if let Err(e) =
                    validate_timetable(sol, args.slots, args.tracks, total_talks, &clashes)
                {
                    panic!("Sequential solution #{i} violated constraints: {e}");
                }
            }

            println!("Validating every parallel solution...");
            for (i, sol) in par_sol.iter().enumerate() {
                if let Err(e) =
                    validate_timetable(sol, args.slots, args.tracks, total_talks, &clashes)
                {
                    panic!("Parallel solution #{i} violated constraints: {e}");
                }
            }

            println!("Validating every bitmask parallel solution...");
            for (i, sol) in bpar_sol.iter().enumerate() {
                if let Err(e) =
                    validate_timetable(sol, args.slots, args.tracks, total_talks, &clashes)
                {
                    panic!("Bitmask parallel solution #{i} violated constraints: {e}");
                }
            }

            assert_eq!(seq_sol.len(), par_sol.len());
            assert_eq!(seq_sol.len(), bseq_sol.len());
            assert_eq!(seq_sol.len(), bpar_sol.len());

            println!(
                "SUCCESS: Verified {} valid solutions across all solvers without error!",
                seq_sol.len()
            );
        }
    }
    println!("===============================================================");
}
