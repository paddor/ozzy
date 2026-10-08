//! Render OMQ-style SVG charts from validated append-only result ledgers.
#![forbid(unsafe_code)]
mod chart;

use clap::Parser;
use ozzy_bench::automation::{self, Result, records};
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    /// Completed run; repeat for disjoint modes from the same checkout.
    #[arg(long, required = true)]
    run_id: Vec<String>,
    /// Replace measured cases with a rerun of the identical worker binary and controls.
    #[arg(long)]
    replace_run_id: Vec<String>,
    /// Select measured modes before attaching cached comparisons.
    #[arg(long, value_delimiter = ',')]
    modes: Vec<String>,
    /// Explicit compatible Iggy baseline for local Ozzy-only runs.
    #[arg(long)]
    iggy_run_id: Option<String>,
    /// Cached Iggy reference retaining its original batch ceiling and workload revision.
    #[arg(long, conflicts_with_all = ["iggy_run_id", "external_reference_run_id"])]
    iggy_reference_run_id: Vec<String>,
    /// Independently measured Redpanda reference alongside cached Iggy results.
    #[arg(long, conflicts_with_all = ["iggy_run_id", "external_reference_run_id"])]
    redpanda_reference_run_id: Vec<String>,
    /// Cached Iggy and Redpanda references, retaining each original measurement.
    #[arg(long, conflicts_with_all = ["iggy_run_id", "iggy_reference_run_id"])]
    external_reference_run_id: Vec<String>,
    /// Scheduled-arrival latency versus offered load, in separate SVGs.
    #[arg(long, conflicts_with = "iggy_run_id")]
    fixed_load: bool,
    /// Explicit fixed-load ceiling per record size, `SIZE:RECORDS_PER_SECOND`.
    #[arg(long, requires = "fixed_load")]
    max_rate: Vec<String>,
    /// Compare against completed Ozzy runs instead of rendering charts.
    #[arg(long, requires = "regression_output", conflicts_with_all = ["iggy_run_id", "iggy_reference_run_id", "redpanda_reference_run_id", "external_reference_run_id", "replace_run_id", "failed_run_id", "output_dir"])]
    baseline_run_id: Vec<String>,
    /// Save the per-cell 5% regression gate and both validated provenances.
    #[arg(long, requires = "baseline_run_id")]
    regression_output: Option<PathBuf>,
    /// Explicit load with an Ozzy measurement but no external reference.
    #[arg(long, requires = "fixed_load")]
    ozzy_only_rate: Vec<u64>,
    /// Explicit single-case backlog failures, annotated without latency values.
    #[arg(long, requires = "fixed_load")]
    failed_run_id: Vec<String>,
    /// SVG root. Defaults to doc/charts.
    #[arg(long)]
    output_dir: Option<PathBuf>,
    /// Optional descriptive filename suffix, for example control.
    #[arg(long, default_value = "")]
    suffix: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args
        .suffix
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err("suffix must contain only ASCII letters, digits, or hyphens".into());
    }
    automation::isolation::require_idle()?;
    let mut data = if args.fixed_load {
        records::select_fixed_load(&automation::cache(), &args.run_id)?
    } else {
        records::select(
            &automation::cache(),
            &args.run_id,
            args.iggy_run_id.as_deref(),
        )?
    };
    records::retain_modes(&mut data, &args.modes)?;
    if args.regression_output.is_some() {
        return regression(&args, &mut data);
    }
    for id in &args.replace_run_id {
        let mut replacement = if args.fixed_load {
            records::select_fixed_load(&automation::cache(), std::slice::from_ref(id))?
        } else {
            records::select(&automation::cache(), std::slice::from_ref(id), None)?
        };
        records::retain_modes(&mut replacement, &args.modes)?;
        chart::replacement::apply(&mut data, &replacement)?;
    }
    if !args.ozzy_only_rate.is_empty() {
        data["ozzy_only_rates"] = json!(args.ozzy_only_rate);
    }
    // Failed offered rates also need comparison coverage.
    if !args.failed_run_id.is_empty() {
        records::include_failed_loads(&automation::cache(), &mut data, &args.failed_run_id)?;
        records::retain_modes(&mut data, &args.modes)?;
    }
    chart::coverage::limit_rates(&mut data, &args.max_rate)?;
    let (references, implementations): (_, &[_]) = if args.external_reference_run_id.is_empty() {
        (&args.iggy_reference_run_id, &["iggy"])
    } else {
        (&args.external_reference_run_id, &["iggy", "redpanda"])
    };
    if !references.is_empty() {
        if !args.fixed_load && references.len() != 1 {
            return Err("saturation charts require one external reference run".into());
        }
        records::include_references_with_ozzy_only_rates(
            &automation::cache(),
            &mut data,
            references,
            implementations,
            args.fixed_load,
            &args.ozzy_only_rate,
        )?;
    }
    if !args.redpanda_reference_run_id.is_empty() {
        if !args.fixed_load && args.redpanda_reference_run_id.len() != 1 {
            return Err("saturation charts require one Redpanda reference run".into());
        }
        records::include_references_with_ozzy_only_rates(
            &automation::cache(),
            &mut data,
            &args.redpanda_reference_run_id,
            &["redpanda"],
            args.fixed_load,
            &args.ozzy_only_rate,
        )?;
    }
    let output = args
        .output_dir
        .unwrap_or_else(|| automation::root().join("doc/charts"));
    let cache = automation::artifact_root().join("ozzy-chart-inputs");
    std::fs::create_dir_all(&cache)?;
    let suffix = if args.suffix.is_empty() {
        String::new()
    } else {
        format!("-{}", args.suffix)
    };
    let kind = if args.fixed_load { "-fixed-load" } else { "" };
    let modes = if args.modes.is_empty() {
        String::new()
    } else {
        format!("-{}", args.modes.join("-"))
    };
    let input = cache.join(format!("{}{modes}{kind}{suffix}.json", args.run_id[0]));
    automation::json_file(&input, &data)?;
    if args.fixed_load {
        chart::render_fixed_load(&data, &output, &suffix)
    } else {
        chart::render(&data, &output, &suffix)
    }
}

fn regression(args: &Args, data: &mut serde_json::Value) -> Result<()> {
    if let Some(output) = &args.regression_output {
        let mut baseline = if args.fixed_load {
            records::select_fixed_load(&automation::cache(), &args.baseline_run_id)?
        } else {
            records::select(&automation::cache(), &args.baseline_run_id, None)?
        };
        records::retain_modes(&mut baseline, &args.modes)?;
        chart::coverage::limit_rates(&mut baseline, &args.max_rate)?;
        chart::coverage::limit_rates(data, &args.max_rate)?;
        let report = chart::regression::compare(&baseline, data, args.fixed_load)?;
        automation::json_file(output, &report)?;
        println!("{}: {}", report["status"], output.display());
        return if report["status"] == "pass" {
            Ok(())
        } else {
            Err("performance gate did not pass; inspect the saved per-cell report".into())
        };
    }
    unreachable!("regression output was selected")
}
