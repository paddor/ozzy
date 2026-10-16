//! Render OMQ-style SVG charts from validated append-only result ledgers.
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]
mod chart;

use clap::Parser;
use ozzy_bench::automation::{self, Result, records};
use serde_json::json;
use std::path::{Path, PathBuf};

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

impl Args {
    /// Validated chart rows of `run_ids`, restricted to the selected modes.
    fn select(&self, run_ids: &[String], iggy_run_id: Option<&str>) -> Result<serde_json::Value> {
        let mut data = if self.fixed_load {
            records::select_fixed_load(&automation::cache()?, run_ids)?
        } else {
            records::select(&automation::cache()?, run_ids, iggy_run_id)?
        };
        records::retain_modes(&mut data, &self.modes)?;
        Ok(data)
    }

    /// Replace measured cases with each explicit rerun.
    fn replace_cases(&self, data: &mut serde_json::Value) -> Result<()> {
        for id in &self.replace_run_id {
            let replacement = self.select(std::slice::from_ref(id), None)?;
            chart::replacement::apply(data, &replacement)?;
        }
        Ok(())
    }

    /// Attach the cached external references the arguments name.
    fn include_references(&self, data: &mut serde_json::Value) -> Result<()> {
        let (references, implementations): (_, &[_]) = if self.external_reference_run_id.is_empty()
        {
            (&self.iggy_reference_run_id, &["iggy"])
        } else {
            (&self.external_reference_run_id, &["iggy", "redpanda"])
        };
        if !references.is_empty() {
            if !self.fixed_load && references.len() != 1 {
                return Err("saturation charts require one external reference run".into());
            }
            records::include_references_with_ozzy_only_rates(
                &automation::cache()?,
                data,
                &records::References {
                    run_ids: references,
                    implementations,
                    fixed_load: self.fixed_load,
                },
                &self.ozzy_only_rate,
            )?;
        }
        if !self.redpanda_reference_run_id.is_empty() {
            if !self.fixed_load && self.redpanda_reference_run_id.len() != 1 {
                return Err("saturation charts require one Redpanda reference run".into());
            }
            records::include_references_with_ozzy_only_rates(
                &automation::cache()?,
                data,
                &records::References {
                    run_ids: &self.redpanda_reference_run_id,
                    implementations: &["redpanda"],
                    fixed_load: self.fixed_load,
                },
                &self.ozzy_only_rate,
            )?;
        }
        Ok(())
    }

    /// The `-suffix` of chart and input file names; empty for none.
    fn file_suffix(&self) -> String {
        if self.suffix.is_empty() {
            String::new()
        } else {
            format!("-{}", self.suffix)
        }
    }
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
    let mut data = args.select(&args.run_id, args.iggy_run_id.as_deref())?;
    if let Some(output) = &args.regression_output {
        return regression(&args, output, &mut data);
    }
    args.replace_cases(&mut data)?;
    if !args.ozzy_only_rate.is_empty() {
        data["ozzy_only_rates"] = json!(args.ozzy_only_rate);
    }
    // Failed offered rates also need comparison coverage.
    if !args.failed_run_id.is_empty() {
        records::include_failed_loads(&automation::cache()?, &mut data, &args.failed_run_id)?;
        records::retain_modes(&mut data, &args.modes)?;
    }
    chart::coverage::limit_rates(&mut data, &args.max_rate)?;
    args.include_references(&mut data)?;
    render(&args, &data)
}

/// Save the chart input beside other artifacts, then draw the charts.
fn render(args: &Args, data: &serde_json::Value) -> Result<()> {
    let output = args
        .output_dir
        .clone()
        .unwrap_or_else(|| automation::root().join("doc/charts"));
    let cache = automation::artifact_root().join("ozzy-chart-inputs");
    std::fs::create_dir_all(&cache)?;
    let suffix = args.file_suffix();
    let kind = if args.fixed_load { "-fixed-load" } else { "" };
    let modes = if args.modes.is_empty() {
        String::new()
    } else {
        format!("-{}", args.modes.join("-"))
    };
    let input = cache.join(format!("{}{modes}{kind}{suffix}.json", args.run_id[0]));
    automation::json_file(&input, data)?;
    if args.fixed_load {
        chart::render_fixed_load(data, &output, &suffix)
    } else {
        chart::render(data, &output, &suffix)
    }
}

/// Gate `data` against the baseline runs and save the per-cell report.
fn regression(args: &Args, output: &Path, data: &mut serde_json::Value) -> Result<()> {
    let mut baseline = args.select(&args.baseline_run_id, None)?;
    chart::coverage::limit_rates(&mut baseline, &args.max_rate)?;
    chart::coverage::limit_rates(data, &args.max_rate)?;
    let report = chart::regression::compare(&baseline, data, args.fixed_load)?;
    automation::json_file(output, &report)?;
    println!("{}: {}", report["status"], output.display());
    if report["status"] == "pass" {
        Ok(())
    } else {
        Err("performance gate did not pass; inspect the saved per-cell report".into())
    }
}
