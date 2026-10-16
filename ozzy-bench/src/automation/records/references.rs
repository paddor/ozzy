//! External reference series attached to validated Ozzy selections.
use super::{
    Result, comparable_reference, json_field, json_field_mut,
    selection::{select, select_fixed_load, select_inner},
    windows::same_fixed_load_timing,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

/// External reference runs to attach, and how the chart compares loads.
#[derive(Debug, Clone, Copy)]
pub struct References<'a> {
    /// Completed reference runs; the first names the reference in captions.
    pub run_ids: &'a [String],
    /// External implementations taken from those runs.
    pub implementations: &'a [&'a str],
    /// Fixed offered loads rather than saturation.
    pub fixed_load: bool,
}

/// Independently validated historical reference, not an identical-workload baseline.
/// Workload revision, SDK ceiling, and Ozzy dependencies may differ; retain provenance.
pub fn select_with_iggy_reference(directory: &Path, ids: &[String], id: &str) -> Result<Value> {
    select_with_references(
        directory,
        ids,
        &References {
            run_ids: &[id.to_owned()],
            implementations: &["iggy"],
            fixed_load: false,
        },
    )
}

/// Reuse explicitly selected external measurements with their original provenance.
pub fn select_with_external_reference(directory: &Path, ids: &[String], id: &str) -> Result<Value> {
    select_with_references(
        directory,
        ids,
        &References {
            run_ids: &[id.to_owned()],
            implementations: &["iggy", "redpanda"],
            fixed_load: false,
        },
    )
}

/// Join selected load runs with compatible external reference rows.
pub fn select_fixed_load_with_external_reference(
    directory: &Path,
    ids: &[String],
    reference_ids: &[String],
) -> Result<Value> {
    select_with_references(
        directory,
        ids,
        &References {
            run_ids: reference_ids,
            implementations: &["iggy", "redpanda"],
            fixed_load: true,
        },
    )
}

/// Join selected load runs with the compatible Iggy reference ledger.
pub fn select_fixed_load_with_iggy_reference(
    directory: &Path,
    ids: &[String],
    reference_ids: &[String],
) -> Result<Value> {
    select_with_references(
        directory,
        ids,
        &References {
            run_ids: reference_ids,
            implementations: &["iggy"],
            fixed_load: true,
        },
    )
}

fn select_with_references(
    directory: &Path,
    ids: &[String],
    references: &References<'_>,
) -> Result<Value> {
    let mut data = if references.fixed_load {
        select_fixed_load(directory, ids)?
    } else {
        select(directory, ids, None)?
    };
    include_references(directory, &mut data, references)?;
    Ok(data)
}

/// Attach external comparisons to validated, optionally mode-filtered results.
/// Every selected case still requires reference coverage.
pub fn include_references(
    directory: &Path,
    data: &mut Value,
    references: &References<'_>,
) -> Result<()> {
    include_references_with_ozzy_only_rates(directory, data, references, &[])
}

/// Keep explicitly measured Ozzy-only loads while attaching external series
/// at every other selected load. Older comparison runs have no 1M/s sample.
pub fn include_references_with_ozzy_only_rates(
    directory: &Path,
    data: &mut Value,
    references: &References<'_>,
    ozzy_only_rates: &[u64],
) -> Result<()> {
    let implementations = references.implementations;
    if !references.fixed_load && !ozzy_only_rates.is_empty() {
        return Err("Ozzy-only rates require a fixed-load chart".into());
    }
    if implementations.is_empty()
        || implementations
            .iter()
            .any(|implementation| !matches!(*implementation, "iggy" | "redpanda"))
    {
        return Err("select at least one supported external reference implementation".into());
    }
    let reference = select_inner(
        directory,
        references.run_ids,
        None,
        references.fixed_load,
        Some(implementations),
    )?;
    if comparable_reference(data) != comparable_reference(&reference) {
        return Err("incompatible reference host, compiler, or measurement settings".into());
    }
    validate_ozzy_only_rates(data, &reference, implementations, ozzy_only_rates)?;
    require_new_series(data, implementations)?;
    let wanted = referenced_ozzy_cases(data, &reference, references.fixed_load, ozzy_only_rates)?;
    let covers = |row: &&Value| {
        implementations.contains(&row["case"]["impl"].as_str().unwrap_or(""))
            && wanted.contains(&reference_case_key(row))
    };
    let selected: Vec<_> = json_field(&reference, "summary", Value::as_array)?
        .iter()
        .filter(covers)
        .cloned()
        .collect();
    let selected_failures: Vec<_> = if references.fixed_load {
        reference["incomplete"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(covers)
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    require_reference_coverage(&wanted, &selected, &selected_failures, implementations)?;
    json_field_mut(data, "summary", Value::as_array_mut)?.extend(selected);
    if !selected_failures.is_empty() {
        if data["incomplete"].is_null() {
            data["incomplete"] = json!([]);
        }
        json_field_mut(data, "incomplete", Value::as_array_mut)?.extend(selected_failures);
    }
    record_provenance(data, &reference, references);
    Ok(())
}

/// The selection must hold Ozzy rows and none of the attached implementations.
fn require_new_series(data: &Value, implementations: &[&str]) -> Result<()> {
    let existing = json_field(data, "summary", Value::as_array)?
        .iter()
        .chain(data["incomplete"].as_array().into_iter().flatten());
    if !existing.clone().any(|r| r["case"]["impl"] == "ozzy")
        || existing
            .clone()
            .any(|r| implementations.contains(&r["case"]["impl"].as_str().unwrap_or("")))
    {
        return Err("reference selection requires Ozzy and a new external series".into());
    }
    Ok(())
}

/// Keep each attached implementation's version, batch ceiling, and reference
/// provenance; the reference never claims an identical workload.
fn record_provenance(data: &mut Value, reference: &Value, references: &References<'_>) {
    for implementation in references.implementations {
        if let Some(value) = reference["versions"].get(*implementation) {
            data["versions"][*implementation] = value.clone();
        }
    }
    for implementation in references.implementations {
        data["batch_records"][*implementation] =
            reference["batch_records"][*implementation].clone();
        data["references"][*implementation] = json!({
            "run_id":references.run_ids[0],"run_ids":references.run_ids,
            "compatibility":reference["compatibility"],
            "compatibility_by_run":reference["compatibility_by_run"],
            "sources":reference["sources"],"executables":reference["executables"],
            "identical_workload":false
        });
    }
}

/// The mode, size, pattern, and rate that pair an Ozzy case with its reference.
pub(super) fn reference_case_key(row: &Value) -> String {
    json!([
        row["case"]["mode"],
        row["case"]["size"],
        row["case"]["pattern"],
        row["case"]["rate"]
    ])
    .to_string()
}

fn referenced_ozzy_cases(
    data: &Value,
    reference: &Value,
    fixed_load: bool,
    ozzy_only_rates: &[u64],
) -> Result<BTreeSet<String>> {
    let selected: Vec<_> = data["summary"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(data["incomplete"].as_array().into_iter().flatten())
        .filter(|row| {
            row["case"]["impl"] == "ozzy"
                && !row["case"]["rate"]
                    .as_u64()
                    .is_some_and(|rate| ozzy_only_rates.contains(&rate))
        })
        .collect();
    if fixed_load {
        for row in &selected {
            let rate = row["case"]["rate"].to_string();
            if !same_fixed_load_timing(
                &data["fixed_load_windows"][&rate],
                &reference["fixed_load_windows"][&rate],
            ) {
                return Err("reference fixed-load timing differs".into());
            }
        }
    }
    Ok(selected.into_iter().map(reference_case_key).collect())
}

fn validate_ozzy_only_rates(
    data: &Value,
    reference: &Value,
    implementations: &[&str],
    rates: &[u64],
) -> Result<()> {
    let measured = data["summary"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(data["incomplete"].as_array().into_iter().flatten());
    let external = reference["summary"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(reference["incomplete"].as_array().into_iter().flatten());
    for rate in rates {
        if !measured
            .clone()
            .any(|row| row["case"]["impl"] == "ozzy" && row["case"]["rate"] == *rate)
        {
            return Err(format!("no selected Ozzy measurement at {rate}/s").into());
        }
        if external.clone().any(|row| {
            implementations.contains(&row["case"]["impl"].as_str().unwrap_or(""))
                && row["case"]["rate"] == *rate
        }) {
            return Err(format!("external reference already measures {rate}/s").into());
        }
    }
    Ok(())
}

fn require_reference_coverage(
    wanted: &BTreeSet<String>,
    selected: &[Value],
    failures: &[Value],
    implementations: &[&str],
) -> Result<()> {
    for implementation in implementations {
        let covered = selected
            .iter()
            .chain(failures)
            .filter(|row| row["case"]["impl"] == *implementation)
            .map(reference_case_key)
            .collect::<BTreeSet<_>>();
        if &covered != wanted {
            return Err(format!("{implementation} reference does not cover selected cases").into());
        }
    }
    Ok(())
}
