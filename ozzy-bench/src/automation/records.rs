//! Append-only result ledgers and fail-closed selection for charts.
mod compatible;
mod failures;
mod references;
mod selection;
mod windows;
use super::{Result, finite, isolation, json_field, json_field_mut};
use compatible::{comparable_reference, compatibility, controls};
pub use failures::include_failed_loads;
pub use references::{
    References, include_references, include_references_with_ozzy_only_rates,
    select_fixed_load_with_external_reference, select_fixed_load_with_iggy_reference,
    select_with_external_reference, select_with_iggy_reference,
};
pub use selection::{retain_modes, select, select_fixed_load};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

/// Append a result row to the implementation ledger and this run directory.
pub fn append(directory: &Path, implementation: &str, row: &Value) -> Result<()> {
    if !matches!(implementation, "ozzy" | "iggy" | "kafka" | "redpanda") {
        return Err("unknown result implementation".into());
    }
    fs::create_dir_all(directory)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(format!("{implementation}.jsonl")))?;
    file.lock()?;
    serde_json::to_writer(&mut file, row)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

/// Read only successful completed measurements from a result ledger.
pub fn completed(path: &Path) -> Result<Vec<Value>> {
    let rows = match fs::File::open(path) {
        Ok(file) => BufReader::new(file)
            .lines()
            .map(|line| Ok(serde_json::from_str::<Value>(&line?)?))
            .collect::<Result<Vec<_>>>()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(error) => return Err(error.into()),
    };
    let ids = |kind| {
        rows.iter()
            .filter(|r| r["kind"] == kind)
            .map(|r| r["run_id"].as_str().unwrap_or("").to_owned())
            .collect::<BTreeSet<_>>()
    };
    let valid = ids("run-complete")
        .difference(&ids("run-failed"))
        .cloned()
        .collect::<BTreeSet<_>>();
    Ok(rows
        .into_iter()
        .filter(|r| {
            r["kind"] == "measurement" && r["run_id"].as_str().is_some_and(|id| valid.contains(id))
        })
        .collect())
}

/// Aggregate repeated measurements while retaining their provenance.
pub fn summarize(rows: &[Value]) -> Result<Value> {
    let mut groups = BTreeMap::<String, Vec<&Value>>::new();
    for row in rows {
        groups.entry(row["case"].to_string()).or_default().push(row);
    }
    let mut output = vec![];
    for group in groups.values() {
        if let Some(reason) = group.iter().find_map(|row| row.get("failure")) {
            output.push(json!({
                "case": group[0]["case"],
                "repetitions": group.len(),
                "failure": reason,
                "measurements": {}
            }));
            continue;
        }
        let mut measurements = serde_json::Map::new();
        for key in group[0]["measurements"]
            .as_object()
            .ok_or("missing measurements")?
            .keys()
        {
            let mut values = group
                .iter()
                .map(|r| finite(&r["measurements"][key]))
                .collect::<Result<Vec<_>>>()?;
            values.sort_by(f64::total_cmp);
            let n = values.len();
            let median = values[(n - 1) / 2].midpoint(values[n / 2]);
            measurements.insert(
                key.clone(),
                json!({"median":median,"minimum":values[0],"maximum":values[n-1]}),
            );
        }
        output.push(
            json!({"case":group[0]["case"],"repetitions":group.len(),"measurements":measurements}),
        );
    }
    Ok(json!(output))
}

/// Close a run in each implementation's ledger: `run-complete` and a summary
/// file of its rows, or `run-failed` with the reason, passing the error on.
pub fn close_run<I: AsRef<str>>(
    implementations: &BTreeSet<I>,
    id: &str,
    directory: &Path,
    outcome: Result<Vec<Value>>,
) -> Result<()> {
    let ledger = super::cache()?;
    let rows = match outcome {
        Ok(rows) => rows,
        Err(error) => {
            for implementation in implementations {
                append(
                    &ledger,
                    implementation.as_ref(),
                    &json!({"kind":"run-failed","run_id":id,"reason":error.to_string()}),
                )?;
            }
            return Err(error);
        }
    };
    for implementation in implementations {
        append(
            &ledger,
            implementation.as_ref(),
            &json!({"kind":"run-complete","run_id":id}),
        )?;
    }
    super::json_file(&directory.join("summary.json"), &summarize(&rows)?)?;
    println!("COMPLETE {id}");
    Ok(())
}
