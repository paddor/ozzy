//! Fail-closed selection of completed runs for one chart.
use super::{
    Result, comparable_reference, compatibility, completed, json_field, json_field_mut,
    references::reference_case_key, summarize, windows::fixed_load_windows,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Select comparison runs and optional baseline rows for chart generation.
pub fn select(directory: &Path, ids: &[String], baseline: Option<&str>) -> Result<Value> {
    select_inner(directory, ids, baseline, false, None)
}

/// Select offered-load results, retaining every requested run identifier.
pub fn select_fixed_load(directory: &Path, ids: &[String]) -> Result<Value> {
    select_inner(directory, ids, None, true, None)
}

/// Restrict an already validated selection to explicit measured modes.
/// Keep run provenance intact and apply the same filter to overload annotations.
pub fn retain_modes(data: &mut Value, modes: &[String]) -> Result<()> {
    if modes.is_empty() {
        return Ok(());
    }
    for mode in modes {
        if !data["summary"]
            .as_array()
            .into_iter()
            .flatten()
            .chain(data["incomplete"].as_array().into_iter().flatten())
            .any(|row| row["case"]["mode"] == *mode)
        {
            return Err(format!("no selected measurements for mode {mode}").into());
        }
    }
    for field in ["summary", "incomplete"] {
        if let Some(rows) = data[field].as_array_mut() {
            rows.retain(|row| modes.iter().any(|mode| row["case"]["mode"] == *mode));
        }
    }
    Ok(())
}

fn load_rows(directory: &Path) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for implementation in ["ozzy", "iggy", "redpanda"] {
        rows.extend(completed(
            &directory.join(format!("{implementation}.jsonl")),
        )?);
    }
    Ok(rows)
}

/// A stage that overloads in any repetition becomes a chart annotation.
/// Never summarize only its successful repetitions.
fn split_failures(selected: &[Value], fixed_load: bool) -> Result<(Vec<Value>, Vec<Value>)> {
    let (failed, measured): (Vec<_>, Vec<_>) = selected
        .iter()
        .cloned()
        .partition(|row| row.get("failure").is_some());
    if !fixed_load && !failed.is_empty() {
        return Err("overloaded ramp stages belong only to fixed-load charts".into());
    }
    let failed_cases: BTreeSet<_> = failed.iter().map(|row| row["case"].to_string()).collect();
    let measured = measured
        .into_iter()
        .filter(|row| !failed_cases.contains(&row["case"].to_string()))
        .collect();
    let failed = failed
        .iter()
        .map(|row| json!({"case":row["case"],"failure":row["failure"]}))
        .collect();
    Ok((failed, measured))
}

/// Validate the selected runs and an optional Iggy baseline as one chart.
/// Reference selections keep only `reference_implementations` rows and keep
/// each run's own controls.
pub(super) fn select_inner(
    directory: &Path,
    ids: &[String],
    baseline: Option<&str>,
    fixed_load: bool,
    reference_implementations: Option<&[&str]>,
) -> Result<Value> {
    if ids.is_empty() {
        return Err("select at least one completed run".into());
    }
    let mut rows = load_rows(directory)?;
    // Historical ledgers stay append-only. Charts show supported modes and
    // storage codecs; cached references may still hold removed modes.
    rows.retain(|row| {
        matches!(
            row["case"]["mode"].as_str(),
            Some("buffered" | "durable" | "disk-quorum")
        ) && matches!(row["case"]["codec"].as_str(), Some("raw" | "lz4"))
    });
    if let Some(implementations) = reference_implementations {
        rows.retain(|row| implementations.contains(&row["case"]["impl"].as_str().unwrap_or("")));
    }
    let mut selected = selected_runs(&rows, ids)?;
    if let Some(id) = baseline {
        let found = iggy_baseline(&rows, &selected, id)?;
        selected.extend(found);
    }
    let (failed, measured) = split_failures(&selected, fixed_load)?;
    let windows = fixed_load
        .then(|| fixed_load_windows(&selected))
        .transpose()?;
    let (expected, compatibility_by_run) =
        validated_controls(&selected, fixed_load, reference_implementations.is_some())?;
    if !one_pattern_and_load(&selected, fixed_load) {
        return Err("select one payload pattern and offered load".into());
    }
    same_ozzy_source(&selected)?;
    let mut data = finish_selection(ids, baseline, &selected, &measured, &expected)?;
    if reference_implementations.is_some() {
        data["compatibility_by_run"] = compatibility_by_run;
    }
    if let Some(windows) = windows {
        data["fixed_load_windows"] = windows;
    }
    if !failed.is_empty() {
        data["incomplete"] = json!(failed);
    }
    Ok(data)
}

/// Every row of each requested run; a run without rows fails the selection.
fn selected_runs(rows: &[Value], ids: &[String]) -> Result<Vec<Value>> {
    let mut selected = vec![];
    for id in ids {
        let found: Vec<_> = rows
            .iter()
            .filter(|r| r["run_id"] == *id)
            .cloned()
            .collect();
        if found.is_empty() {
            return Err(format!("no completed measurements for {id}").into());
        }
        selected.extend(found);
    }
    Ok(selected)
}

/// Iggy rows of run `id` covering exactly the selected cases.
fn iggy_baseline(rows: &[Value], selected: &[Value], id: &str) -> Result<Vec<Value>> {
    if selected.iter().any(|r| r["case"]["impl"] == "iggy") {
        return Err("selected runs already contain Iggy".into());
    }
    let wanted: BTreeSet<_> = selected.iter().map(reference_case_key).collect();
    let found: Vec<_> = rows
        .iter()
        .filter(|r| {
            r["run_id"] == id
                && r["case"]["impl"] == "iggy"
                && wanted.contains(&reference_case_key(r))
        })
        .cloned()
        .collect();
    if wanted.is_empty()
        || found
            .iter()
            .map(reference_case_key)
            .collect::<BTreeSet<_>>()
            != wanted
    {
        return Err("Iggy baseline does not cover selected cases".into());
    }
    Ok(found)
}

/// Whether all rows share one payload pattern and, for saturation charts,
/// one offered rate.
fn one_pattern_and_load(selected: &[Value], fixed_load: bool) -> bool {
    selected
        .iter()
        .map(|r| {
            json!([
                r["case"]["pattern"],
                if fixed_load {
                    &Value::Null
                } else {
                    &r["case"]["rate"]
                }
            ])
            .to_string()
        })
        .collect::<BTreeSet<_>>()
        .len()
        == 1
}

fn validated_controls(
    selected: &[Value],
    fixed_load: bool,
    external_reference: bool,
) -> Result<(Value, Value)> {
    let compare = |row: &Value| -> Result<Value> {
        let mut value = compatibility(row)?;
        if fixed_load {
            let config = json_field_mut(&mut value, "configuration", Value::as_object_mut)?;
            config.remove("records_per_second");
            config.remove("duration");
            // Stage windows are checked per rate; ramps may end at different rates.
            config.remove("ramp");
        }
        Ok(value)
    };
    let controls = selected.iter().map(compare).collect::<Result<Vec<_>>>()?;
    let expected = &controls[0];
    // External references may span workload/dependency revisions. Each run
    // must still be internally uniform and retain its original controls.
    let mut compatibility_by_run = serde_json::Map::new();
    for (row, controls) in selected.iter().zip(&controls) {
        let id = row["run_id"].as_str().ok_or("missing run ID")?;
        if let Some(prior) = compatibility_by_run.insert(id.into(), controls.clone())
            && prior != *controls
        {
            return Err("incompatible measurement settings within one run".into());
        }
    }
    let matches = |controls: &Value| {
        if external_reference {
            comparable_reference(&json!({"compatibility":controls}))
                == comparable_reference(&json!({"compatibility":expected}))
        } else {
            controls == expected
        }
    };
    if !controls.iter().all(matches) {
        return Err("incompatible workload, host, compiler, or measurement configuration".into());
    }
    Ok((expected.clone(), json!(compatibility_by_run)))
}

fn same_ozzy_source(rows: &[Value]) -> Result<()> {
    let settings: BTreeSet<_> = rows
        .iter()
        .filter(|r| r["case"]["impl"] == "ozzy")
        .map(|r| {
            json!([
                r["configuration"]["payload_compression"],
                r["configuration"]["payload_compression_threshold"]
            ])
            .to_string()
        })
        .collect();
    if settings.len() > 1 {
        return Err("selected Ozzy runs use different payload compression settings".into());
    }
    if rows
        .iter()
        .filter(|r| r["case"]["impl"] == "ozzy")
        .map(|r| r["source"].to_string())
        .collect::<BTreeSet<_>>()
        .len()
        > 1
    {
        return Err("selected Ozzy runs contain different source revisions".into());
    }
    Ok(())
}

fn finish_selection(
    ids: &[String],
    baseline: Option<&str>,
    selected: &[Value],
    measured: &[Value],
    expected: &Value,
) -> Result<Value> {
    let mut groups = BTreeMap::<String, Vec<&Value>>::new();
    let mut sources = serde_json::Map::new();
    let mut executables = serde_json::Map::new();
    let mut versions = serde_json::Map::new();
    for row in selected {
        groups.entry(row["case"].to_string()).or_default().push(row);
        let id = json_field(row, "run_id", Value::as_str)?;
        sources.insert(id.into(), row["source"].clone());
        executables.insert(id.into(), row["executable_sha256"].clone());
        if let Some(version) = row["server_identity"]["release"]
            .as_str()
            .or_else(|| row["server_identity"]["build"]["release"].as_str())
        {
            let implementation = row["case"]["impl"]
                .as_str()
                .ok_or("missing implementation")?;
            if let Some(previous) = versions.insert(implementation.into(), json!(version))
                && previous != version
            {
                return Err("selected broker versions differ".into());
            }
        }
    }
    for group in groups.values() {
        let count = group[0]["configuration"]["repetitions"]
            .as_u64()
            .ok_or("missing repetitions")?;
        let mut repetitions = group
            .iter()
            .map(|r| r["repetition"].as_u64().unwrap_or(0))
            .collect::<Vec<_>>();
        repetitions.sort_unstable();
        if group.iter().any(|r| r["run_id"] != group[0]["run_id"])
            || repetitions != (1..=count).collect::<Vec<_>>()
        {
            return Err("duplicate case, incomplete or duplicate repetitions".into());
        }
    }
    let payload_compression = selected
        .iter()
        .find(|row| row["case"]["impl"] == "ozzy")
        .map_or(Value::Null, |row| {
            row["configuration"]["payload_compression"].clone()
        });
    Ok(
        json!({"run_ids":ids,"baseline_run_id":baseline,"compatibility":expected,"sources":sources,"executables":executables,"versions":versions,"batch_records":batch_records(selected)?,"writer_protocols":writer_protocols(selected)?,"writer_payload_caps":writer_payload_caps(selected)?,"payload_compression":payload_compression,"summary":summarize(measured)?}),
    )
}

// SDK grouping is implementation-specific. Never combine different ceilings
// within one implementation, and retain each ceiling for the chart caption.
fn batch_records(rows: &[Value]) -> Result<Value> {
    let mut limits = serde_json::Map::new();
    for row in rows {
        let implementation = row["case"]["impl"]
            .as_str()
            .ok_or("missing implementation")?;
        let limit = row["configuration"]["request_records"].clone();
        if let Some(previous) = limits.insert(implementation.into(), limit.clone())
            && previous != limit
        {
            return Err("selected runs mix batch ceilings within one implementation".into());
        }
    }
    Ok(Value::Object(limits))
}

// The worker's effective payload bound can be smaller than its requested target.
// Preserve the largest observed bound per mode for captions across record sizes.
fn writer_payload_caps(rows: &[Value]) -> Result<Value> {
    let mut caps = BTreeMap::<&str, u64>::new();
    for row in rows.iter().filter(|row| row["case"]["impl"] == "ozzy") {
        let mode = row["case"]["mode"].as_str().ok_or("missing mode")?;
        if let Some(bytes) = row["raw"]["writer_batch_payload_bytes_max"].as_u64() {
            caps.entry(mode)
                .and_modify(|cap| *cap = (*cap).max(bytes))
                .or_insert(bytes);
        }
    }
    Ok(json!(caps))
}

fn writer_protocols(rows: &[Value]) -> Result<Value> {
    let mut protocols = serde_json::Map::new();
    for row in rows.iter().filter(|row| row["case"]["impl"] == "ozzy") {
        let mode = row["case"]["mode"].as_str().ok_or("missing mode")?;
        // Historical native runs used PEER APPENDs before this field existed.
        let protocol = row["raw"]["writer_protocol"]
            .as_str()
            .unwrap_or("peer-appends");
        if !matches!(protocol, "peer-appends" | "push-records" | "push-appends") {
            return Err("unknown measured writer protocol".into());
        }
        // Early local PUSH runs retained the old mode-based label. Counters
        // record the actual wire path; derive the label without rewriting ledgers.
        let mut observed = None;
        for writer in row["raw"]["writers"].as_array().into_iter().flatten() {
            for lane in writer["lanes"].as_array().into_iter().flatten() {
                let stats = &lane["protocol_requests"];
                let Some(messages) = stats["data_messages"].as_u64() else {
                    continue;
                };
                let current = if messages == 0 {
                    "peer-appends"
                } else {
                    if stats["requests"].as_u64() != Some(messages)
                        || stats["max_records"].as_u64() != Some(1)
                        || stats["max_inflight_appends"].as_u64() != Some(0)
                    {
                        return Err("mixed or invalid measured writer traffic".into());
                    }
                    "push-records"
                };
                if observed.replace(current).is_some_and(|old| old != current) {
                    return Err("mixed native writer protocols in one result".into());
                }
            }
        }
        let protocol = observed.unwrap_or(protocol);
        if let Some(previous) = protocols.insert(mode.into(), json!(protocol))
            && previous != protocol
        {
            return Err("mixed native writer protocols in one chart mode".into());
        }
    }
    Ok(Value::Object(protocols))
}
