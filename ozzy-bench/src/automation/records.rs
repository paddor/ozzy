//! Append-only result ledgers and fail-closed selection for charts.
mod failures;
use super::{Result, finite, isolation};
pub use failures::include_failed_loads;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

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

const SELECTION: &[&str] = &[
    "impl",
    "sizes",
    "modes",
    "codecs",
    "patterns",
    "repetitions",
    "results_dir",
    "artifact_dir",
    "no_build",
    "check_only",
];

fn compatibility(row: &Value) -> Result<Value> {
    if !row["configuration"]["deployment"].is_null() {
        let entries = row["configuration"]["deployment"]
            .as_array()
            .filter(|entries| entries.len() == 3)
            .ok_or("invalid saved placement")?;
        if entries.iter().all(|p| p["ssh"].is_null())
            && row["configuration"]["deployment_kind"] != "all-local"
        {
            return Err("local placement lacks explicit topology label".into());
        }
    }
    if row["configuration"]["deployment"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|p| !p["ssh"].is_null()))
    {
        let audit = &row["remote_process_isolation"];
        if audit["samples"].as_u64().unwrap_or(0) == 0
            || audit["processes"].as_array().is_none_or(|p| p.len() != 1)
            || row["remote_after_shutdown"]
                .as_array()
                .is_none_or(|hosts| hosts.len() != 1 || hosts[0]["processes"] != json!([]))
        {
            return Err("distributed run lacks remote process isolation proof".into());
        }
    }
    if row["configuration"]["payload_zstd_level"].is_i64() {
        return Err("retired payload codec cannot enter current charts".into());
    }
    let proof = &row["process_isolation"];
    if proof["contract"] != isolation::CONTRACT
        || proof["observations"].as_u64().unwrap_or(0) == 0
        || proof["after_shutdown"] != isolation::empty_proof()
    {
        return Err("run lacks verified strict process isolation; rerun it".into());
    }
    controls(row)
}

fn controls(row: &Value) -> Result<Value> {
    if row["raw"]["broker_runtime"] == "production-deployment" {
        super::validation::production_cpu_placement(&row["raw"], &row["configuration"])?;
    }
    if row["workload_sha256"].is_null() || row["environment"].is_null() {
        return Err("run lacks workload/environment fingerprints; rerun it".into());
    }
    let mut config = row["configuration"]
        .as_object()
        .ok_or("missing configuration")?
        .clone();
    if let Some(workers) = row["raw"]["producer_workers"].as_u64() {
        config.insert("producer_workers".into(), json!(workers));
    }
    config.remove("payload_compression");
    // Native-only tuning is checked together across Ozzy rows below. External
    // adapters never use it, so it must not invalidate an external reference.
    config.remove("payload_compression_threshold");
    config.remove("request_records");
    for key in SELECTION {
        config.remove(*key);
    }
    Ok(
        json!({"workload":row["workload_sha256"],"environment":row["environment"],"compiler":row["source"]["compiler"],"build_environment":row["source"].get("build_environment").unwrap_or(&json!({})),"configuration":config}),
    )
}

pub fn select(directory: &Path, ids: &[String], baseline: Option<&str>) -> Result<Value> {
    select_inner(directory, ids, baseline, false)
}

pub fn select_fixed_load(directory: &Path, ids: &[String]) -> Result<Value> {
    select_inner(directory, ids, None, true)
}

/// Independently validated historical reference, not an identical-workload baseline.
/// Workload revision, SDK ceiling, and Ozzy dependencies may differ; retain provenance.
pub fn select_with_iggy_reference(directory: &Path, ids: &[String], id: &str) -> Result<Value> {
    select_with_references(directory, ids, &[id.to_owned()], &["iggy"], false)
}

/// Reuse explicitly selected external measurements with their original provenance.
pub fn select_with_external_reference(directory: &Path, ids: &[String], id: &str) -> Result<Value> {
    select_with_references(
        directory,
        ids,
        &[id.to_owned()],
        &["iggy", "redpanda"],
        false,
    )
}

pub fn select_fixed_load_with_external_reference(
    directory: &Path,
    ids: &[String],
    reference_ids: &[String],
) -> Result<Value> {
    select_with_references(directory, ids, reference_ids, &["iggy", "redpanda"], true)
}

pub fn select_fixed_load_with_iggy_reference(
    directory: &Path,
    ids: &[String],
    reference_ids: &[String],
) -> Result<Value> {
    select_with_references(directory, ids, reference_ids, &["iggy"], true)
}

fn comparable_reference(value: &Value) -> Value {
    let mut value = value["compatibility"].clone();
    value.as_object_mut().unwrap().remove("workload");
    // OMQ/fanring revisions belong to Ozzy, not external broker binaries.
    // Keep their original provenance below, but compare actual host settings.
    if let Some(environment) = value["environment"].as_object_mut() {
        environment.remove("dependencies");
    }
    let config = value["configuration"].as_object_mut().unwrap();
    config.remove("request_records");
    // External adapters do not use native APPEND, disk, or reader controls.
    // These settings stay in each selection's recorded controls and captions.
    config.remove("writer_inflight_appends");
    config.remove("disk_aio_depth");
    config.remove("disk_io_backend");
    config.remove("disk_direct_io");
    config.remove("live_readers");
    config.remove("broker_omq_on_shard");
    if config.get("broker_cpu_sets") == Some(&json!([config["broker_cpus"]])) {
        config.remove("broker_cpu_sets");
    }
    if config.get("ramp") == Some(&Value::Null) {
        config.remove("ramp");
    }
    value
}

fn reference_case_key(row: &Value) -> String {
    json!([
        row["case"]["mode"],
        row["case"]["size"],
        row["case"]["pattern"],
        row["case"]["rate"]
    ])
    .to_string()
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

fn select_with_references(
    directory: &Path,
    ids: &[String],
    reference_ids: &[String],
    implementations: &[&str],
    fixed_load: bool,
) -> Result<Value> {
    let mut data = if fixed_load {
        select_fixed_load(directory, ids)?
    } else {
        select(directory, ids, None)?
    };
    include_references(
        directory,
        &mut data,
        reference_ids,
        implementations,
        fixed_load,
    )?;
    Ok(data)
}

/// Attach external comparisons to validated, optionally mode-filtered results.
/// Every selected case still requires reference coverage.
pub fn include_references(
    directory: &Path,
    data: &mut Value,
    reference_ids: &[String],
    implementations: &[&str],
    fixed_load: bool,
) -> Result<()> {
    if implementations.is_empty()
        || implementations
            .iter()
            .any(|implementation| !matches!(*implementation, "iggy" | "redpanda"))
    {
        return Err("select at least one supported external reference implementation".into());
    }
    let reference = if fixed_load {
        select_fixed_load(directory, reference_ids)?
    } else {
        select(directory, reference_ids, None)?
    };
    if comparable_reference(data) != comparable_reference(&reference) {
        return Err("incompatible reference host, compiler, or measurement settings".into());
    }
    let rows = data["summary"].as_array().unwrap();
    let references = reference["summary"].as_array().unwrap();
    let existing = rows
        .iter()
        .chain(data["incomplete"].as_array().into_iter().flatten());
    if !existing.clone().any(|r| r["case"]["impl"] == "ozzy")
        || existing
            .clone()
            .any(|r| implementations.contains(&r["case"]["impl"].as_str().unwrap_or("")))
    {
        return Err("reference selection requires Ozzy and a new external series".into());
    }
    let wanted: BTreeSet<_> = existing
        .clone()
        .filter(|row| row["case"]["impl"] == "ozzy")
        .map(reference_case_key)
        .collect();
    if fixed_load {
        for rate in existing
            .filter(|row| row["case"]["impl"] == "ozzy")
            .map(|row| row["case"]["rate"].to_string())
        {
            if !same_fixed_load_timing(
                &data["fixed_load_windows"][&rate],
                &reference["fixed_load_windows"][&rate],
            ) {
                return Err("reference fixed-load timing differs".into());
            }
        }
    }
    let selected: Vec<_> = references
        .iter()
        .filter(|r| {
            implementations.contains(&r["case"]["impl"].as_str().unwrap_or(""))
                && wanted.contains(&reference_case_key(r))
        })
        .cloned()
        .collect();
    let selected_failures: Vec<_> = if fixed_load {
        reference["incomplete"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| {
                implementations.contains(&r["case"]["impl"].as_str().unwrap_or(""))
                    && wanted.contains(&reference_case_key(r))
            })
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    require_reference_coverage(&wanted, &selected, &selected_failures, implementations)?;
    data["summary"].as_array_mut().unwrap().extend(selected);
    if !selected_failures.is_empty() {
        if data["incomplete"].is_null() {
            data["incomplete"] = json!([]);
        }
        data["incomplete"]
            .as_array_mut()
            .unwrap()
            .extend(selected_failures);
    }
    for implementation in implementations {
        if let Some(value) = reference["versions"].get(*implementation) {
            data["versions"][*implementation] = value.clone();
        }
    }
    for implementation in implementations {
        data["batch_records"][*implementation] =
            reference["batch_records"][*implementation].clone();
        data["references"][*implementation] = json!({
            "run_id":reference_ids[0],"run_ids":reference_ids,
            "compatibility":reference["compatibility"],
            "sources":reference["sources"],"executables":reference["executables"],
            "identical_workload":false
        });
    }
    Ok(())
}

fn same_fixed_load_timing(left: &Value, right: &Value) -> bool {
    ["duration", "warmup"]
        .iter()
        .all(|key| !left[key].is_null() && left[key] == right[key])
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

fn select_inner(
    directory: &Path,
    ids: &[String],
    baseline: Option<&str>,
    fixed_load: bool,
) -> Result<Value> {
    if ids.is_empty() {
        return Err("select at least one completed run".into());
    }
    let mut rows = load_rows(directory)?;
    // Historical ledgers stay append-only. Charts show supported storage codecs.
    rows.retain(|row| matches!(row["case"]["codec"].as_str(), Some("raw" | "lz4")));
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
    if let Some(id) = baseline {
        if selected.iter().any(|r| r["case"]["impl"] == "iggy") {
            return Err("selected runs already contain Iggy".into());
        }
        let case_key = |r: &Value| {
            json!([
                r["case"]["mode"],
                r["case"]["size"],
                r["case"]["pattern"],
                r["case"]["rate"]
            ])
            .to_string()
        };
        let wanted: BTreeSet<_> = selected.iter().map(case_key).collect();
        let found: Vec<_> = rows
            .iter()
            .filter(|r| {
                r["run_id"] == id && r["case"]["impl"] == "iggy" && wanted.contains(&case_key(r))
            })
            .cloned()
            .collect();
        if wanted.is_empty() || found.iter().map(case_key).collect::<BTreeSet<_>>() != wanted {
            return Err("Iggy baseline does not cover selected cases".into());
        }
        selected.extend(found);
    }
    let compare = |row: &Value| -> Result<Value> {
        let mut value = compatibility(row)?;
        if fixed_load {
            let config = value["configuration"].as_object_mut().unwrap();
            config.remove("records_per_second");
            config.remove("duration");
            // Stage windows are checked per rate; ramps may end at different rates.
            config.remove("ramp");
        }
        Ok(value)
    };
    let (failed, measured) = split_failures(&selected, fixed_load)?;
    let windows = fixed_load
        .then(|| fixed_load_windows(&selected))
        .transpose()?;
    let expected = compare(&selected[0])?;
    if selected
        .iter()
        .map(compare)
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|c| *c != expected)
    {
        return Err("incompatible workload, host, compiler, or measurement configuration".into());
    }
    if selected
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
        != 1
    {
        return Err("select one payload pattern and offered load".into());
    }
    same_ozzy_source(&selected)?;
    let mut data = finish_selection(ids, baseline, &selected, &measured, &expected)?;
    if let Some(windows) = windows {
        data["fixed_load_windows"] = windows;
    }
    if !failed.is_empty() {
        data["incomplete"] = json!(failed);
    }
    Ok(data)
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

/// Seconds and exact planned arrivals of one offered rate: a whole fixed-rate
/// run, or one stage of a continuous ramp.
fn stage_window(config: &Value, rate: u64, warmup: f64) -> Result<(f64, u64)> {
    let warmup = (warmup * 1e9) as u64;
    let (plan, start, ns) = if let Some(ramp) = config["ramp"].as_str() {
        let ramp: crate::schedule::Ramp = ramp.parse()?;
        let index = ramp
            .stages()
            .iter()
            .position(|(stage, _)| *stage == rate)
            .ok_or("offered rate is not a ramp stage")?;
        let before: u64 = ramp.stages()[..index].iter().map(|(_, ns)| ns).sum();
        (ramp.plan(warmup)?, warmup + before, ramp.stages()[index].1)
    } else {
        let plan = crate::schedule::Load {
            records_per_second: Some(rate),
            ..Default::default()
        }
        .plan(1)?
        .ok_or("missing schedule")?;
        (plan, warmup, (finite(&config["duration"])? * 1e9) as u64)
    };
    let end = start.checked_add(ns).ok_or("window overflow")?;
    Ok((
        ns as f64 / 1e9,
        (plan.count(end)? - plan.count(start)?) as u64,
    ))
}

fn fixed_load_windows(rows: &[Value]) -> Result<Value> {
    let mut windows = serde_json::Map::new();
    for row in rows {
        let case = &row["case"];
        let rate = case["rate"]
            .as_u64()
            .filter(|r| *r > 0)
            .ok_or("positive offered load required")?;
        let config = &row["configuration"];
        if !matches!(case["impl"].as_str(), Some("ozzy" | "iggy" | "redpanda"))
            || !matches!(
                case["mode"].as_str(),
                Some("buffered" | "durable" | "disk-quorum" | "replicated-persisting")
            )
            || case["size"].as_u64().is_none_or(|size| size == 0)
            || (config["records_per_second"] != rate && config["ramp"].is_null())
            || config["repetitions"] != rows[0]["configuration"]["repetitions"]
        {
            return Err("fixed-load charts require record sizes, supported modes, and matching configured rates".into());
        }
        let warmup = finite(&config["warmup"])?;
        let (duration, expected) = stage_window(config, rate, warmup)?;
        let window =
            json!({"duration":duration,"warmup":warmup,"repetitions":config["repetitions"]});
        if let Some(prior) = windows.insert(rate.to_string(), window.clone())
            && prior != window
        {
            return Err("one offered load has mismatched measurement windows".into());
        }
        // An overloaded ramp stage has a window but no scheduled cohort.
        if row.get("failure").is_some() {
            continue;
        }
        if duration <= 0.0
            || warmup < 0.0
            || expected == 0
            || row["measurements"]["scheduled_samples"] != expected
        {
            return Err("missing or incomplete scheduled arrivals".into());
        }
        for metric in [
            "scheduled_ack_p50_us",
            "scheduled_ack_p99_us",
            "scheduled_delivery_p50_us",
            "scheduled_delivery_p99_us",
        ] {
            if finite(&row["measurements"][metric])? <= 0.0 {
                return Err("missing positive scheduled latency".into());
            }
        }
    }
    Ok(Value::Object(windows))
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
        sources.insert(
            row["run_id"].as_str().ok_or("missing run ID")?.into(),
            row["source"].clone(),
        );
        executables.insert(
            row["run_id"].as_str().unwrap().into(),
            row["executable_sha256"].clone(),
        );
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
        json!({"run_ids":ids,"baseline_run_id":baseline,"compatibility":expected,"sources":sources,"executables":executables,"versions":versions,"batch_records":batch_records(selected)?,"writer_protocols":writer_protocols(selected)?,"payload_compression":payload_compression,"summary":summarize(measured)?}),
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

#[cfg(test)]
mod fixed_load_reference_tests {
    use super::*;

    #[test]
    fn reference_repetitions_may_differ_but_timing_must_match() {
        let measured = json!({"duration":8.0,"warmup":2.0,"repetitions":2});
        assert!(same_fixed_load_timing(
            &measured,
            &json!({"duration":8.0,"warmup":2.0,"repetitions":1})
        ));
        assert!(!same_fixed_load_timing(
            &measured,
            &json!({"duration":10.0,"warmup":2.0,"repetitions":1})
        ));
        assert!(!same_fixed_load_timing(
            &measured,
            &json!({"duration":8.0,"warmup":5.0,"repetitions":1})
        ));
    }
}
