//! Explicit failure annotations. These never enter measurement summaries.
use super::{Result, controls, finite};
use serde_json::{Value, json};
use std::{fs, path::Path};

/// Include failed offered-load cases as explicit failures in the selected chart data.
pub fn include_failed_loads(directory: &Path, data: &mut Value, ids: &[String]) -> Result<()> {
    let mut failures = vec![];
    for id in ids {
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err("invalid failed run ID".into());
        }
        let current = super::super::artifacts().join("runs").join(id);
        let run = if current.exists() {
            current
        } else {
            directory.join("runs").join(id)
        };
        let mut manifest: Value = serde_json::from_slice(&fs::read(run.join("manifest.json"))?)?;
        let reason = fs::read_to_string(run.join("FAILED"))?;
        if manifest["run_id"] != *id || !reason.contains("scheduled backlog exceeded: lane=") {
            return Err(
                "only recorded scheduled-backlog failures can annotate fixed-load charts".into(),
            );
        }
        let config = manifest["arguments"].clone();
        let implementation = config["impl"].as_str().ok_or("missing implementation")?;
        if !matches!(implementation, "ozzy" | "iggy" | "redpanda") {
            return Err("failed-load annotations require one implementation".into());
        }
        let one = |key: &str| -> Result<Value> {
            let values = config[key].as_array().ok_or("missing case selection")?;
            if values.len() != 1 {
                return Err("failed-load run must select exactly one case".into());
            }
            Ok(values[0].clone())
        };
        let case = json!({"impl":implementation,"mode":one("modes")?,"codec":one("codecs")?,
            "size":one("sizes")?,"pattern":one("patterns")?,"rate":config["records_per_second"]});
        let rate = case["rate"]
            .as_u64()
            .filter(|r| *r > 0)
            .ok_or("missing offered load")?;
        let summaries = data["summary"].as_array().ok_or("missing summaries")?;
        let repeat = summaries.iter().any(|row| row["case"] == case);
        let matches_series = |row: &&Value| {
            ["impl", "mode", "codec", "size", "pattern"]
                .iter()
                .all(|key| row["case"][key] == case[key])
        };
        if !summaries.iter().any(|row| matches_series(&row))
            || failures
                .iter()
                .chain(data["incomplete"].as_array().into_iter().flatten())
                .any(|row: &Value| row["case"] == case)
        {
            return Err("failed case is uncovered or duplicated".into());
        }
        require_matching_build(data, &manifest)?;
        manifest["configuration"] = config.clone();
        let mut control = controls(&manifest)?;
        let settings = control["configuration"].as_object_mut().unwrap();
        settings.remove("records_per_second");
        settings.remove("duration");
        settings.remove("ramp");
        settings.insert(
            "producer_workers".into(),
            json!(producer_workers(&run, &case)?),
        );
        if control != data["compatibility"] {
            return Err("failed run has incompatible workload or environment".into());
        }
        let repetitions = config["repetitions"]
            .as_u64()
            .ok_or("missing repetitions")?;
        let evidence = failure_evidence(&run, id, &case, repetitions, data)?;
        let duration = finite(&config["duration"])?;
        if duration <= 0.0 {
            return Err("invalid failed measurement window".into());
        }
        let window = json!({"duration":duration,"warmup":config["warmup"],"repetitions":config["repetitions"]});
        let prior = &data["fixed_load_windows"][rate.to_string()];
        if !prior.is_null() && *prior != window {
            return Err("failed-load measurement window differs".into());
        }
        data["fixed_load_windows"][rate.to_string()] = window;
        failures.push(
            json!({"run_id":id,"case":case,"failure":"scheduled backlog limit exceeded",
            "reason":reason,"evidence":evidence,"repeat":repeat}),
        );
    }
    // Keep overloaded ramp stages already selected with the measurements.
    let mut incomplete = data["incomplete"].as_array().cloned().unwrap_or_default();
    incomplete.extend(failures);
    data["incomplete"] = json!(incomplete);
    Ok(())
}

fn require_matching_build(data: &Value, manifest: &Value) -> Result<()> {
    if manifest["executable_sha256"].as_str().is_none() {
        return Err("missing executable fingerprint".into());
    }
    for (field, evidence) in [("sources", "source"), ("executables", "executable_sha256")] {
        if manifest[evidence].is_null()
            || !data[field]
                .as_object()
                .ok_or("missing build provenance")?
                .values()
                .any(|value| *value == manifest[evidence])
        {
            return Err("failed run used a different benchmark build".into());
        }
    }
    Ok(())
}

fn producer_workers(run: &Path, case: &Value) -> Result<u64> {
    let case_dir = run.join(format!(
        "r1-0-{}-{}-{}-{}",
        case["impl"].as_str().ok_or("missing implementation")?,
        case["mode"].as_str().ok_or("missing mode")?,
        case["size"],
        case["codec"].as_str().ok_or("missing codec")?
    ));
    let command: Vec<String> = serde_json::from_slice(&fs::read(case_dir.join("command.json"))?)?;
    let counts = command
        .windows(2)
        .filter(|pair| pair[0] == "--producer-workers")
        .map(|pair| pair[1].parse::<u64>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    match counts.as_slice() {
        [count] if *count > 0 => Ok(*count),
        _ => Err("failed run has no unique positive producer-worker count".into()),
    }
}

fn failure_evidence(
    run: &Path,
    id: &str,
    case: &Value,
    repetitions: u64,
    data: &Value,
) -> Result<std::path::PathBuf> {
    let implementation = case["impl"].as_str().ok_or("missing implementation")?;
    for repetition in 1..=repetitions {
        let case_dir = run.join(format!(
            "r{repetition}-0-{implementation}-{}-{}-{}",
            case["mode"].as_str().ok_or("missing mode")?,
            case["size"],
            case["codec"].as_str().ok_or("missing codec")?
        ));
        let stderr = case_dir.join("stderr");
        if stderr.exists()
            && fs::read_to_string(&stderr)?.contains("scheduled backlog exceeded: lane=")
        {
            if implementation != "ozzy" {
                let identity: Value = serde_json::from_slice(&fs::read(
                    run.join(format!("{implementation}-{repetition}-0-{id}/inspect.json")),
                )?)?;
                let version = identity
                    .get("release")
                    .unwrap_or(&identity["build"]["release"]);
                if version.is_null() || *version != data["versions"][implementation] {
                    return Err("failed run has a different broker version".into());
                }
            }
            return Ok(stderr);
        }
    }
    Err("missing failed benchmark stderr".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_are_annotations_with_matching_build_and_controls() {
        let temp = tempfile::tempdir().unwrap();
        let run = temp.path().join("runs/failed");
        fs::create_dir_all(run.join("r1-0-ozzy-buffered-128-raw")).unwrap();
        let config = json!({"impl":"ozzy","modes":["buffered"],"codecs":["raw"],"sizes":[128],
            "patterns":["events"],"records_per_second":1_000_000,"duration":10,"warmup":5,
            "repetitions":2,"partitions":4,"ramp":null});
        let manifest = json!({"run_id":"failed","arguments":config,"configuration":config,
            "source":{"compiler":"rustc","revision":"same"},"executable_sha256":"same-binary",
            "environment":{"cpu":"cpu"},"workload_sha256":"workload"});
        let mut expected = controls(&manifest).unwrap();
        expected["configuration"]
            .as_object_mut()
            .unwrap()
            .remove("duration");
        expected["configuration"]
            .as_object_mut()
            .unwrap()
            .remove("records_per_second");
        expected["configuration"]
            .as_object_mut()
            .unwrap()
            .remove("ramp");
        expected["configuration"]["producer_workers"] = json!(4);
        fs::write(
            run.join("r1-0-ozzy-buffered-128-raw/command.json"),
            json!(["--producer-workers", "4"]).to_string(),
        )
        .unwrap();
        let data = json!({"compatibility":expected,"sources":{"valid":manifest["source"]},
            "executables":{"valid":"same-binary"},"fixed_load_windows":{},"summary":[{
                "case":{"impl":"ozzy","mode":"buffered","codec":"raw","size":128,"pattern":"events","rate":100_000},
                "measurements":{"scheduled_ack_p99_us":123}}]});
        let reason = "scheduled backlog exceeded: lane=0 admitted=1024 limit=8192";
        let evidence = run.join("r1-0-ozzy-buffered-128-raw/stderr");
        fs::write(run.join("FAILED"), reason).unwrap();
        fs::write(&evidence, reason).unwrap();
        fs::write(run.join("manifest.json"), manifest.to_string()).unwrap();
        let mut annotated = data.clone();
        include_failed_loads(temp.path(), &mut annotated, &["failed".into()]).unwrap();
        assert_eq!(annotated["summary"], data["summary"]);
        assert!(annotated["incomplete"][0].get("measurements").is_none());
        assert_eq!(
            annotated["incomplete"][0]["evidence"],
            evidence.to_str().unwrap()
        );
        let mut repeated = data.clone();
        repeated["summary"][0]["case"]["rate"] = json!(1_000_000);
        let measured = repeated["summary"].clone();
        include_failed_loads(temp.path(), &mut repeated, &["failed".into()]).unwrap();
        assert_eq!(repeated["summary"], measured);
        assert_eq!(repeated["incomplete"][0]["repeat"], true);
        for pointer in [
            "/source/revision",
            "/executable_sha256",
            "/environment/cpu",
            "/arguments/warmup",
        ] {
            let mut changed = manifest.clone();
            *changed.pointer_mut(pointer).unwrap() = json!("different");
            fs::write(run.join("manifest.json"), changed.to_string()).unwrap();
            assert!(
                include_failed_loads(temp.path(), &mut data.clone(), &["failed".into()]).is_err(),
                "accepted {pointer}"
            );
        }
        fs::write(run.join("manifest.json"), manifest.to_string()).unwrap();
        assert!(
            include_failed_loads(
                temp.path(),
                &mut data.clone(),
                &["failed".into(), "failed".into()]
            )
            .is_err()
        );
        fs::write(run.join("FAILED"), "timeout").unwrap();
        assert!(include_failed_loads(temp.path(), &mut data.clone(), &["failed".into()]).is_err());
    }
}
