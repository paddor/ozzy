//! Recorded controls that rows must share to appear in one chart.
use super::{Result, isolation};
use serde_json::{Value, json};

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

pub(super) fn compatibility(row: &Value) -> Result<Value> {
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

pub(super) fn controls(row: &Value) -> Result<Value> {
    if row["raw"]["broker_runtime"] == "production-deployment" {
        crate::automation::validation::production_cpu_placement(
            &row["raw"],
            &row["configuration"],
        )?;
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

/// Controls an external reference must share with the measured rows.
pub(super) fn comparable_reference(value: &Value) -> Value {
    let mut value = value["compatibility"].clone();
    if let Some(controls) = value.as_object_mut() {
        controls.remove("workload");
    }
    // OMQ/fanring revisions belong to Ozzy, not external broker binaries.
    // Keep their original provenance below, but compare actual host settings.
    if let Some(environment) = value["environment"].as_object_mut() {
        environment.remove("dependencies");
    }
    // The selector may differ while the recorded compiler is identical.
    if let Some(build_environment) = value["build_environment"].as_object_mut() {
        build_environment.remove("RUSTUP_TOOLCHAIN");
    }
    let Some(config) = value["configuration"].as_object_mut() else {
        return value;
    };
    config.remove("request_records");
    // External adapters do not use native APPEND, disk, or reader controls.
    // These settings stay in each selection's recorded controls and captions.
    config.remove("writer_inflight_appends");
    config.remove("native_batch_target_bytes");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_toolchain_selector_may_differ_but_compiler_and_flags_must_match() {
        let measured = json!({"compatibility": {
            "compiler": "rustc 1.98.0",
            "build_environment": {"RUSTUP_TOOLCHAIN": "1.98.0", "RUSTFLAGS": "-C target-cpu=native"},
            "configuration": {"broker_cpus": [0, 1]}
        }});
        let mut reference = measured.clone();
        reference["compatibility"]["build_environment"]
            .as_object_mut()
            .unwrap()
            .remove("RUSTUP_TOOLCHAIN");
        assert_eq!(
            comparable_reference(&measured),
            comparable_reference(&reference)
        );
        reference["compatibility"]["compiler"] = json!("rustc 1.97.0");
        assert_ne!(
            comparable_reference(&measured),
            comparable_reference(&reference)
        );
        reference["compatibility"]["compiler"] = measured["compatibility"]["compiler"].clone();
        reference["compatibility"]["build_environment"]["RUSTFLAGS"] = json!("");
        assert_ne!(
            comparable_reference(&measured),
            comparable_reference(&reference)
        );
    }
}
