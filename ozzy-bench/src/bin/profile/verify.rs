//! Checks around a profiled run: the replayed worker build before it starts,
//! and reader verification and stage samples after it ends.
use crate::Kind;
use ozzy_bench::automation::{self, Result, source};
use serde_json::Value;

/// The verified worker build this profile replays.
pub(crate) struct Build {
    /// Source identity of the checkout.
    pub(crate) identity: Value,
    pub(crate) executable_sha256: String,
}

impl Build {
    /// The comparison build stamp must match this checkout's worker, and the
    /// case must have used it unless `current_build` opts in.
    pub(crate) fn verify(case_manifest: &Value, current_build: bool) -> Result<Self> {
        let identity = source::identity(&automation::root())?;
        let executable_sha256 = source::sha256(&automation::worker_binary())?;
        verify_build(
            &automation::read_json(&automation::artifacts().join("comparison-build.json"))?,
            &identity,
            &source::worker_identity(&automation::root())?,
            &executable_sha256,
        )?;
        verify_case_build(case_manifest, &executable_sha256, current_build)?;
        Ok(Self {
            identity,
            executable_sha256,
        })
    }
}

fn verify_build(
    saved: &Value,
    identity: &Value,
    worker_inputs: &Value,
    executable: &str,
) -> Result<()> {
    let inputs_match = if saved["worker_inputs"].is_null() {
        saved["source"] == *identity
    } else {
        saved["worker_inputs"] == *worker_inputs
    };
    if !inputs_match || saved["executable_sha256"] != executable {
        return Err("profile build stamp differs; run ozzy_compare --check-only first".into());
    }
    Ok(())
}

fn verify_case_build(manifest: &Value, executable: &str, current_build: bool) -> Result<()> {
    if !current_build && manifest["executable_sha256"] != executable {
        return Err(
            "profile case used a different executable; use --current-build to replay its workload"
                .into(),
        );
    }
    Ok(())
}

/// The run carried the profile label, verified records on the production live
/// reader path, and, for stage profiles, sampled every local role.
pub(crate) fn verify_run(row: &Value, kind: Kind) -> Result<()> {
    let verified = row["profiled"] == true
        && row["native_client_protocol"] == true
        && row["total_verified_records"]
            .as_u64()
            .is_some_and(|n| n != 0)
        && ozzy_bench::dedup::Feeds::attempts_match(row);
    if !verified {
        return Err("profile failed reader verification or instrumentation label".into());
    }
    verify_live_readers(row)?;
    if kind == Kind::Stages {
        verify_stage_profiles(row, &super::local_host()?)?;
    }
    Ok(())
}

fn verify_live_readers(row: &Value) -> Result<()> {
    let live_records: u64 = row["readers"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|reader| reader["topic_readers"].as_array().into_iter().flatten())
        .filter_map(|reader| reader["live_records"].as_u64())
        .sum();
    if row["broker_runtime"] != "production-deployment"
        || row["native_reader_api"] != "decoded-records"
        || row["live_readers"] != true
        || live_records == 0
        || row["reader_transport"]
            != "TCP PUB/SUB live records; PEER subscriptions, replay, gap repair"
    {
        return Err("profile did not use production live readers".into());
    }
    Ok(())
}

fn stage_samples(process: &Value, stage: &str) -> u64 {
    process["usage"]["stages"]
        .as_array()
        .and_then(|stages| stages.iter().find(|entry| entry["stage"] == stage))
        .and_then(|entry| entry["samples"].as_u64())
        .unwrap_or(0)
}

fn verify_stage_profiles(row: &Value, host: &str) -> Result<()> {
    for (role, stage) in [
        ("writers", "request_roundtrip"),
        ("readers", "reader_materialize"),
    ] {
        let processes = row[role].as_array().ok_or("missing profiled clients")?;
        if processes.is_empty()
            || processes
                .iter()
                .any(|process| stage_samples(process, stage) == 0)
        {
            return Err(format!("{role} have no {stage} stage samples").into());
        }
    }
    let brokers = row["brokers"]
        .as_array()
        .ok_or("missing profiled brokers")?;
    if brokers
        .iter()
        .filter(|broker| {
            broker["identity"]["host"]
                .as_str()
                .is_none_or(|name| name == host)
        })
        .any(|broker| stage_samples(broker, "replica_confirmation") > 0)
    {
        Ok(())
    } else {
        Err("local brokers have no replica_confirmation stage samples".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn current_build_stamps_check_worker_inputs_and_binary_independently() {
        let source = json!({"revision": "original"});
        let inputs = json!({"code": "unchanged"});
        let saved = json!({"source":source,"worker_inputs":inputs,"executable_sha256":"binary","workload_sha256":"fixture"});
        assert!(verify_build(&saved, &source, &inputs, "binary").is_ok());
        assert!(verify_build(&saved, &json!({"docs":"changed"}), &inputs, "binary").is_ok());
        assert!(verify_build(&saved, &source, &json!({"code":"changed"}), "binary").is_err());
        assert!(verify_build(&saved, &source, &inputs, "changed-binary").is_err());
        let legacy = json!({"source":source,"executable_sha256":"binary"});
        assert!(verify_build(&legacy, &source, &inputs, "binary").is_ok());
        assert!(verify_build(&legacy, &json!({}), &inputs, "binary").is_err());
    }

    #[test]
    fn changing_profile_binary_requires_explicit_opt_in() {
        let manifest = json!({"executable_sha256":"original"});
        assert!(verify_case_build(&manifest, "original", false).is_ok());
        assert!(verify_case_build(&manifest, "modified", false).is_err());
        assert!(verify_case_build(&manifest, "modified", true).is_ok());
    }

    #[test]
    fn profiles_require_production_readers_on_the_live_publication_path() {
        let row = json!({"broker_runtime":"production-deployment", "native_reader_api":"decoded-records", "live_readers":true,
            "reader_transport":"TCP PUB/SUB live records; PEER subscriptions, replay, gap repair",
            "readers":[{"topic_readers":[{"live_records":2,"replayed_records":0}]}]});
        verify_live_readers(&row).unwrap();
        let mut replay_only = row.clone();
        replay_only["readers"][0]["topic_readers"][0]["live_records"] = json!(0);
        assert!(verify_live_readers(&replay_only).is_err());
        for key in [
            "broker_runtime",
            "native_reader_api",
            "live_readers",
            "reader_transport",
        ] {
            let mut bad = row.clone();
            bad[key] = Value::Null;
            assert!(verify_live_readers(&bad).is_err());
        }
    }

    #[test]
    fn stage_profile_requires_samples_from_each_local_role() {
        let mut row = json!({
            "writers":[{"usage":{"stages":[{"stage":"request_roundtrip","samples":2}]}}],
            "readers":[{"usage":{"stages":[{"stage":"reader_materialize","samples":3}]}}],
            "brokers":[
                {"identity":{"host":"local"},"usage":{"stages":[{"stage":"replica_confirmation","samples":4}]}},
                {"identity":{"host":"remote"},"usage":{}}
            ]
        });
        verify_stage_profiles(&row, "local").unwrap();
        row["readers"][0]["usage"]["stages"][0]["samples"] = json!(0);
        assert!(verify_stage_profiles(&row, "local").is_err());
        row["readers"][0]["usage"]["stages"][0]["samples"] = json!(3);
        row["writers"][0]["usage"]["stages"] = json!([]);
        assert!(verify_stage_profiles(&row, "local").is_err());
        row["writers"][0]["usage"]["stages"] = json!([{"stage":"request_roundtrip","samples":2}]);
        row["brokers"][0]["usage"]["stages"] = json!([]);
        assert!(verify_stage_profiles(&row, "local").is_err());
    }
}
