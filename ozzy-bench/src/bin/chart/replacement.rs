//! Explicit case reruns preserve other measurements and both runs' provenance.
use ozzy_bench::automation::Result;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn executable(data: &Value) -> Result<&str> {
    let hashes: BTreeSet<_> = data["executables"]
        .as_object()
        .ok_or("missing measured executables")?
        .values()
        .map(|value| value.as_str().ok_or("missing measured executable hash"))
        .collect::<std::result::Result<_, _>>()?;
    if hashes.len() != 1 {
        return Err("case replacement requires one measured worker binary".into());
    }
    let hash = *hashes.first().unwrap();
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("case replacement requires a SHA-256 executable hash".into());
    }
    Ok(hash)
}

fn key(row: &Value) -> String {
    let case = &row["case"];
    json!([
        case["impl"],
        case["mode"],
        case["size"],
        case["codec"],
        case["pattern"],
        case["rate"]
    ])
    .to_string()
}

pub(crate) fn apply(data: &mut Value, replacement: &Value) -> Result<()> {
    if executable(data)? != executable(replacement)? {
        return Err("case replacement requires the identical measured worker binary".into());
    }
    for field in ["compatibility", "payload_compression"] {
        if data[field] != replacement[field] {
            return Err(format!("case replacement has different {field}").into());
        }
    }
    if data["batch_records"]["ozzy"] != replacement["batch_records"]["ozzy"]
        || replacement["incomplete"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
    {
        return Err(
            "case replacement requires matching batch limits and completed measurements".into(),
        );
    }
    let rows = replacement["summary"]
        .as_array()
        .ok_or("missing rerun summary")?;
    let existing = data["summary"].as_array().ok_or("missing chart summary")?;
    let keys: BTreeSet<_> = rows.iter().map(key).collect();
    if rows.is_empty() || keys.len() != rows.len() {
        return Err("case replacement requires unique measured cases".into());
    }
    for row in rows {
        if row["case"]["impl"] != "ozzy" || !existing.iter().any(|old| key(old) == key(row)) {
            return Err("case replacement must match an existing Ozzy case".into());
        }
        let mode = row["case"]["mode"].as_str().ok_or("missing rerun mode")?;
        if data["writer_protocols"][mode] != replacement["writer_protocols"][mode] {
            return Err("case replacement has different writer_protocols".into());
        }
        // Payload-cap captions aggregate all selected sizes. A partial rerun can
        // have a smaller maximum; identical binary and controls enforce its cap.
        if let Some(rate) = row["case"]["rate"].as_u64()
            && data["fixed_load_windows"][rate.to_string()]
                != replacement["fixed_load_windows"][rate.to_string()]
        {
            return Err("case replacement has different arrival windows".into());
        }
    }
    merge(data, replacement, &keys)
}

fn merge(data: &mut Value, replacement: &Value, keys: &BTreeSet<String>) -> Result<()> {
    for field in ["sources", "executables"] {
        let extra = replacement[field]
            .as_object()
            .ok_or("missing rerun provenance")?;
        let original = data[field]
            .as_object_mut()
            .ok_or("missing original provenance")?;
        original.extend(extra.clone());
    }
    let ids = replacement["run_ids"]
        .as_array()
        .ok_or("missing rerun IDs")?;
    data["run_ids"]
        .as_array_mut()
        .ok_or("missing original run IDs")?
        .extend(ids.clone());
    let rows = data["summary"].as_array_mut().unwrap();
    rows.retain(|row| !keys.contains(&key(row)));
    rows.extend(replacement["summary"].as_array().unwrap().clone());
    if data["case_replacements"].is_null() {
        data["case_replacements"] = json!([]);
    }
    data["case_replacements"]
        .as_array_mut()
        .ok_or("invalid case replacement provenance")?
        .push(
            json!({"run_ids":ids,"cases":replacement["summary"].as_array().unwrap()
            .iter().map(|row| &row["case"]).collect::<Vec<_>>()}),
        );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(id: &str, sizes: &[u64]) -> Value {
        json!({"run_ids":[id],"sources":{id:{"revision":id}},
            "executables":{id:"a".repeat(64)},"compatibility":{"host":"same"},
            "batch_records":{"ozzy":2048},"summary":sizes.iter().map(|size|
                json!({"case":{"impl":"ozzy","mode":"disk-quorum","size":size,
                    "codec":"raw","pattern":"events"},"repetitions":1,
                    "measurements":{"ack_p99_us":{"median":size}}}))
                .collect::<Vec<_>>()})
    }

    #[test]
    fn partial_rerun_keeps_other_sizes_and_both_sources() {
        let mut data = fixture("full", &[128, 1024, 8192]);
        let mut rerun = fixture("rerun", &[128]);
        data["writer_payload_caps"] = json!({"disk-quorum": 2_097_152});
        rerun["writer_payload_caps"] = json!({"disk-quorum": 262_144});
        rerun["summary"][0]["measurements"]["ack_p99_us"]["median"] = json!(172);
        apply(&mut data, &rerun).unwrap();
        assert_eq!(data["summary"].as_array().unwrap().len(), 3);
        assert_eq!(
            data["summary"][2]["measurements"]["ack_p99_us"]["median"],
            172
        );
        assert_eq!(data["run_ids"], json!(["full", "rerun"]));
        assert_eq!(data["sources"]["full"]["revision"], "full");
        assert_eq!(data["sources"]["rerun"]["revision"], "rerun");
        assert_eq!(data["case_replacements"][0]["run_ids"], json!(["rerun"]));
        assert_eq!(data["writer_payload_caps"]["disk-quorum"], 2_097_152);
    }

    #[test]
    fn replacement_rejects_different_binary_controls_and_unmeasured_cases() {
        for field in [
            "binary",
            "compatibility",
            "size",
            "batch",
            "window",
            "incomplete",
        ] {
            let mut data = fixture("full", &[128, 1024, 8192]);
            let original = data.clone();
            let mut rerun = fixture("rerun", &[1024]);
            match field {
                "binary" => rerun["executables"]["rerun"] = json!("b".repeat(64)),
                "compatibility" => rerun["compatibility"]["host"] = json!("different"),
                "size" => rerun["summary"][0]["case"]["size"] = json!(4096),
                "batch" => rerun["batch_records"]["ozzy"] = json!(1024),
                "window" => {
                    for selection in [&mut data, &mut rerun] {
                        selection["summary"][0]["case"]["rate"] = json!(500_000);
                    }
                    // Match an existing size/rate but change its arrival duration.
                    data["summary"][1]["case"]["rate"] = json!(500_000);
                    rerun["fixed_load_windows"]["500000"] = json!({"duration":10});
                }
                _ => rerun["incomplete"] = json!([{"failure":"backlog limit"}]),
            }
            let before = data.clone();
            assert!(apply(&mut data, &rerun).is_err(), "{field}");
            assert_eq!(data, before);
            if field != "window" {
                assert_eq!(data, original);
            }
        }
    }
}
