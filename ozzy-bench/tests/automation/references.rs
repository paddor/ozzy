//! Reference selection across modes, revisions, fixed-load rate windows and ramp stages.

use super::*;

#[test]
fn references_require_the_same_observed_writer_process_count() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "native");
    native["raw"]["producer_workers"] = json!(4);
    save(temp.path(), &native, true);
    let mut external = saved("iggy", "old");
    external["raw"]["producer_workers"] = json!(2);
    save(temp.path(), &external, true);
    assert!(records::select_with_iggy_reference(temp.path(), &["native".into()], "old").is_err());
    external["run_id"] = json!("matched");
    external["raw"]["producer_workers"] = json!(4);
    save(temp.path(), &external, true);
    let data =
        records::select_with_iggy_reference(temp.path(), &["native".into()], "matched").unwrap();
    assert_eq!(
        data["compatibility"]["configuration"]["producer_workers"],
        4
    );
}

#[test]
fn mode_selection_reuses_partial_references_without_relaxing_case_coverage() {
    let temp = tempfile::tempdir().unwrap();
    for mode in ["durable", "disk-quorum"] {
        let mut native = saved("ozzy", "both");
        native["case"]["mode"] = json!(mode);
        save(temp.path(), &native, true);
    }
    let mut external = saved("iggy", "single");
    external["case"]["mode"] = json!("durable");
    external["workload_sha256"] = json!("older-workload");
    save(temp.path(), &external, true);
    assert!(records::select_with_iggy_reference(temp.path(), &["both".into()], "single").is_err());
    let mut data = records::select(temp.path(), &["both".into()], None).unwrap();
    let provenance = data["sources"].clone();
    assert!(records::retain_modes(&mut data, &["unknown".into()]).is_err());
    assert_eq!(data["summary"].as_array().unwrap().len(), 2);
    records::retain_modes(&mut data, &["durable".into()]).unwrap();
    records::include_references(
        temp.path(),
        &mut data,
        &records::References {
            run_ids: &["single".into()],
            implementations: &["iggy"],
            fixed_load: false,
        },
    )
    .unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 2);
    assert!(
        data["summary"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["case"]["mode"] == "durable")
    );
    assert_eq!(data["sources"], provenance);
    assert_eq!(data["references"]["iggy"]["run_id"], "single");
    assert_eq!(
        data["references"]["iggy"]["sources"]["single"],
        external["source"]
    );
    // A second selected size still needs its own measured reference.
    let mut larger = data["summary"][0].clone();
    larger["case"]["size"] = json!(1024);
    data["summary"] = json!([larger]);
    assert!(
        records::include_references(
            temp.path(),
            &mut data,
            &records::References {
                run_ids: &["single".into()],
                implementations: &["iggy"],
                fixed_load: false
            }
        )
        .is_err()
    );
}

#[test]
fn independent_external_references_preserve_each_revision_and_reject_duplicates() {
    for fixed_load in [false, true] {
        independent_references(fixed_load);
    }
}

fn independent_references(fixed_load: bool) {
    let temp = tempfile::tempdir().unwrap();
    let row = |implementation, id| {
        let mut row = saved(implementation, id);
        if fixed_load {
            row["case"]["rate"] = json!(100);
            row["configuration"]["records_per_second"] = json!(100);
            row["measurements"] = json!({
                "scheduled_samples":1000,"scheduled_ack_p50_us":1,
                "scheduled_ack_p99_us":2,"scheduled_delivery_p50_us":1,
                "scheduled_delivery_p99_us":2
            });
            if implementation != "redpanda" {
                row["failure"] = json!("scheduled backlog exceeded");
                row["measurements"] = json!({});
            }
        }
        row
    };
    save(temp.path(), &row("ozzy", "native"), true);
    for implementation in ["iggy", "redpanda"] {
        let mut external = row(implementation, implementation);
        external["workload_sha256"] = json!(format!("{implementation}-workload"));
        external["source"]["revision"] = json!(format!("{implementation}-revision"));
        save(temp.path(), &external, true);
    }
    let mut data = if fixed_load {
        records::select_fixed_load(temp.path(), &["native".into()]).unwrap()
    } else {
        records::select(temp.path(), &["native".into()], None).unwrap()
    };
    let native_sources = data["sources"].clone();
    for implementation in ["iggy", "redpanda"] {
        records::include_references(
            temp.path(),
            &mut data,
            &records::References {
                run_ids: &[implementation.into()],
                implementations: &[implementation],
                fixed_load,
            },
        )
        .unwrap();
        assert_eq!(
            data["references"][implementation]["sources"][implementation]["revision"],
            format!("{implementation}-revision")
        );
    }
    assert_eq!(data["sources"], native_sources);
    assert_eq!(
        data["summary"].as_array().unwrap().len(),
        if fixed_load { 1 } else { 3 }
    );
    if fixed_load {
        assert_eq!(data["incomplete"].as_array().unwrap().len(), 2);
    }
    assert!(
        records::include_references(
            temp.path(),
            &mut data,
            &records::References {
                run_ids: &["iggy".into()],
                implementations: &["iggy"],
                fixed_load
            }
        )
        .is_err()
    );
}

#[test]
fn fixed_load_reference_keeps_explicit_ozzy_only_rate() {
    let temp = tempfile::tempdir().unwrap();
    let row = |implementation, run_id, rate| {
        let mut row = saved(implementation, run_id);
        row["case"]["rate"] = json!(rate);
        row["configuration"]["records_per_second"] = json!(rate);
        row["measurements"] = json!({
            "scheduled_samples":rate * 10,
            "scheduled_ack_p50_us":1,"scheduled_ack_p99_us":2,
            "scheduled_delivery_p50_us":1,"scheduled_delivery_p99_us":2
        });
        row["source"]["revision"] = json!(implementation);
        row
    };
    save(temp.path(), &row("ozzy", "low", 100), true);
    save(temp.path(), &row("ozzy", "high", 1_000_000), true);
    save(temp.path(), &row("iggy", "reference", 100), true);
    let ids = &["low".into(), "high".into()];
    let mut data = records::select_fixed_load(temp.path(), ids).unwrap();
    assert!(
        records::include_references(
            temp.path(),
            &mut data.clone(),
            &records::References {
                run_ids: &["reference".into()],
                implementations: &["iggy"],
                fixed_load: true
            }
        )
        .is_err()
    );
    assert!(
        records::include_references_with_ozzy_only_rates(
            temp.path(),
            &mut data.clone(),
            &records::References {
                run_ids: &["reference".into()],
                implementations: &["iggy"],
                fixed_load: true,
            },
            &[100],
        )
        .is_err()
    );
    records::include_references_with_ozzy_only_rates(
        temp.path(),
        &mut data,
        &records::References {
            run_ids: &["reference".into()],
            implementations: &["iggy"],
            fixed_load: true,
        },
        &[1_000_000],
    )
    .unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 3);
    assert_eq!(data["fixed_load_windows"]["1000000"]["duration"], 10.0);
}

#[test]
fn mode_selection_retains_overloads_even_when_no_numeric_case_survives() {
    let mut data = json!({
        "summary": [{"case":{"mode":"durable"}}],
        "incomplete": [{"case":{"mode":"disk-quorum"},
            "failure":"scheduled backlog exceeded"}],
        "sources":{"both":{"revision":"unchanged"}}
    });
    records::retain_modes(&mut data, &["disk-quorum".into()]).unwrap();
    assert_eq!(data["summary"], json!([]));
    assert_eq!(data["incomplete"].as_array().unwrap().len(), 1);
    assert_eq!(data["sources"]["both"]["revision"], "unchanged");
}

#[test]
fn fixed_load_selection_allows_rate_windows_but_preserves_source_and_workload_gates() {
    let temp = tempfile::tempdir().unwrap();
    let mut ids = Vec::new();
    for (rate, duration) in [(100, 30), (1000, 10), (10000, 10)] {
        let id = rate.to_string();
        let mut row = saved("ozzy", &id);
        row["source"]["revision"] = json!("same-build");
        row["case"]["rate"] = json!(rate);
        row["configuration"]["records_per_second"] = json!(rate);
        row["configuration"]["duration"] = json!(duration);
        row["measurements"]["scheduled_samples"] = json!(rate * duration);
        for key in [
            "scheduled_ack_p50_us",
            "scheduled_ack_p99_us",
            "scheduled_delivery_p50_us",
            "scheduled_delivery_p99_us",
        ] {
            row["measurements"][key] = json!(1000);
        }
        save(temp.path(), &row, true);
        for implementation in ["iggy", "redpanda"] {
            let mut external = row.clone();
            external["case"]["impl"] = json!(implementation);
            external["configuration"]["impl"] = json!(implementation);
            external["source"]["revision"] = json!(format!("{implementation}-build"));
            save(temp.path(), &external, true);
        }
        ids.push(id);
    }
    let data = records::select_fixed_load(temp.path(), &ids).unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 9);
    assert_eq!(data["fixed_load_windows"]["100"]["duration"], 30.0);
    assert!(records::select(temp.path(), &ids, None).is_err());
    let base = records::completed(&temp.path().join("ozzy.jsonl")).unwrap()[1].clone();
    let cluster = tempfile::tempdir().unwrap();
    let mut row = base.clone();
    row["case"]["mode"] = json!("disk-quorum");
    save(cluster.path(), &row, true);
    // Cached runs keep rows of removed modes; selection skips them.
    let mut removed = row.clone();
    removed["case"]["mode"] = json!("replicated-persisting");
    save(cluster.path(), &removed, true);
    let selection =
        records::select_fixed_load(cluster.path(), &[row["run_id"].as_str().unwrap().into()])
            .unwrap();
    assert_eq!(selection["summary"].as_array().unwrap().len(), 1);
    assert_eq!(selection["fixed_load_windows"]["1000"]["duration"], 10.0);
    assert_eq!(
        selection["fixed_load_windows"]["1000"]["warmup"],
        json!(row["configuration"]["warmup"].as_f64().unwrap())
    );
    // Several record sizes share one chart when each rate keeps one window.
    let mut larger = base.clone();
    larger["run_id"] = json!("larger");
    larger["case"]["size"] = json!(1024);
    save(temp.path(), &larger, true);
    let mut sized = ids.clone();
    sized.push("larger".into());
    assert_eq!(
        records::select_fixed_load(temp.path(), &sized).unwrap()["summary"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
    for field in ["warmup", "source", "duration", "samples", "latency", "rate"] {
        let mut bad = base.clone();
        bad["run_id"] = json!(field);
        bad["case"]["codec"] = json!("lz4");
        match field {
            "warmup" => bad["configuration"]["warmup"] = json!(6),
            "source" => bad["source"]["revision"] = json!("different"),
            "duration" => {
                bad["configuration"]["duration"] = json!(20);
                bad["measurements"]["scheduled_samples"] = json!(20000);
            }
            "samples" => bad["measurements"]["scheduled_samples"] = json!(9999),
            "latency" => bad["measurements"]["scheduled_ack_p50_us"] = Value::Null,
            "rate" => bad["case"]["rate"] = Value::Null,
            _ => unreachable!(),
        }
        save(temp.path(), &bad, true);
        let mut selected = ids.clone();
        selected.push(field.into());
        assert!(
            records::select_fixed_load(temp.path(), &selected).is_err(),
            "accepted {field}"
        );
    }
}

#[test]
fn fixed_load_reuses_external_runs_without_replacing_current_ozzy() {
    let temp = tempfile::tempdir().unwrap();
    for (rate, duration) in [(100, 20), (10_000, 5)] {
        let mut native = saved("ozzy", &format!("new-{rate}"));
        native["source"]["revision"] = json!("new-build");
        native["case"]["rate"] = json!(rate);
        native["configuration"]["records_per_second"] = json!(rate);
        native["configuration"]["duration"] = json!(duration);
        native["configuration"]["broker_omq_on_shard"] = json!(true);
        native["measurements"]["scheduled_samples"] = json!(rate * duration);
        for key in [
            "scheduled_ack_p50_us",
            "scheduled_ack_p99_us",
            "scheduled_delivery_p50_us",
            "scheduled_delivery_p99_us",
        ] {
            native["measurements"][key] = json!(1000);
        }
        save(temp.path(), &native, true);
        for implementation in ["ozzy", "iggy", "redpanda"] {
            let mut old = native.clone();
            old["run_id"] = json!(format!("old-{rate}"));
            old["case"]["impl"] = json!(implementation);
            old["configuration"]["impl"] = json!(implementation);
            old["configuration"]
                .as_object_mut()
                .unwrap()
                .remove("broker_omq_on_shard");
            old["source"]["revision"] = json!(format!("old-{implementation}"));
            old["workload_sha256"] = json!(format!("workload-{rate}"));
            old["environment"]["dependencies"] = json!([format!("OMQ-{rate}")]);
            if rate == 10_000 && implementation == "redpanda" {
                old["failure"] = json!("scheduled backlog exceeded");
            }
            save(temp.path(), &old, true);
        }
    }
    let current = ["new-100".into(), "new-10000".into()];
    let reference = ["old-100".into(), "old-10000".into()];
    let data =
        records::select_fixed_load_with_external_reference(temp.path(), &current, &reference)
            .unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 5);
    assert_eq!(data["incomplete"][0]["case"]["impl"], "redpanda");
    assert_eq!(data["references"]["iggy"]["run_ids"], json!(reference));
    for implementation in ["iggy", "redpanda"] {
        let provenance = &data["references"][implementation]["compatibility_by_run"];
        assert_eq!(provenance["old-100"]["workload"], "workload-100");
        assert_eq!(provenance["old-10000"]["workload"], "workload-10000");
        assert_eq!(
            provenance["old-10000"]["environment"]["dependencies"],
            json!(["OMQ-10000"])
        );
    }
    assert_eq!(
        data["compatibility"]["configuration"]["broker_omq_on_shard"],
        true
    );
}

#[test]
fn mixed_reference_revisions_keep_host_and_per_run_workload_checks() {
    for mismatch in ["host", "within-run-workload"] {
        let temp = tempfile::tempdir().unwrap();
        save(temp.path(), &saved("ozzy", "native"), true);
        for size in [128, 1024] {
            let mut row = saved("iggy", "external");
            row["case"]["size"] = json!(size);
            if mismatch == "host" {
                row["environment"]["cpu"] = json!("different CPU");
            } else if size == 1024 {
                row["workload_sha256"] = json!("different workload in same run");
            }
            save(temp.path(), &row, true);
        }
        let error =
            records::select_with_iggy_reference(temp.path(), &["native".into()], "external")
                .unwrap_err();
        assert!(error.to_string().contains("incompatible"), "{error}");
    }
}

#[test]
fn ramp_stages_select_as_rates_and_overloaded_stages_as_annotations() {
    let temp = tempfile::tempdir().unwrap();
    let ramp = "100:20,10000:5,1000000:5";
    for implementation in ["ozzy", "iggy", "redpanda"] {
        let mut base = saved(implementation, "ramp");
        base["configuration"]["ramp"] = json!(ramp);
        base["configuration"]["duration"] = json!(30);
        base["case"]["ramp"] = json!(ramp);
        for (rate, samples) in [(100, 2000), (10000, 50000)] {
            let mut row = base.clone();
            row["case"]["rate"] = json!(rate);
            row["measurements"] = json!({"scheduled_samples":samples});
            for key in [
                "scheduled_ack_p50_us",
                "scheduled_ack_p99_us",
                "scheduled_delivery_p50_us",
                "scheduled_delivery_p99_us",
            ] {
                row["measurements"][key] = json!(1000);
            }
            save(temp.path(), &row, false);
        }
        let mut failed = base.clone();
        failed["case"]["rate"] = json!(1_000_000);
        failed["failure"] = json!("scheduled backlog exceeded");
        failed["measurements"] = json!({});
        save(temp.path(), &failed, true);
    }
    let data = records::select_fixed_load(temp.path(), &["ramp".into()]).unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 6);
    assert_eq!(data["incomplete"].as_array().unwrap().len(), 3);
    assert_eq!(
        data["incomplete"][0]["failure"],
        "scheduled backlog exceeded"
    );
    assert_eq!(data["fixed_load_windows"]["100"]["duration"], 20.0);
    assert_eq!(data["fixed_load_windows"]["1000000"]["duration"], 5.0);
    // Overloaded stages never enter saturation charts.
    assert!(records::select(temp.path(), &["ramp".into()], None).is_err());
}

#[test]
fn fixed_load_omits_successful_repetitions_of_an_unstable_stage() {
    let temp = tempfile::tempdir().unwrap();
    for repetition in 1..=2 {
        for rate in [100, 10_000] {
            let mut row = saved("ozzy", "unstable");
            row["repetition"] = json!(repetition);
            row["configuration"]["repetitions"] = json!(2);
            row["configuration"]["ramp"] = json!("100:20,10000:5");
            row["configuration"]["duration"] = json!(25);
            row["case"]["ramp"] = json!("100:20,10000:5");
            row["case"]["rate"] = json!(rate);
            row["measurements"] = json!({
                "scheduled_samples": if rate == 100 { 2000 } else { 50_000 },
                "scheduled_ack_p50_us": 1000,
                "scheduled_ack_p99_us": 1000,
                "scheduled_delivery_p50_us": 1000,
                "scheduled_delivery_p99_us": 1000,
            });
            if rate == 10_000 && repetition == 1 {
                row["failure"] = json!("scheduled backlog exceeded");
                row["measurements"] = json!({});
            }
            save(temp.path(), &row, false);
        }
    }
    records::append(
        temp.path(),
        "ozzy",
        &json!({"kind":"run-complete","run_id":"unstable"}),
    )
    .unwrap();
    let data = records::select_fixed_load(temp.path(), &["unstable".into()]).unwrap();
    let measured = data["summary"].as_array().unwrap();
    assert_eq!(measured.len(), 1);
    assert_eq!(measured[0]["case"]["rate"], 100);
    assert_eq!(measured[0]["repetitions"], 2);
    assert_eq!(data["incomplete"].as_array().unwrap().len(), 1);
    assert_eq!(data["incomplete"][0]["case"]["rate"], 10_000);
    assert_eq!(
        data["incomplete"][0]["failure"],
        "scheduled backlog exceeded"
    );
}
