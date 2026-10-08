//! Regression coverage for measurement validity, isolation, and cached baselines.
use clap::Parser;
use ozzy_bench::automation::{
    artifact_root, compare, isolation, records, source, supervise, validation, workloads,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, path::Path, process::Command, time::Duration};

// Configuration fixtures describe CPUs independently of the executing host.
const VALIDATION_CPUS: &[usize] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[test]
fn separate_checkouts_cannot_share_benchmark_build_artifacts() {
    use ozzy_bench::automation::build_target;
    let main = build_target(Path::new("/workspace/main/ozzy"));
    let experiment = build_target(Path::new("/workspace/experiment/ozzy"));
    assert_ne!(main, experiment);
    assert!(main.starts_with(artifact_root().join("cargo-target/checkouts")));
    assert_eq!(main, build_target(Path::new("/workspace/main/ozzy")));
}

#[test]
fn launcher_creates_configured_artifacts_and_preserves_cargo_overrides() {
    let directory = tempfile::tempdir().unwrap();
    let checkout = ozzy_bench::automation::root();
    let key = ozzy_bench::automation::build_target(&checkout);
    for explicit_target in [false, true] {
        let artifacts = directory.path().join(if explicit_target {
            "explicit artifact root"
        } else {
            "artifact root"
        });
        let target = if explicit_target {
            directory.path().join("explicit target")
        } else {
            artifacts
                .join("cargo-target/checkouts")
                .join(key.file_name().unwrap())
        };
        let mut command = Command::new("bash");
        command
            .args([
                "--noprofile",
                "--norc",
                "-c",
                "source \"$1\"; printf '%s\\n' \"$OZZY_ARTIFACT_ROOT\" \"$CARGO_TARGET_DIR\" \"$TMPDIR\"",
                "ozzy-path-test",
            ])
            .arg(checkout.join("scripts/ozzy_tools.sh"))
            .env("OZZY_ARTIFACT_ROOT", &artifacts)
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("TMPDIR");
        if explicit_target {
            command.env("CARGO_TARGET_DIR", &target);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        let output = String::from_utf8(output.stdout).unwrap();
        let paths: Vec<_> = output.lines().map(Path::new).collect();
        assert_eq!(
            paths,
            [artifacts.as_path(), target.as_path(), artifacts.as_path()]
        );
        assert!(artifacts.is_dir());
        assert!(target.join("ozzy-tools").is_dir());
    }
}

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
    for mode in ["durable", "replicated-persisting"] {
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
    records::include_references(temp.path(), &mut data, &["single".into()], &["iggy"], false)
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
        records::include_references(temp.path(), &mut data, &["single".into()], &["iggy"], false)
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
            &[implementation.into()],
            &[implementation],
            fixed_load,
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
            &["iggy".into()],
            &["iggy"],
            fixed_load
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
            &["reference".into()],
            &["iggy"],
            true
        )
        .is_err()
    );
    assert!(
        records::include_references_with_ozzy_only_rates(
            temp.path(),
            &mut data.clone(),
            &["reference".into()],
            &["iggy"],
            true,
            &[100],
        )
        .is_err()
    );
    records::include_references_with_ozzy_only_rates(
        temp.path(),
        &mut data,
        &["reference".into()],
        &["iggy"],
        true,
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
        "incomplete": [{"case":{"mode":"replicated-persisting"},
            "failure":"scheduled backlog exceeded"}],
        "sources":{"both":{"revision":"unchanged"}}
    });
    records::retain_modes(&mut data, &["replicated-persisting".into()]).unwrap();
    assert_eq!(data["summary"], json!([]));
    assert_eq!(data["incomplete"].as_array().unwrap().len(), 1);
    assert_eq!(data["sources"]["both"]["revision"], "unchanged");
}

#[test]
fn native_cases_apply_broker_masks_and_keep_clients_in_their_own_pool() {
    let temp = tempfile::tempdir().unwrap();
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--sizes", "128"]);
    for case in args.cases() {
        let command = args
            .case_command(Path::new("bench"), &case, temp.path(), None)
            .unwrap();
        assert_eq!(option(&command, "-c"), "3,4,5");
        let placements = ozzy_bench::placement::Placement::load(Some(Path::new(option(
            &command,
            "--placements",
        ))))
        .unwrap();
        let count = if case["mode"] == "durable" { 1 } else { 3 };
        let expected = args.broker_cpus.brokers(count);
        for (index, placement) in placements.iter().take(count).enumerate() {
            assert_eq!(placement.cpus.as_ref().unwrap(), &expected[index]);
            assert_eq!(
                placement.storage_dir.as_deref(),
                Some(artifact_root().join("ozzy-bench").as_path())
            );
        }
        let brokers: Vec<_> = expected
            .iter()
            .map(|cpus| {
                let execution = json!({"threads":[{"cpus":cpus}]});
                json!({"identity":{"execution":execution}, "usage":{"execution":execution}})
            })
            .collect();
        let mut row = json!({"brokers":brokers});
        validation::production_cpu_placement(&row, &args.configuration()).unwrap();
        row["brokers"][0]["identity"]["execution"]["threads"][0]["cpus"] =
            json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert!(validation::production_cpu_placement(&row, &args.configuration()).is_err());
        row["brokers"][0]["identity"]["execution"]["threads"] = json!([]);
        assert!(validation::production_cpu_placement(&row, &args.configuration()).is_err());
    }
}

fn saved(implementation: &str, id: &str) -> Value {
    json!({"kind":"measurement","run_id":id,"repetition":1,
        "case":{"impl":implementation,"mode":"buffered","size":128,"codec":"raw","pattern":"json","rate":null},
        "measurements":{"confirmed_mib_s":5,"verified_mib_s":5},
        "source":{"compiler":"rustc","revision":id},"workload_sha256":"workload",
        "process_isolation":{"contract":isolation::CONTRACT,"observations":1,"after_shutdown":isolation::empty_proof()},
        "environment":{"cpu":"cpu","memory":"24GiB","filesystem":"ssd"},
        "configuration":{"impl":implementation,"repetitions":1,"duration":10,"warmup":5,"broker_cpus":[0,1]}})
}

#[test]
fn dictionary_worker_has_its_own_isolation_class() {
    let worker = json!({"pid":123,"started":1,"group":99,
        "executable":"ozzy_lz4_dict_bench","arguments":["--worker"]});
    isolation::Guard::new("lz4-dictionary", None)
        .check_processes(99, vec![worker.clone()])
        .unwrap();
    assert!(
        isolation::Guard::new("ozzy", None)
            .check_processes(99, vec![worker.clone()])
            .is_err()
    );
    assert!(
        isolation::Guard::new("lz4-dictionary", None)
            .check_processes(100, vec![worker])
            .is_err()
    );
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
    for mode in ["disk-quorum", "replicated-persisting"] {
        let cluster = tempfile::tempdir().unwrap();
        let mut row = base.clone();
        row["case"]["mode"] = json!(mode);
        save(cluster.path(), &row, true);
        let selection =
            records::select_fixed_load(cluster.path(), &[row["run_id"].as_str().unwrap().into()])
                .unwrap();
        assert_eq!(selection["fixed_load_windows"]["1000"]["duration"], 10.0);
        assert_eq!(
            selection["fixed_load_windows"]["1000"]["warmup"],
            json!(row["configuration"]["warmup"].as_f64().unwrap())
        );
    }
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

fn save(root: &Path, row: &Value, complete: bool) {
    let implementation = row["case"]["impl"].as_str().unwrap();
    records::append(root, implementation, row).unwrap();
    if complete {
        records::append(
            root,
            implementation,
            &json!({"kind":"run-complete","run_id":row["run_id"]}),
        )
        .unwrap();
    }
}
#[test]
fn ledgers_append_and_exclude_failed_interrupted_runs() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("ozzy.jsonl");
    let first = saved("ozzy", "first");
    save(temp.path(), &first, true);
    let original = fs::read(&path).unwrap();
    for id in ["failed", "interrupted"] {
        save(temp.path(), &saved("ozzy", id), id == "failed");
    }
    records::append(
        temp.path(),
        "ozzy",
        &json!({"kind":"run-failed","run_id":"failed"}),
    )
    .unwrap();
    save(temp.path(), &saved("iggy", "baseline"), true);
    assert!(fs::read(&path).unwrap().starts_with(&original));
    assert_eq!(records::completed(&path).unwrap(), vec![first]);
}
#[test]
fn charts_select_supported_codecs_without_rewriting_history() {
    let temp = tempfile::tempdir().unwrap();
    for codec in ["raw", "lz4", "retired"] {
        let mut row = saved("ozzy", "mixed");
        row["case"]["codec"] = json!(codec);
        save(temp.path(), &row, false);
    }
    records::append(
        temp.path(),
        "ozzy",
        &json!({"kind":"run-complete","run_id":"mixed"}),
    )
    .unwrap();
    let path = temp.path().join("ozzy.jsonl");
    let before = fs::read(&path).unwrap();
    let data = records::select(temp.path(), &["mixed".into()], None).unwrap();
    let rows = data["summary"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["case"]["codec"] == "raw"));
    assert!(rows.iter().any(|row| row["case"]["codec"] == "lz4"));
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn baseline_reuse_requires_matching_workload_host_and_configuration() {
    let temp = tempfile::tempdir().unwrap();
    save(temp.path(), &saved("ozzy", "new"), true);
    save(temp.path(), &saved("iggy", "old"), true);
    assert_eq!(
        records::select(temp.path(), &["new".into()], Some("old")).unwrap()["summary"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for (field, value) in [
        ("workload_sha256", json!("different")),
        ("environment", json!({"cpu":"other"})),
        ("configuration", json!({"duration":20})),
    ] {
        let mut bad = saved("iggy", field);
        bad[field] = value;
        save(temp.path(), &bad, true);
        assert!(
            records::select(temp.path(), &["new".into()], Some(field))
                .unwrap_err()
                .to_string()
                .contains("incompatible")
        );
    }
}

#[test]
fn fresh_comparisons_keep_each_implementations_batch_ceiling() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "native");
    native["configuration"]["request_records"] = json!(2048);
    let mut iggy = saved("iggy", "iggy");
    iggy["configuration"]["request_records"] = json!(1024);
    save(temp.path(), &native, true);
    save(temp.path(), &iggy, true);
    let selected = records::select(temp.path(), &["native".into(), "iggy".into()], None).unwrap();
    assert_eq!(selected["batch_records"], json!({"ozzy":2048,"iggy":1024}));
    assert!(selected["references"].is_null());
    let mut changed = native.clone();
    changed["run_id"] = json!("changed");
    changed["case"]["codec"] = json!("lz4");
    changed["configuration"]["request_records"] = json!(1024);
    save(temp.path(), &changed, true);
    assert!(
        records::select(temp.path(), &["native".into(), "changed".into()], None)
            .unwrap_err()
            .to_string()
            .contains("batch ceilings")
    );
}

#[test]
fn chart_selection_keeps_effective_worker_payload_caps_per_mode() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "native");
    row["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    for (mode, size, bytes) in [
        ("durable", 128, 256 * 1024),
        ("durable", 8192, 832 * 1024),
        ("disk-quorum", 128, 128 * 1024),
    ] {
        row["case"]["mode"] = json!(mode);
        row["case"]["size"] = json!(size);
        row["raw"]["writer_batch_payload_bytes_max"] = json!(bytes);
        save(temp.path(), &row, true);
    }
    let selected = records::select(temp.path(), &["native".into()], None).unwrap();
    assert_eq!(
        selected["writer_payload_caps"],
        json!({
            "durable":832 * 1024,"disk-quorum":128 * 1024,
        })
    );
}

#[test]
fn cached_reference_keeps_its_controls_and_requires_matching_measurement_conditions() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "new");
    native["configuration"]["request_records"] = json!(8192);
    native["configuration"]["payload_compression_threshold"] = json!(1024);
    native["configuration"]["writer_inflight_appends"] = json!(1);
    native["configuration"]["native_batch_target_bytes"] = json!(2 * 1024 * 1024);
    native["configuration"]["disk_aio_depth"] = json!(8);
    native["configuration"]["disk_io_backend"] = json!("aio");
    native["configuration"]["disk_direct_io"] = json!(true);
    native["configuration"]["live_readers"] = json!(true);
    let mut reference = saved("iggy", "old");
    reference["configuration"]["request_records"] = json!(1024);
    reference["configuration"]["payload_compression_threshold"] = json!(4096);
    reference["configuration"]["writer_inflight_appends"] = json!(3);
    reference["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    reference["workload_sha256"] = json!("older-workload");
    save(temp.path(), &native, true);
    save(temp.path(), &reference, true);
    assert!(records::select(temp.path(), &["new".into()], Some("old")).is_err());
    let data = records::select_with_iggy_reference(temp.path(), &["new".into()], "old").unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 2);
    assert_eq!(data["batch_records"]["ozzy"], 8192);
    assert_eq!(data["batch_records"]["iggy"], 1024);
    assert_eq!(data["references"]["iggy"]["identical_workload"], false);
    for (field, value) in [
        ("disk_io_backend", json!("aio")),
        ("disk_direct_io", json!(true)),
        ("live_readers", json!(true)),
    ] {
        assert_eq!(data["compatibility"]["configuration"][field], value);
        assert!(
            data["references"]["iggy"]["compatibility"]["configuration"]
                .get(field)
                .is_none()
        );
        let mut mismatched = native.clone();
        mismatched["run_id"] = json!(field);
        mismatched["configuration"][field] = Value::Null;
        save(temp.path(), &mismatched, true);
        assert!(records::select(temp.path(), &["new".into(), field.into()], None).is_err());
    }
    assert_eq!(data["compatibility"]["configuration"]["disk_aio_depth"], 8);
    assert_eq!(
        data["compatibility"]["configuration"]["writer_inflight_appends"],
        1
    );
    assert_eq!(
        data["references"]["iggy"]["compatibility"]["configuration"]["writer_inflight_appends"],
        3
    );
    assert_eq!(
        data["compatibility"]["configuration"]["native_batch_target_bytes"],
        2 * 1024 * 1024
    );
    assert_eq!(
        data["references"]["iggy"]["compatibility"]["configuration"]["native_batch_target_bytes"],
        4 * 1024 * 1024
    );
    let mut different_native = native.clone();
    different_native["run_id"] = json!("different-native-target");
    different_native["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    save(temp.path(), &different_native, true);
    assert!(
        records::select(
            temp.path(),
            &["new".into(), "different-native-target".into()],
            None
        )
        .is_err()
    );
    for (field, value) in [("duration", json!(20)), ("warmup", json!(17))] {
        let mut bad = reference.clone();
        bad["run_id"] = json!(field);
        bad["configuration"][field] = value;
        save(temp.path(), &bad, true);
        assert!(records::select_with_iggy_reference(temp.path(), &["new".into()], field).is_err());
    }
}

#[test]
fn external_references_keep_both_series_and_current_native_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "new");
    native["environment"]["dependencies"] = json!(["new OMQ"]);
    native["configuration"]["broker_cpu_sets"] = json!([[0, 1]]);
    native["configuration"]["ramp"] = Value::Null;
    save(temp.path(), &native, true);
    for implementation in ["ozzy", "iggy", "redpanda"] {
        let mut reference = saved(implementation, "old");
        reference["workload_sha256"] = json!("older-workload");
        reference["environment"]["dependencies"] = json!(["old OMQ"]);
        save(temp.path(), &reference, true);
    }
    let data =
        records::select_with_external_reference(temp.path(), &["new".into()], "old").unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 3);
    assert_eq!(data["references"]["iggy"]["run_id"], "old");
    assert_eq!(data["references"]["redpanda"]["run_id"], "old");
    assert_eq!(
        data["compatibility"]["environment"]["dependencies"],
        json!(["new OMQ"])
    );
    assert_eq!(
        data["references"]["redpanda"]["compatibility"]["environment"]["dependencies"],
        json!(["old OMQ"])
    );
    let mut wrong_host = saved("iggy", "wrong-host");
    wrong_host["environment"]["host"] = json!("another machine");
    save(temp.path(), &wrong_host, true);
    assert!(
        records::select_with_external_reference(temp.path(), &["new".into()], "wrong-host")
            .is_err()
    );
    let missing = saved("iggy", "iggy-only");
    save(temp.path(), &missing, true);
    assert!(
        records::select_with_external_reference(temp.path(), &["new".into()], "iggy-only").is_err()
    );
}

#[test]
fn chart_protocol_uses_observed_traffic_without_rewriting_old_results() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "local-push");
    row["raw"] = json!({"writer_protocol":"peer-appends","writers":[{"lanes":[{
        "protocol_requests":{"data_messages":100,"requests":100,
            "max_records":1,"max_inflight_appends":0}
    }]}]});
    save(temp.path(), &row, true);
    let path = temp.path().join("ozzy.jsonl");
    let before = fs::read(&path).unwrap();
    let selected = records::select(temp.path(), &["local-push".into()], None).unwrap();
    assert_eq!(selected["writer_protocols"]["buffered"], "push-records");
    assert_eq!(fs::read(&path).unwrap(), before);

    row["run_id"] = json!("mixed");
    row["raw"]["writers"][0]["lanes"][0]["protocol_requests"]["requests"] = json!(101);
    save(temp.path(), &row, true);
    assert!(records::select(temp.path(), &["mixed".into()], None).is_err());
}

#[test]
fn chart_selection_preserves_push_append_transport() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "push-appends");
    row["raw"] = json!({"writer_protocol":"push-appends"});
    save(temp.path(), &row, true);
    let selected = records::select(temp.path(), &["push-appends".into()], None).unwrap();
    assert_eq!(selected["writer_protocols"]["buffered"], "push-appends");
}

#[test]
fn dsync_is_default_and_cannot_mix_with_old_sync_chart_cohorts() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "durable"]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["durable_segment_io"], "odsync");
    assert!(compare::Args::try_parse_from(["compare", "--direct-io"]).is_err());
    assert!(compare::Args::try_parse_from(["compare", "--write-padding", "512"]).is_err());
    for case in args.cases() {
        assert!(
            !args
                .command(Path::new("bench"), &case, None)
                .unwrap()
                .iter()
                .any(|arg| arg == "--sync-writes")
        );
    }
    assert!(compare::Args::try_parse_from(["compare", "--sync-writes"]).is_err());

    let temp = tempfile::tempdir().unwrap();
    for (id, mode) in [("explicit", "fdatasync"), ("sync", "odsync")] {
        let mut row = saved("ozzy", id);
        row["configuration"]["durable_segment_io"] = json!(mode);
        save(temp.path(), &row, true);
    }
    assert!(
        records::select(temp.path(), &["explicit".into(), "sync".into()], None)
            .unwrap_err()
            .to_string()
            .contains("incompatible")
    );
}

#[test]
fn tiny_record_queues_keep_full_range_while_append_groups_cap_at_two_k() {
    for records in [8192, 16_384, 65_536] {
        let args = compare::Args::parse_from([
            "compare",
            "--impl",
            "ozzy",
            "--modes",
            "replicated-persisting",
            "--sizes",
            "16",
            "--request-records",
            &records.to_string(),
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        let command = args
            .command(Path::new("bench"), &args.cases()[0], None)
            .unwrap();
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--request-records", &records.to_string()])
        );
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--writer-batch-records", "2048"])
        );
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--writer-inflight-appends", "1"])
        );
        assert!(!command.iter().any(|arg| arg == "--writer-linger-us"));
        assert!(command.iter().any(|arg| arg == "--binary-payload"));
    }
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--sizes",
        "16",
        "--request-records",
        "65537",
    ]);
    assert!(args.validate(VALIDATION_CPUS).is_err());
}
#[test]
fn charts_reject_incomplete_duplicate_and_unaudited_data() {
    let temp = tempfile::tempdir().unwrap();
    for state in [
        "failed",
        "interrupted",
        "missing-repetition",
        "unaudited",
        "resident",
    ] {
        let mut row = saved("ozzy", state);
        match state {
            "missing-repetition" => row["configuration"]["repetitions"] = json!(2),
            "unaudited" => row["process_isolation"] = Value::Null,
            "resident" => {
                row["process_isolation"]["after_shutdown"]["resident_processes"] = json!([123]);
            }
            _ => (),
        }
        save(temp.path(), &row, state != "interrupted");
        if state == "failed" {
            records::append(
                temp.path(),
                "ozzy",
                &json!({"kind":"run-failed","run_id":state}),
            )
            .unwrap();
        }
        assert!(
            records::select(temp.path(), &[state.into()], None).is_err(),
            "{state}"
        );
    }
    save(temp.path(), &saved("ozzy", "first"), true);
    assert!(records::select(temp.path(), &["first".into(), "first".into()], None).is_err());
    let mut second = saved("ozzy", "second");
    second["case"]["mode"] = json!("durable");
    save(temp.path(), &second, true);
    assert!(
        records::select(temp.path(), &["first".into(), "second".into()], None)
            .unwrap_err()
            .to_string()
            .contains("source revisions")
    );
}
fn raw() -> Value {
    let mut row = json!({"system":"single-buffered","native_client_protocol":true,"record_bytes":1024,
        "record_corpus":ozzy_bench::workload::JSON_EVENT_CORPUS,
        "topic_count":1,"partition_count":4,"partitions_per_topic":4,
        "partition_to_application_shard":[0,1,2,0],"partition_hashes":[4,5,6,7],"partition_assignment_hash":"xxh3-64",
        "broker_application_shards":3,"broker_application_threads":3,"broker_supervisor_threads":1,"broker_dispatch_threads":0,
        "disk_owner_threads_per_broker":3,"disk_io_threads_per_broker":9,"segment_write_mode":"buffered",
        "total_verified_records":100,"total_confirmed_records":100,"producer_records_per_second":100,"consumer_records_per_second":100,
        "producer_ack":{"samples":100,"p50_us":1,"p99_us":2},"reader_delivery":{"samples":100,"p50_us":1,"p99_us":2},"reader_drain_seconds":0,
        "warmup_seconds":5,"measurement_seconds":10,"request_records":1024,"reader_records_max":16384,"reader_payload_bytes_max":16_777_216,"writer_batch_records_max":1024,"writer_inflight_appends":1,
        "writers":[{"lanes":(0..4).map(|_|json!({"protocol_requests":{"max_inflight_appends":1}})).collect::<Vec<_>>()}],
        "brokers":[{"local_read_cache":{"journal_records":100},"usage":{"journal_writes":{"encoded_group_bytes":65536},"journal_reads":{"resident_selections":1,"persisted_operation_loads":0}}}]});
    row["broker_omq_mode"] = json!("dedicated-io");
    row["broker_omq_io_threads"] = json!(1);
    for field in [
        "writer_batch_target_bytes",
        "operation_target_bytes",
        "local_write_group_target_bytes",
    ] {
        row[field] = json!(4 * 1024 * 1024);
    }
    row["payload_compression"] = json!("adaptive-lz4");
    row["payload_compression_threshold"] =
        json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD);
    row["writer_protocol"] = json!("peer-appends");
    row["writer_batch_target_bytes"] = json!(832 * 1024);
    row["writer_batch_configuration_applies"] = json!(true);
    row["native_reader_api"] = json!("decoded-records");
    row["max_record_bytes"] = json!(1024 * 1024);
    row
}

fn four_shard_row() -> Value {
    let mut row = raw();
    row["broker_application_shards"] = json!(4);
    row["broker_application_threads"] = json!(4);
    row["disk_owner_threads_per_broker"] = json!(4);
    row["disk_io_threads_per_broker"] = json!(11);
    row["partition_to_application_shard"] = json!([0, 1, 2, 3]);
    row
}

#[test]
fn ram_confirmed_comparisons_require_bounded_complete_persistence_and_resident_reads() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
        "--sizes",
        "1024",
        "--patterns",
        "json",
        "--partitions",
        "4",
    ]);
    let mut row = raw();
    row["system"] = json!("replicated-persisting");
    row["disk_owner_threads_per_broker"] = json!(1);
    row["disk_io_threads_per_broker"] = json!(5);
    row["segment_write_mode"] = json!("buffered");
    row["commit_policy"] = json!("quorum_replicated_persisting");
    row["ack_copies"] = json!(2);
    row["journal_writes"] = json!(3);
    row["reader_records_max"] = json!(1024);
    row["reader_payload_bytes_max"] = json!(1024 * 1024);
    row["background_persistence"] = json!({"max_pending_operations":1024,"max_pending_body_bytes":8 * 1024 * 1024, "compression_workers_per_shard":0,"effective_compression_workers_per_shard":0});
    let mut broker = row["brokers"][0].clone();
    broker["operation"] = json!(100);
    broker["disk_written"] = json!(100);
    broker["disk_durable"] = json!(0);
    broker["persistence_drain_seconds"] = json!(0.01);
    broker["persistence"] = json!({
        "samples":10,"operation_limit":1024,"body_bytes_limit":8 * 1024 * 1024,
        "completion_boundary":"buffered_write",
        "pending_operations_max_observed":4,"accepted_lag_operations_max_observed":4,
        "confirmed_lag_operations_max_observed":3,"pending_body_bytes_max_observed":4096,
    });
    row["brokers"] = json!([broker, broker, broker]);
    let case = &args.cases()[0];
    let config = args.configuration();
    validation::comparison(case, &row, &config).unwrap();
    let mut bounded = config.clone();
    bounded["native_persistence_backlog_bytes"] = json!(8 * 1024 * 1024);
    bounded["native_replication_cache_bytes"] = json!(64 * 1024 * 1024);
    assert!(validation::comparison(case, &row, &bounded).is_err());
    let mut observed = row.clone();
    observed["replication_replay_cache"] = json!({"max_body_bytes":64 * 1024 * 1024});
    validation::comparison(case, &observed, &bounded).unwrap();
    for key in [
        "native_persistence_backlog_bytes",
        "native_replication_cache_bytes",
    ] {
        let mut mismatched = bounded.clone();
        mismatched[key] = json!(256 * 1024 * 1024);
        assert!(validation::comparison(case, &observed, &mismatched).is_err());
    }
    let mut bounded = config.clone();
    bounded["native_write_call_bytes"] = json!(64 * 1024);
    assert!(validation::comparison(case, &row, &bounded).is_err());
    let mut observed = row.clone();
    observed["background_persistence"]["write_call_bytes"] = json!(64 * 1024);
    validation::comparison(case, &observed, &bounded).unwrap();
    assert!(validation::comparison(case, &observed, &config).is_err());
    for (pointer, value) in [
        ("/disk_io_threads_per_broker", json!(3)),
        ("/disk_owner_threads_per_broker", json!(2)),
        (
            "/background_persistence/compression_workers_per_shard",
            json!(1),
        ),
        (
            "/background_persistence/effective_compression_workers_per_shard",
            json!(2),
        ),
        ("/brokers/0/disk_durable", json!(101)),
        ("/brokers/0/disk_written", json!(99)),
        (
            "/brokers/0/persistence/completion_boundary",
            json!("data_sync"),
        ),
        ("/brokers/0/persistence/samples", json!(0)),
        (
            "/brokers/0/persistence/pending_operations_max_observed",
            json!(1025),
        ),
        (
            "/brokers/0/persistence/pending_body_bytes_max_observed",
            json!(8 * 1024 * 1024 + 1),
        ),
        (
            "/brokers/0/usage/journal_reads/persisted_operation_loads",
            json!(1),
        ),
        ("/commit_policy", json!("quorum_durable")),
    ] {
        let mut invalid = row.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert!(
            validation::comparison(case, &invalid, &config).is_err(),
            "{pointer}"
        );
    }
}

#[test]
fn comparison_rejects_legacy_replication_storage_switches() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    let command = args
        .command(Path::new("bench"), &args.cases()[0], None)
        .unwrap();
    for flag in [
        "--persistence-backlog-mib",
        "--replication-cache-mib",
        "--resident-read-mib",
        "--write-call-kib",
    ] {
        assert!(
            compare::Args::try_parse_from(["compare", flag, "64"]).is_err(),
            "{flag}"
        );
        assert!(!command.iter().any(|arg| arg == flag), "{flag}");
    }
    for field in [
        "native_persistence_backlog_bytes",
        "native_replication_cache_bytes",
        "native_resident_read_bytes",
        "native_write_call_bytes",
    ] {
        assert!(args.configuration().get(field).is_none(), "{field}");
    }
}

#[test]
fn shard_count_is_independent_of_cpu_count_and_audited() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "durable",
        "--sizes",
        "1024",
        "--shards",
        "4",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--partitions",
        "4",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    let case = args.cases()[0].clone();
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--app-threads"), "4");
    assert!(
        !command
            .iter()
            .any(|arg| arg == "--disk-owner-threads" || arg == "--disk-workers")
    );
    let config = args.configuration();
    assert_eq!(config["native_shards"], 4);
    let archived = json!({"impl":"ozzy","mode":"buffered","size":1024,"pattern":"json"});
    validation::comparison(&archived, &four_shard_row(), &config).unwrap();
    for field in [
        "broker_application_shards",
        "broker_application_threads",
        "disk_owner_threads_per_broker",
    ] {
        let mut wrong = four_shard_row();
        wrong[field] = json!(3);
        assert!(validation::comparison(&archived, &wrong, &config).is_err());
    }
    for shards in [0, 5] {
        args.shards = Some(shards);
        assert!(args.validate(VALIDATION_CPUS).is_err());
        let mut wrong = config.clone();
        wrong["native_shards"] = json!(shards);
        assert!(validation::comparison(&archived, &four_shard_row(), &wrong).is_err());
    }
    args.shards = Some(4);
    args.implementation = "all".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.implementation = "ozzy".into();
    args.modes = vec!["disk-quorum".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn comparison_rejects_one_persisted_load_and_warmup_or_topology_drift() {
    let args = compare::Args::parse_from(["compare", "--partitions", "4"]);
    let case = json!({"impl":"ozzy","mode":"buffered","size":1024,"pattern":"json"});
    let valid = raw();
    assert_eq!(
        validation::comparison(&case, &valid, &args.configuration()).unwrap()["encoded_bytes_per_payload_byte"],
        json!(0.64)
    );
    for (pointer, value) in [
        (
            "/brokers/0/usage/journal_reads/persisted_operation_loads",
            json!(1),
        ),
        (
            "/brokers/0/usage/journal_reads/resident_selections",
            json!(0),
        ),
        (
            "/brokers/0/usage/journal_writes/encoded_group_bytes",
            json!(0),
        ),
        ("/partition_to_application_shard", json!([3, 3, 3, 2])),
        ("/disk_io_threads_per_broker", json!(4)),
        ("/segment_write_mode", json!("odsync")),
        ("/record_corpus", json!("structured-v1")),
        ("/record_bytes", json!(16)),
        ("/writer_inflight_appends", json!(2)),
        ("/reader_records_max", json!(1)),
        ("/reader_payload_bytes_max", json!(1)),
        (
            "/writers/0/lanes/0/protocol_requests/max_inflight_appends",
            json!(9),
        ),
        (
            "/writers/0/lanes/0/protocol_requests/max_inflight_appends",
            json!(0),
        ),
        (
            "/writers/0/lanes/0/protocol_requests/max_inflight_appends",
            Value::Null,
        ),
        ("/warmup_seconds", json!(0)),
        ("/measurement_seconds", json!(20)),
        ("/total_verified_records", json!(99)),
        ("/producer_records_per_second", json!(0)),
        ("/consumer_records_per_second", Value::Null),
        ("/producer_ack/samples", json!(0)),
        ("/reader_delivery/samples", json!(0)),
    ] {
        let mut bad = valid.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(
            validation::comparison(&case, &bad, &args.configuration()).is_err(),
            "{pointer}"
        );
    }
    for key in ["profiled", "allocation_counted"] {
        let mut bad = valid.clone();
        bad[key] = json!(true);
        assert!(validation::comparison(&case, &bad, &args.configuration()).is_err());
    }
}

#[test]
fn event_corpus_requires_binary_at_16_bytes_and_json_at_larger_sizes() {
    let args = compare::Args::parse_from(["compare", "--partitions", "4"]);
    for size in [16, 128, 1024, 8192] {
        let case = json!({"impl":"ozzy","mode":"buffered","size":size,"pattern":"events"});
        let mut row = raw();
        row["record_bytes"] = json!(size);
        let records = 16384;
        row["reader_records_max"] = json!(records);
        row["reader_payload_bytes_max"] = json!(records * size);
        if size == 16 {
            assert!(validation::comparison(&case, &row, &args.configuration()).is_err());
            row["record_corpus"] = json!(ozzy_bench::workload::BINARY_EVENT_CORPUS);
        }
        validation::comparison(&case, &row, &args.configuration()).unwrap();
        row["record_corpus"] = json!(if size == 16 {
            ozzy_bench::workload::JSON_EVENT_CORPUS
        } else {
            ozzy_bench::workload::BINARY_EVENT_CORPUS
        });
        assert!(validation::comparison(&case, &row, &args.configuration()).is_err());
    }
}

#[test]
fn fixed_load_summaries_preserve_scheduled_latency_and_reject_missing_arrivals() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--sizes",
        "1024",
        "--records-per-second",
        "1000",
        "--partitions",
        "4",
    ]);
    let mut archived = args.cases()[0].clone();
    archived["mode"] = json!("buffered");
    let case = &archived;
    let mut row = raw();
    row["reader_payload_bytes_max"] = json!(16384 * 1024);
    row["total_confirmed_records"] = json!(15000);
    row["total_verified_records"] = json!(15000);
    row["brokers"][0]["local_read_cache"]["journal_records"] = json!(15000);
    row["submitted_records_per_second"] = json!(1000);
    row["scheduled_load"] = json!({
        "records_per_second":1000,"planned_total_records":15000,"planned_measurement_records":10000,
        "producer_ack":{"samples":10000,"p50_us":11,"p99_us":22},
        "reader_delivery":{"samples":10000,"p50_us":33,"p99_us":44},
        "scheduling_lag":{"samples":10000,"p50_us":5,"p99_us":7}
    });
    let metrics = validation::comparison(case, &row, &args.configuration()).unwrap();
    assert_eq!(metrics["scheduled_ack_p99_us"], 22.0);
    assert_eq!(metrics["scheduled_delivery_p50_us"], 33.0);
    assert_eq!(metrics["scheduling_lag_p99_us"], 7.0);
    assert_eq!(metrics["scheduled_samples"], 10000);
    assert_eq!(metrics["ack_p99_us"], 2.0);
    let mut with_p999 = row.clone();
    for field in ["producer_ack", "reader_delivery"] {
        with_p999[field]["p999_us"] = json!(100);
        with_p999[field]["max_us"] = json!(200);
    }
    for field in ["producer_ack", "reader_delivery", "scheduling_lag"] {
        with_p999["scheduled_load"][field]["p999_us"] = json!(100);
        with_p999["scheduled_load"][field]["max_us"] = json!(200);
    }
    let captured = validation::comparison(case, &with_p999, &args.configuration()).unwrap();
    for key in [
        "ack_p999_us",
        "delivery_p999_us",
        "scheduled_ack_p999_us",
        "scheduled_delivery_p999_us",
        "scheduling_lag_p999_us",
    ] {
        assert_eq!(captured[key], 100.0);
    }
    with_p999["scheduled_load"]["producer_ack"]["p999_us"] = json!(1);
    assert!(validation::comparison(case, &with_p999, &args.configuration()).is_err());
    let mut iggy_case = case.clone();
    iggy_case["impl"] = json!("iggy");
    let mut iggy_row = row.clone();
    iggy_row["native_client_protocol"] = json!(false);
    iggy_row["system"] = json!("iggy-buffered");
    iggy_row["records_per_append"] = json!(1024);
    assert_eq!(
        validation::comparison(&iggy_case, &iggy_row, &args.configuration()).unwrap()["scheduled_ack_p99_us"],
        22.0
    );
    for (pointer, value) in [
        ("/scheduled_load/records_per_second", json!(100)),
        ("/scheduled_load/planned_measurement_records", json!(9999)),
        ("/scheduled_load/planned_total_records", json!(14999)),
        ("/scheduled_load/producer_ack/samples", json!(9999)),
        ("/scheduled_load/reader_delivery/samples", json!(9999)),
        ("/scheduled_load/scheduling_lag/samples", json!(9999)),
        ("/scheduled_load/scheduling_lag/p99_us", Value::Null),
        ("/scheduled_load/producer_ack/p99_us", json!(-1)),
    ] {
        let mut bad = row.clone();
        *bad.pointer_mut(pointer).unwrap() = value.clone();
        assert!(
            validation::comparison(case, &bad, &args.configuration()).is_err(),
            "{pointer}"
        );
        let mut bad = iggy_row.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(
            validation::comparison(&iggy_case, &bad, &args.configuration()).is_err(),
            "Iggy {pointer}"
        );
    }
    let saturation = compare::Args::parse_from(["compare", "--impl", "ozzy", "--sizes", "1024"]);
    assert!(
        validation::comparison(&saturation.cases()[0], &row, &saturation.configuration()).is_err()
    );
}
#[test]
fn workload_verifies_shards_and_explicit_controls() {
    let case = json!({"profile":"single-buffered","partitions":4,"app_threads":4});
    let valid = four_shard_row();
    validation::measurements(&case, &valid).unwrap();
    let mut observed = valid.clone();
    observed["logical_stream_count"] = json!(1);
    observed["effective_topic_topology"] = json!({
        "logical_stream_count":1, "topic_count":1,
        "partitions_per_topic":4, "partition_count":4,
    });
    validation::measurements(&case, &observed).unwrap();
    for field in [
        "logical_stream_count",
        "topic_count",
        "partitions_per_topic",
        "partition_count",
    ] {
        let mut mismatched = observed.clone();
        mismatched["effective_topic_topology"][field] = json!(16);
        assert!(
            validation::measurements(&case, &mismatched).is_err(),
            "{field}"
        );
    }
    for (field, value) in [
        ("topic_count", json!(4)),
        ("partition_count", json!(1)),
        ("partitions_per_topic", json!(1)),
        ("broker_application_threads", json!(3)),
        ("broker_supervisor_threads", json!(0)),
        ("broker_dispatch_threads", json!(1)),
        ("partition_assignment_hash", json!("murmur3")),
        ("partition_hashes", json!([2, 3, 4, -1])),
        ("partition_to_application_shard", json!([0, 0, 0, 0])),
        ("native_client_protocol", json!(false)),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        assert!(validation::measurements(&case, &bad).is_err(), "{field}");
    }
    let mut zero = case;
    zero["app_threads"] = json!(0);
    assert!(validation::measurements(&zero, &valid).is_err());
}
fn option<'a>(command: &'a [String], key: &str) -> &'a str {
    &command[command.iter().position(|v| v == key).unwrap() + 1]
}

#[test]
fn comparison_commands_declare_the_same_topic_partitions_for_every_adapter() {
    let compare = compare::Args::parse_from([
        "compare",
        "--impl",
        "all",
        "--sizes",
        "128",
        "--modes",
        "durable",
        "--partitions",
        "2",
    ]);
    compare.validate(VALIDATION_CPUS).unwrap();
    for case in compare.cases() {
        let command = compare
            .command(Path::new("bench"), &case, Some("127.0.0.1:19092"))
            .unwrap();
        assert_eq!(option(&command, "--partitions"), "2", "{}", case["impl"]);
        assert_eq!(option(&command, "--window"), "2", "{}", case["impl"]);
        assert_eq!(
            option(&command, "--producer-workers"),
            "1",
            "{}",
            case["impl"]
        );
    }
    let workloads = workloads::Args::parse_from([
        "workloads",
        "--dry-run",
        "--profiles",
        "single-durable,iggy-durable,redpanda-durable",
        "--connections",
        "2",
        "--workers",
        "1,2",
    ]);
    workloads.validate().unwrap();
    for case in workloads.cases() {
        let command = workloads.command(&case, Path::new("/var/tmp/ozzy-bench"));
        assert_eq!(option(&command, "--partitions"), "2", "{}", case["profile"]);
    }
}

#[test]
fn archived_storage_group_bounds_remain_verified_without_a_live_override() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "durable"]);
    args.validate(VALIDATION_CPUS).unwrap();
    let command = args
        .command(Path::new("bench"), &args.cases()[0], None)
        .unwrap();
    assert!(!command.iter().any(|arg| arg == "--storage-group-records"));
    assert!(compare::Args::try_parse_from(["compare", "--storage-group-records", "1024"]).is_err());

    let mut config = compare::Args::parse_from(["compare", "--impl", "ozzy", "--partitions", "4"])
        .configuration();
    config["storage_group_records"] = json!(1024);
    let case = json!({"impl":"ozzy","mode":"buffered","size":1024,"pattern":"json"});
    let mut row = raw();
    assert!(validation::comparison(&case, &row, &config).is_err());
    row["local_storage_group_records_max"] = json!(1024);
    row["local_storage_group_payload_bytes_max"] = json!(1024 * 1024);
    validation::comparison(&case, &row, &config).unwrap();
    for field in [
        "local_storage_group_records_max",
        "local_storage_group_payload_bytes_max",
    ] {
        let mut bad = row.clone();
        bad[field] = json!(1);
        assert!(validation::comparison(&case, &bad, &config).is_err());
    }
}

#[test]
fn archived_storage_targets_remain_verified_without_live_overrides() {
    let mut config = compare::Args::parse_from(["compare", "--impl", "ozzy", "--partitions", "4"])
        .configuration();
    let case = json!({"impl":"ozzy","mode":"buffered","size":1024,"pattern":"json"});
    for kib in [64, 256, 1024, 4096, 8192] {
        config["native_operation_target_bytes"] = json!(kib * 1024);
        config["native_write_group_target_bytes"] = json!(8 * 1024 * 1024);
        let mut row = raw();
        row["operation_target_bytes"] = json!(kib * 1024);
        row["local_write_group_target_bytes"] = json!(8 * 1024 * 1024);
        row["brokers"][0]["local_read_cache"]["prepared_operations"] = json!(100);
        row["brokers"][0]["local_read_cache"]["prepared_payload_bytes_max"] = json!(kib * 1024);
        validation::comparison(&case, &row, &config).unwrap();
        for observed in [Value::Null, json!(0), json!(kib * 1024 + 1)] {
            let mut wrong = row.clone();
            wrong["brokers"][0]["local_read_cache"]["prepared_payload_bytes_max"] = observed;
            assert!(validation::comparison(&case, &wrong, &config).is_err());
        }
        for field in [
            "writer_batch_target_bytes",
            "operation_target_bytes",
            "local_write_group_target_bytes",
        ] {
            let mut wrong = row.clone();
            wrong[field] = json!(1);
            assert!(validation::comparison(&case, &wrong, &config).is_err());
        }
    }
    for flag in ["--operation-target-kib", "--write-group-target-kib"] {
        assert!(
            compare::Args::try_parse_from(["compare", flag, "64"]).is_err(),
            "{flag}"
        );
    }
}

#[test]
fn append_window_override_is_native_only_and_nonzero() {
    let args = compare::Args::parse_from([
        "compare",
        "--writer-inflight-appends",
        "3",
        "--shard-resident-mib",
        "1024",
        "--writer-batch-target-kib",
        "4096",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["writer_inflight_appends"], 3);
    assert_eq!(args.configuration()["shard_resident_mib"], 1024);
    assert_eq!(
        args.configuration()["native_batch_target_bytes"],
        4 * 1024 * 1024
    );
    for case in args.cases() {
        let command = args
            .command(Path::new("bench"), &case, Some("localhost:1234"))
            .unwrap();
        if case["impl"] == "ozzy" {
            assert_eq!(option(&command, "--writer-inflight-appends"), "3");
            assert_eq!(option(&command, "--shard-resident-mib"), "1024");
            assert_eq!(option(&command, "--writer-batch-target-kib"), "4096");
        } else {
            assert!(!command.iter().any(|arg| arg == "--writer-inflight-appends"));
            assert!(!command.iter().any(|arg| arg == "--shard-resident-mib"));
            assert!(!command.iter().any(|arg| arg == "--writer-batch-target-kib"));
        }
    }
    let mut invalid = args;
    invalid.writer_inflight_appends = 0;
    assert!(invalid.validate(VALIDATION_CPUS).is_err());
    assert!(compare::Args::try_parse_from(["compare", "--writer-batch-target-kib", "0"]).is_err());
    assert!(
        compare::Args::try_parse_from(["compare", "--writer-batch-target-kib", "16385"]).is_err()
    );
}
#[test]
fn offered_load_reaches_every_selected_implementation_without_native_queue_overrides() {
    for implementation in ["all", "ozzy", "iggy", "redpanda"] {
        let args = compare::Args::parse_from([
            "compare",
            "--impl",
            implementation,
            "--records-per-second",
            "1000",
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        for case in args.cases() {
            assert_eq!(case["rate"], 1000);
            let command = args
                .command(Path::new("bench"), &case, Some("localhost:1234"))
                .unwrap();
            assert_eq!(option(&command, "--records-per-second"), "1000");
            if case["impl"] == "iggy" {
                assert!(!command.iter().any(|a| a == "--writer-inflight-appends"));
            }
        }
        let mut bad = args;
        bad.records_per_second = Some(0);
        assert!(bad.validate(VALIDATION_CPUS).is_err());
    }
}

#[test]
fn mixed_runs_apply_one_request_ceiling_to_every_implementation() {
    for requested in [512, 2048, 8192] {
        let args = compare::Args::parse_from([
            "compare",
            "--request-records",
            &requested.to_string(),
            "--modes",
            "replicated-persisting",
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        for case in args.cases() {
            let expected = requested;
            let command = args
                .command(Path::new("worker"), &case, Some("localhost:1234"))
                .unwrap();
            assert_eq!(option(&command, "--request-records"), expected.to_string());
            assert_eq!(
                option(&command, "--writer-batch-records"),
                expected
                    .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS as u64)
                    .to_string()
            );
            assert_eq!(args.case_configuration(&case)["request_records"], expected);
            assert!(!command.iter().any(|arg| arg == "--payload-lz4"));
            assert_eq!(
                args.case_configuration(&case)["payload_compression"],
                if case["impl"] == "ozzy" {
                    "adaptive-lz4"
                } else {
                    "none"
                }
            );
        }
    }
}

#[test]
fn all_and_native_only_preserve_workload_and_ssd_cpu_budgets() {
    let all = compare::Args::parse_from(["compare"]);
    let native = compare::Args::parse_from(["compare", "--impl", "ozzy"]);
    assert!(native.cases().iter().all(|case| case["mode"] != "buffered"));
    assert!(
        all.cases()
            .iter()
            .any(|case| case["impl"] != "ozzy" && case["mode"] == "durable")
    );
    let buffered = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "buffered"]);
    assert_eq!(buffered.cases().len(), 0);
    assert!(buffered.validate(VALIDATION_CPUS).is_err());
    assert!(
        buffered
            .command(
                Path::new("bench"),
                &json!({"impl":"ozzy","mode":"buffered"}),
                None
            )
            .is_err()
    );
    assert_eq!(
        native.cases(),
        all.cases()
            .into_iter()
            .filter(|c| c["impl"] == "ozzy")
            .collect::<Vec<_>>()
    );
    assert_eq!(native.sizes, [128, 1024, 8192]);
    assert_eq!(native.partitions, 8);
    assert_eq!(native.repetitions, 1);
    assert_eq!((native.warmup, native.duration), (5.0, 10.0));
    assert_eq!(native.patterns, ["events"]);
    assert_eq!(native.configuration()["codecs"], json!(["raw"]));
    assert!(compare::Args::try_parse_from(["compare", "--codecs", "raw"]).is_err());
    for case in all.cases() {
        let cmd = all
            .command(Path::new("bench"), &case, Some("localhost:1234"))
            .unwrap();
        assert_eq!(
            cmd.iter().any(|v| v == "--binary-payload"),
            case["size"] == 16
        );
        assert_eq!(
            cmd.iter().any(|v| v == "--json-payload"),
            case["size"] != 16
        );
        let batch = all.request_records.to_string();
        assert_eq!(option(&cmd, "--request-records"), batch);
        assert_eq!(option(&cmd, "--writer-batch-records"), batch);
        assert_eq!(option(&cmd, "--reader-records"), "16384");
        assert_eq!(option(&cmd, "--reader-payload-mib"), "128");
        assert_eq!(option(&cmd, "--partitions"), "8");
        assert_eq!(option(&cmd, "--window"), "8");
        assert_eq!(option(&cmd, "--producer-workers"), "4");
        assert_eq!(option(&cmd, "--reader-workers"), "8");
        assert_eq!(
            option(&cmd, "--storage-dir"),
            artifact_root().join("ozzy-bench").to_str().unwrap()
        );
        if case["impl"] == "ozzy" {
            assert_eq!(option(&cmd, "--writer-inflight-appends"), "1");
            assert_eq!(option(&cmd, "-c"), "3,4,5");
            for flag in [
                "--disk-owner-threads",
                "--disk-workers",
                "--history-mib",
                "--history-operations",
                "--segment-decoded-mib",
                "--codec",
                "--writer-linger-us",
            ] {
                assert!(!cmd.iter().any(|arg| arg == flag), "{flag}");
            }
        }
    }
    let cluster = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "disk-quorum,replicated-persisting",
    ]);
    for case in cluster.cases() {
        let cmd = cluster.command(Path::new("bench"), &case, None).unwrap();
        assert!(!cmd.iter().any(|arg| arg == "--history-operations"));
    }
}

#[test]
fn redpanda_runs_local_and_group_cases_through_its_own_external_adapter() {
    let mut args = compare::Args::parse_from(["compare", "--impl", "redpanda"]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.cases().len(), 6);
    args.modes = vec!["disk-quorum".into(), "replicated-persisting".into()];
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        assert_eq!(case["impl"], "redpanda");
        let command = args
            .command(Path::new("bench"), &case, Some("127.0.0.1:9092"))
            .unwrap();
        assert_eq!(option(&command, "--external-system"), "redpanda");
        assert_eq!(
            option(&command, "--external-policy"),
            case["mode"].as_str().unwrap()
        );
        assert!(!command.iter().any(|arg| arg == "--system"));
        assert!(!command.iter().any(|arg| arg == "--disk-workers"));
    }
    args.implementation = "all".into();
    assert!(args.cases().iter().any(|case| case["impl"] == "redpanda"));
}

#[test]
fn redpanda_results_require_the_requested_confirmation_boundary() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "redpanda",
        "--sizes",
        "1024",
        "--modes",
        "buffered",
        "--partitions",
        "4",
    ]);
    let case = &args.cases()[0];
    let mut row = raw();
    row["system"] = json!("redpanda-buffered");
    row["native_client_protocol"] = json!(false);
    row["write_caching"] = json!(true);
    row["reader_records_max"] = json!(1);
    row["reader_payload_bytes_max"] = json!(1024);
    row["records_per_append"] = json!(1);
    row["client_sdk"] = json!({"acks":"all","unconfirmed_records_per_writer":1024});
    validation::comparison(case, &row, &args.configuration()).unwrap();
    for (pointer, value) in [
        ("/write_caching", json!(false)),
        ("/client_sdk/acks", json!("1")),
        ("/client_sdk/unconfirmed_records_per_writer", json!(2048)),
    ] {
        let mut bad = row.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(validation::comparison(case, &bad, &args.configuration()).is_err());
    }
}
#[test]
fn native_controls_never_leak_into_external_or_cluster_commands() {
    let args = workloads::Args::parse_from([
        "workloads",
        "--dry-run",
        "--profiles",
        "single-durable,disk-quorum,iggy-buffered",
        "--workers",
        "1",
        "--writer-batch-records",
        "32",
        "--writer-inflight-appends",
        "3",
    ]);
    args.validate().unwrap();
    for case in args.cases() {
        let cmd = args.command(&case, Path::new("/var/tmp/ozzy-bench"));
        assert_eq!(option(&cmd, "--request-records"), "1024");
        assert_eq!(option(&cmd, "--reader-workers"), "4");
        if case["profile"] == "single-durable" {
            assert_eq!(option(&cmd, "--writer-batch-records"), "32");
        }
        assert!(!cmd.iter().any(|v| matches!(
            v.as_str(),
            "--history-mib"
                | "--history-operations"
                | "--disk-workers"
                | "--disk-owner-threads"
                | "--read-depth"
                | "--storage-group-records"
                | "--packing-block-kib"
                | "--storage-group-kib"
                | "--writer-linger-us"
                | "--io-threads"
        )));
        if case["profile"] == "iggy-buffered" {
            assert_eq!(option(&cmd, "--external-policy"), "buffered");
            assert!(!cmd.iter().any(|v| v == "--system"));
            assert!(!cmd.iter().any(|v| v == "--writer-inflight-appends"
                || v == "--writer-batch-records"
                || v == "--app-threads"));
        } else {
            assert_eq!(option(&cmd, "--writer-inflight-appends"), "3");
        }
    }
    for (flag, value) in [
        ("--app-threads", "5"),
        ("--reader-workers", "5"),
        ("--writer-batch-records", "65537"),
        ("--writer-inflight-appends", "0"),
    ] {
        let bad = workloads::Args::parse_from(["workloads", flag, value]);
        assert!(bad.validate().is_err(), "{flag}");
    }
    assert!(
        workloads::Args::parse_from(["workloads", "--profiles", "single-durable,iggy-buffered"])
            .validate()
            .is_err()
    );
}
fn process(name: &str, pid: u32, group: u32, arguments: Value) -> Value {
    let mut row = json!({"pid":pid,"executable":name,"state":"S","group":group,"started":1});
    row["arguments"] = arguments;
    row
}

#[test]
fn workloads_only_offer_live_native_profiles_and_controls() {
    let args = workloads::Args::parse_from(["workloads"]);
    args.validate().unwrap();
    assert_eq!(args.profiles, ["single-durable", "replicated-persisting"]);
    for profile in ["single-volatile", "single-buffered"] {
        let args = workloads::Args::parse_from(["workloads", "--profiles", profile]);
        assert!(args.validate().is_err(), "{profile}");
    }
    for flag in [
        "--history-mib",
        "--history-operations",
        "--disk-workers",
        "--disk-owner-threads",
        "--read-depth",
        "--storage-group-records",
        "--packing-block-kib",
        "--storage-group-kib",
        "--writer-linger-us",
        "--io-threads",
    ] {
        assert!(
            workloads::Args::try_parse_from(["workloads", flag, "1"]).is_err(),
            "{flag}"
        );
    }
}
#[test]
fn guard_rejects_overlap_compilers_and_wrong_process_groups() {
    let server = process("iggy-server", 100, 100, json!([]));
    let client = process(
        "ozzy_timed_bench",
        201,
        200,
        json!([
            "bench",
            "--producer-worker",
            "0",
            "--external-system",
            "iggy"
        ]),
    );
    let mut guard = isolation::Guard::new("iggy", Some(100));
    guard
        .check_processes(200, vec![server.clone(), client.clone()])
        .unwrap();
    assert_eq!(guard.report().unwrap()["observations"], 1);
    let mut broker = client;
    broker["arguments"] = json!(["bench", "--worker-index", "0"]);
    assert!(
        guard
            .check_processes(200, vec![server.clone(), broker])
            .is_err()
    );
    assert!(guard.check_processes(200, vec![]).is_err());
    for name in [
        "iggy-server",
        "java",
        "redpanda",
        "rustc",
        "cargo",
        "perf",
        "ozzy_timed_bench",
    ] {
        assert!(
            isolation::Guard::new("ozzy", None)
                .check_processes(200, vec![process(name, 100, 100, json!([]))])
                .is_err(),
            "{name}"
        );
    }
    let mut profiled = isolation::Guard::new("ozzy", None)
        .with_profiling(true)
        .unwrap();
    profiled
        .check_processes(200, vec![process("perf", 201, 200, json!([]))])
        .unwrap();
    assert!(
        profiled
            .check_processes(200, vec![process("perf", 201, 201, json!([]))])
            .is_err()
    );
    for (implementation, name) in [
        ("iggy", "iggy-server"),
        ("kafka", "java"),
        ("redpanda", "redpanda"),
    ] {
        isolation::Guard::new(implementation, Some(100))
            .check_processes(200, vec![process(name, 100, 100, json!([]))])
            .unwrap();
    }
}
#[test]
fn stopped_broker_is_still_discovered() {
    let temp = tempfile::tempdir().unwrap();
    let exe = temp.path().join("iggy-server");
    fs::copy("/bin/sleep", &exe).unwrap();
    // Concurrent process tests can briefly inherit the copy's writable FD
    // between fork and exec. Linux returns ETXTBSY until that FD closes.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut child = loop {
        match Command::new(&exe).arg("30").spawn() {
            Ok(child) => break child,
            Err(error)
                if error.raw_os_error() == Some(26) && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("start stopped-broker fixture: {error}"),
        }
    };
    let pid = child.id();
    assert!(
        Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let result = || {
        for _ in 0..100 {
            if let Some(row) = isolation::processes()
                .unwrap()
                .into_iter()
                .find(|p| p["pid"] == pid && p["state"] == "T")
            {
                assert!(
                    isolation::Guard::new("ozzy", None)
                        .check_processes(200, vec![row])
                        .is_err()
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("stopped server missing from process audit");
    };
    let result = std::panic::catch_unwind(result);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(result.is_ok());
}
#[tokio::test(flavor = "current_thread")]
async fn supervision_preserves_success_and_aborts_diagnostics_or_hangs() {
    let temp = tempfile::tempdir().unwrap();
    let command = vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf '{\"valid\":true}\\n'".into(),
    ];
    assert_eq!(
        supervise::execute(
            &command,
            temp.path(),
            Duration::from_secs(2),
            &BTreeMap::new(),
            None,
            None
        )
        .await
        .unwrap()["valid"],
        true
    );
    assert!(
        fs::read_to_string(temp.path().join("stdout"))
            .unwrap()
            .contains("valid")
    );
    for script in [
        "printf 'WARNING: broken\\n'; sleep 30",
        "printf 'ERROR: broken\\n'; sleep 30",
        "printf 'diagnostic' >&2; sleep 30",
        "exit 7",
        "sleep 30",
    ] {
        let cmd = vec!["/bin/sh".into(), "-c".into(), script.into()];
        let start = std::time::Instant::now();
        assert!(
            supervise::execute(
                &cmd,
                temp.path(),
                Duration::from_millis(200),
                &BTreeMap::new(),
                None,
                None
            )
            .await
            .is_err()
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
#[test]
fn source_hash_detects_dirty_and_untracked_contents_not_just_git_status() {
    let temp = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        assert!(
            Command::new("git")
                .args(args)
                .current_dir(temp.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    };
    git(&["init", "-q"]);
    let tracked = temp.path().join("lib.rs");
    fs::write(&tracked, "original").unwrap();
    git(&["add", "lib.rs"]);
    git(&[
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "-qm",
        "fixture",
    ]);
    fs::write(&tracked, "dirty").unwrap();
    let untracked = temp.path().join("new.rs");
    fs::write(&untracked, "first").unwrap();
    let before = source::checkout(temp.path()).unwrap();
    fs::write(&untracked, "second").unwrap();
    let after = source::checkout(temp.path()).unwrap();
    assert_eq!(before["status"], after["status"]);
    assert_ne!(before["content_sha256"], after["content_sha256"]);
    fs::write(tracked, "changed again").unwrap();
    assert_ne!(
        after["content_sha256"],
        source::checkout(temp.path()).unwrap()["content_sha256"]
    );
    let worker = source::worker_checkout(temp.path()).unwrap();
    for name in [
        "doc/charts/single/buffered.svg",
        "ozzy-bench/src/automation/server/redpanda.rs",
        "ozzy-bench/src/bin/chart/fixed.rs",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "fixture or rendering changed").unwrap();
        assert_eq!(source::worker_checkout(temp.path()).unwrap(), worker);
    }
    for name in [
        "ozzy-bench/src/bin/timed_bench/timed/external/kafka.rs",
        "ozzy-bench/src/automation/mod.rs",
        "ozzy-runtime/src/lib.rs",
        "Cargo.lock",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "measured code changed").unwrap();
        assert_ne!(source::worker_checkout(temp.path()).unwrap(), worker);
        fs::remove_file(path).unwrap();
    }
}
#[test]
fn workload_hash_ignores_native_edits_but_detects_corpus_and_lock_changes() {
    let temp = tempfile::tempdir().unwrap();
    for name in [
        "Cargo.lock",
        "ozzy-bench/Cargo.toml",
        "ozzy-bench/src/bin/timed_bench/corpus.rs",
        "ozzy-runtime/src/lib.rs",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "original").unwrap();
    }
    let original = source::workload_fingerprint(temp.path()).unwrap();
    fs::write(
        temp.path().join("ozzy-runtime/src/lib.rs"),
        "native changed",
    )
    .unwrap();
    assert_eq!(source::workload_fingerprint(temp.path()).unwrap(), original);
    for name in ["Cargo.lock", "ozzy-bench/src/bin/timed_bench/corpus.rs"] {
        fs::write(temp.path().join(name), "changed").unwrap();
        assert_ne!(source::workload_fingerprint(temp.path()).unwrap(), original);
        fs::write(temp.path().join(name), "original").unwrap();
    }
}

#[test]
fn cluster_guard_requires_exactly_all_three_external_brokers() {
    let brokers = [100, 101, 102].map(|pid| process("iggy-server", pid, pid, json!([])));
    let mut guard = isolation::Guard::new("iggy", None).with_servers(&[100, 101, 102]);
    guard.check_processes(200, brokers.to_vec()).unwrap();
    assert!(guard.check_processes(200, brokers[..2].to_vec()).is_err());
    let mut extra = brokers.to_vec();
    extra.push(process("iggy-server", 103, 103, json!([])));
    assert!(guard.check_processes(200, extra).is_err());
    let mut mixed = brokers.to_vec();
    mixed.push(process(
        "ozzy_timed_bench",
        201,
        200,
        json!(["--worker-index", "0"]),
    ));
    assert!(guard.check_processes(200, mixed).is_err());
}

#[test]
fn iggy_cluster_cases_use_explicit_external_confirmation_modes() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "iggy",
        "--modes",
        "disk-quorum,replicated-persisting",
        "--sizes",
        "1024",
    ]);
    assert_eq!(args.cases().len(), 2);
    for case in args.cases() {
        let command = args
            .command(std::path::Path::new("bench"), &case, Some("127.0.0.1:8090"))
            .unwrap();
        assert!(
            command
                .windows(2)
                .any(|w| w == ["--external-system", "iggy"])
        );
        assert!(
            command
                .windows(2)
                .any(|w| w[0] == "--external-policy" && w[1] == case["mode"])
        );
    }
}

#[test]
fn adaptive_payload_compression_is_one_series_and_rejects_mixed_settings() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
    ]);
    let config = args.configuration();
    assert_eq!(config["payload_compression"], "adaptive-lz4");
    assert_eq!(
        config["payload_compression_threshold"],
        ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD
    );
    assert_eq!(args.cases().len(), 3);
    assert!(args.cases().iter().all(|case| case["codec"] == "raw"));
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "encoded");
    row["configuration"]["payload_compression"] = json!("adaptive-lz4");
    row["configuration"]["payload_compression_threshold"] =
        json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD);
    save(temp.path(), &row, true);
    let data = records::select(temp.path(), &["encoded".into()], None).unwrap();
    assert_eq!(data["summary"][0]["case"]["codec"], "raw");
    assert_eq!(data["payload_compression"], "adaptive-lz4");
    let stored = records::completed(&temp.path().join("ozzy.jsonl")).unwrap();
    assert_eq!(stored[0]["case"]["codec"], "raw");
    let mut plain = row.clone();
    plain["run_id"] = json!("plain");
    plain["case"]["codec"] = json!("lz4");
    plain["configuration"]["payload_compression"] = json!("none");
    save(temp.path(), &plain, true);
    let error =
        records::select(temp.path(), &["encoded".into(), "plain".into()], None).unwrap_err();
    assert!(error.to_string().contains("payload compression settings"));
    let mut threshold = row.clone();
    threshold["run_id"] = json!("different-threshold");
    threshold["case"]["codec"] = json!("lz4");
    threshold["configuration"]["payload_compression_threshold"] = json!(16384);
    save(temp.path(), &threshold, true);
    let error = records::select(
        temp.path(),
        &["encoded".into(), "different-threshold".into()],
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("payload compression settings"));
}

#[test]
fn saved_local_placements_do_not_waive_remote_audits() {
    let temp = tempfile::tempdir().unwrap();
    for (id, remote, label, accepted) in [
        ("local", false, "all-local", true),
        ("unlabeled", false, "", false),
        ("remote", true, "all-local", false),
    ] {
        let mut row = saved("ozzy", id);
        row["configuration"]["deployment"] = json!([
            {"bind":"127.0.0.1"}, {"bind":"127.0.0.1"}, {"bind":"127.0.0.1"}
        ]);
        row["configuration"]["deployment_kind"] = json!(label);
        if remote {
            row["configuration"]["deployment"][2]["ssh"] = json!("remote");
        }
        save(temp.path(), &row, true);
        assert_eq!(
            records::select(temp.path(), &[id.into()], None).is_ok(),
            accepted
        );
    }
}

#[test]
fn all_local_comparison_checks_every_broker_cpu_and_labels_topology() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("placements.json");
    let mut rows = json!([
        {"bind":"127.0.0.1","storage_dir":"/var/tmp","cpus":[0]},
        {"bind":"127.0.0.1","storage_dir":"/mnt/bench/tmp","cpus":[1]},
        {"bind":"127.0.0.1","storage_dir":"/var/tmp","cpus":[2]}
    ]);
    fs::write(&path, rows.to_string()).unwrap();
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--placements",
        path.to_str().unwrap(),
        "--control-bind",
        "tcp://127.0.0.1:0",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert!(args.validate(&[0, 1, 2, 3]).is_err());
    assert_eq!(args.configuration()["deployment_kind"], "all-local");
    let isolated = args.configuration();
    for row in rows.as_array_mut().unwrap() {
        row["cpus"] = json!([0, 1, 2]);
    }
    fs::write(&path, rows.to_string()).unwrap();
    args.validate(VALIDATION_CPUS).unwrap();
    assert_ne!(args.configuration()["deployment"], isolated["deployment"]);
    for bad in [json!([0, 1]), json!([1, 2, 3]), json!([0, 1, 2, 6])] {
        rows[2]["cpus"] = bad;
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
        row["cpus"] = json!([i]);
    }
    for bad in [0, 1, 3, 4, 5] {
        rows[2]["cpus"] = json!([bad]);
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    if std::thread::available_parallelism().is_ok_and(|cores| cores.get() >= 9) {
        // Check sibling CPU masks only when the VM exposes those CPU IDs.
        let broker_cpus = std::mem::replace(
            &mut args.broker_cpus,
            ozzy_bench::automation::cpus::BrokerCpus::parse("0,1,2,6,7,8").unwrap(),
        );
        for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
            row["cpus"] = json!([i, i + 6]);
        }
        fs::write(&path, rows.to_string()).unwrap();
        args.validate(VALIDATION_CPUS).unwrap();
        for bad in [json!([2, 7]), json!([2, 9]), json!([])] {
            rows[2]["cpus"] = bad;
            fs::write(&path, rows.to_string()).unwrap();
            assert!(args.validate(VALIDATION_CPUS).is_err());
        }
        args.implementation = "iggy".into();
        rows[2]["cpus"] = json!([2, 8]);
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
        args.implementation = "ozzy".into();
        args.broker_cpus = broker_cpus;
    }
    for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
        row["cpus"] = json!([i]);
    }
    rows[2]["cpus"] = json!([2]);
    fs::write(&path, rows.to_string()).unwrap();
    for implementation in ["ozzy", "iggy", "all"] {
        args.implementation = implementation.into();
        for mode in ["replicated-persisting", "disk-quorum"] {
            args.modes = vec![mode.into()];
            args.validate(VALIDATION_CPUS).unwrap();
        }
    }
    args.modes = vec!["durable".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.modes = vec!["replicated-persisting".into()];
    args.implementation = "redpanda".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.implementation = "ozzy".into();
    args.control_bind = None;
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn distributed_comparison_keeps_adapter_windows_and_freezes_topology() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("placements.json");
    let rows = json!([
        {"bind":"192.0.2.1","storage_dir":"/var/tmp","cpus":[0]},
        {"bind":"192.0.2.1","storage_dir":"/mnt/bench/tmp","cpus":[1]},
        {"bind":"192.0.2.2","ssh":"remote","executable":"/mnt/bench/tmp/bin/ozzy_timed_bench","storage_dir":"/mnt/bench/tmp","cpus":[0]}
    ]);
    fs::write(&path, rows.to_string()).unwrap();
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "all",
        "--modes",
        "replicated-persisting",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--placements",
        path.to_str().unwrap(),
        "--control-bind",
        "tcp://192.0.2.1:0",
        "--request-records",
        "2048",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["deployment"], rows);
    for case in args.cases() {
        let command = args
            .command(
                Path::new("/bin/ozzy_timed_bench"),
                &case,
                Some("192.0.2.1:9000"),
            )
            .unwrap();
        assert_eq!(
            command.iter().any(|v| v == "--placements"),
            case["impl"] == "ozzy"
        );
        assert_eq!(args.case_configuration(&case)["request_records"], 2048);
    }
    let target = directory.path().join("frozen");
    fs::create_dir(&target).unwrap();
    let frozen = ozzy_bench::automation::distributed::freeze(&path, &target).unwrap();
    fs::write(&path, "[]").unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(frozen).unwrap()).unwrap(),
        rows
    );
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn default_segment_limits_follow_record_size_and_explicit_limits_override() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        let expected = if case["size"].as_u64().unwrap() >= 1024 {
            "1024"
        } else {
            "256"
        };
        let command = args.command(Path::new("bench"), &case, None).unwrap();
        assert_eq!(option(&command, "--segment-mib"), expected);
        assert!(!command.iter().any(|arg| arg == "--segment-decoded-mib"));
    }
    assert!(args.configuration()["segment_mib"].is_null());

    args.segment_mib = Some(64);
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        let command = args.command(Path::new("bench"), &case, None).unwrap();
        assert_eq!(option(&command, "--segment-mib"), "64");
        assert!(!command.iter().any(|arg| arg == "--segment-decoded-mib"));
    }
    for invalid in [3, 1025] {
        args.segment_mib = Some(invalid);
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    for flag in [
        "--segment-decoded-mib",
        "--compression-workers",
        "--zero-ahead",
        "--lz4-transport",
    ] {
        assert!(
            compare::Args::try_parse_from(["compare", flag, "128"]).is_err(),
            "{flag}"
        );
    }
}

#[test]
fn summaries_preserve_overload_in_either_repetition_order() {
    let healthy = json!({"case":{"rate":100_000},"measurements":{"latency":12}});
    let overloaded = json!({
        "case":{"rate":100_000},
        "failure":"scheduled backlog exceeded",
        "measurements":{}
    });
    let other = json!({"case":{"rate":50_000},"measurements":{"latency":4}});
    for rows in [
        vec![healthy.clone(), overloaded.clone(), other.clone()],
        vec![overloaded.clone(), healthy.clone(), other.clone()],
    ] {
        let summary = records::summarize(&rows).unwrap();
        let summary = summary.as_array().unwrap();
        let failed = summary
            .iter()
            .find(|row| row["case"]["rate"] == 100_000)
            .unwrap();
        assert_eq!(failed["failure"], "scheduled backlog exceeded");
        assert_eq!(failed["repetitions"], 2);
        assert_eq!(failed["measurements"], json!({}));
        let measured = summary
            .iter()
            .find(|row| row["case"]["rate"] == 50_000)
            .unwrap();
        assert_eq!(measured["measurements"]["latency"]["median"], 4.0);
    }
}

#[test]
fn obsolete_broker_omq_placement_flag_is_rejected() {
    assert!(compare::Args::try_parse_from(["compare", "--broker-omq-on-shard"]).is_err());
}

#[test]
fn several_readers_per_partition_run_as_separate_ozzy_reader_processes() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "replicated-persisting",
        "--partitions",
        "2",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert!(args.configuration().get("readers_per_partition").is_none());
    let case = args.cases().remove(0);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--reader-workers"), "2");
    assert_eq!(option(&command, "--readers-per-partition"), "1");

    args.readers_per_partition = 16;
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["readers_per_partition"], 16);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--reader-workers"), "32");
    assert_eq!(option(&command, "--readers-per-partition"), "16");

    assert!(!command.iter().any(|arg| arg == "--broker-io-threads"));
    assert!(args.configuration().get("broker_io_threads").is_none());
    args.broker_io_threads = Some(2);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["broker_io_threads"], 2);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--broker-io-threads"), "2");
    args.broker_io_threads = Some(33);
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.broker_io_threads = None;

    assert!(!command.iter().any(|arg| arg == "--live-readers"));
    assert!(args.configuration().get("live_readers").is_none());
    args.live_readers = true;
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration()["live_readers"], true);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert!(command.iter().any(|arg| arg == "--live-readers"));
    args.modes = vec!["buffered".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.modes = vec!["replicated-persisting".into()];
    args.live_readers = false;

    args.readers_per_partition = 17;
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.readers_per_partition = 2;
    args.implementation = "all".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
}
