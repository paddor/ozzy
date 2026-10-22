//! SDK process launch adapters; the broker and measurement protocol are shared.
use super::{Args, Error, Worker, cpus};
use serde_json::{Value, json};
use std::{collections::BTreeMap, process::Command, sync::mpsc::Sender};
pub(super) fn clients(
    args: &Args,
    addresses: &Value,
    workers: &mut BTreeMap<String, Worker>,
    events: &Sender<(String, Value)>,
) -> Result<Vec<String>, Error> {
    let cpus = cpus(&args.producer_cpus)?;
    let mut roles = Vec::new();
    for (i, cpu) in cpus.iter().copied().chain([args.consumer_cpu]).enumerate() {
        let role = if i == cpus.len() {
            "consumer".to_owned()
        } else {
            format!("producer{i}")
        };
        let mut command = Command::new("taskset");
        command.args(["-c", &cpu.to_string()]);
        let options = json!({
            "role": if i == cpus.len() { "consumer" } else { "producer" },
            "producer": i, "producers": cpus.len(), "brokers": addresses,
            "api": args.api, "codec": args.codec, "size": args.size,
            "profile": args.profile, "confirmation": args.confirmation,
            "reader_queue_messages": args.reader_queue_messages,
            "window": args.window, "warmup": args.warmup, "duration": args.duration,
        });
        if args.java.is_some() {
            java(&mut command, args, &role, &options)?;
        } else {
            command
                .arg(args.python.as_ref().unwrap())
                .arg("-I")
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../bindings/python/examples/pipeline.py"
                ))
                .env_remove("PYTHONPATH")
                .env("OZZY_BENCH_OPTIONS", options.to_string());
        }
        workers.insert(role.clone(), Worker::spawn(&role, &mut command, events)?);
        roles.push(role);
    }
    Ok(roles)
}

fn java(command: &mut Command, args: &Args, role: &str, options: &Value) -> Result<(), Error> {
    command.arg(args.java.as_ref().unwrap()).args([
        "--enable-native-access=ALL-UNNAMED",
        "-Xms128m",
        "-Xmx512m",
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=1",
        "-XX:+PreserveFramePointer",
    ]);
    if let Some(path) = &args.java_library {
        command.arg(format!("-Dozzy.native.path={}", path.display()));
    }
    profile(command, args, role)?;
    command.args([
        "-cp",
        args.java_classpath.as_ref().unwrap(),
        "io.ozzy.bench.Pipeline",
    ]);
    for key in [
        "role",
        "producer",
        "producers",
        "api",
        "codec",
        "size",
        "confirmation",
        "reader_queue_messages",
        "window",
    ] {
        let value = &options[key];
        command.arg(
            value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned),
        );
    }
    for broker in options["brokers"].as_array().ok_or("missing brokers")? {
        for value in broker.as_array().ok_or("invalid broker")? {
            command.arg(value.as_str().ok_or("invalid address")?);
        }
    }
    Ok(())
}

fn profile(command: &mut Command, args: &Args, role: &str) -> Result<(), Error> {
    if let Some(agent) = &args.java_agent {
        let path = args
            .profile
            .as_ref()
            .ok_or("--java-agent requires --profile")?;
        command.arg(format!(
            "-agentpath:{}=start,event=cpu,cstack=dwarf,threads,file={}.{}.collapsed,log={}.{}.profiler.log",
            agent.display(), path.display(), role, path.display(), role));
    } else if let Some(path) = &args.profile {
        command.arg("-Xlog:jfr*=off").arg(format!(
            "-XX:StartFlightRecording=filename={}.{}.jfr,settings=profile,dumponexit=true",
            path.display(),
            role
        ));
    }
    Ok(())
}
