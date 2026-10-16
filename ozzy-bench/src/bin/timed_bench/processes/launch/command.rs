//! Worker command lines: the arguments a child role runs with and the local
//! or SSH command that starts it at its placement.

use super::super::super::{Args, PayloadCompression, Result, error};
use super::{Placement, Process};
use ozzy_bench::automation::distributed::quote;
use ozzy_bench::budget::{EXIT_GRACE, timeout_arg};
use ozzy_proto::GroupId;
use std::process::Command;
use std::time::Duration;

/// The storage directory and I/O thread count one worker runs with: the
/// run's own, or a broker's placement storage and broker I/O threads.
pub(super) struct Resources {
    storage_dir: String,
    io_threads: usize,
}

impl Resources {
    /// The run-wide values, as every client role uses them.
    pub(super) fn shared(args: &Args) -> Self {
        Self {
            storage_dir: args.storage_dir.to_string_lossy().into_owned(),
            io_threads: args.io_threads,
        }
    }

    fn for_role(args: &Args, placement: &Placement, role: &str) -> Result<Self> {
        let mut resources = Self::shared(args);
        if let (Some(threads), "--worker-index") = (args.broker_io_threads, role) {
            resources.io_threads = usize::try_from(threads)?;
        }
        if let Some(storage) = &placement.storage_dir {
            resources.storage_dir = storage
                .to_str()
                .ok_or_else(|| error("non-UTF8 storage path"))?
                .into();
        }
        Ok(resources)
    }
}

impl Process {
    pub(super) fn arguments(
        args: &Args,
        index: usize,
        group: GroupId,
        placement: &Placement,
        role: &str,
    ) -> Result<Vec<String>> {
        let resources = Resources::for_role(args, placement, role)?;
        let mut arguments = vec![
            role.into(),
            index.to_string(),
            "--bind".into(),
            placement.bind.to_string(),
            "--group".into(),
            group.to_string(),
            "--record-bytes".into(),
            args.record_bytes.to_string(),
            "--history-mib".into(),
            args.history_mib.to_string(),
        ];
        #[cfg(feature = "comparisons")]
        Self::check_external(args)?;
        Self::execution_arguments(args, resources, &mut arguments);
        Self::mode_arguments(args, &mut arguments);
        Ok(arguments)
    }

    /// Workload, deployment, sizing and timing settings every role shares.
    pub(super) fn execution_arguments(
        args: &Args,
        resources: Resources,
        arguments: &mut Vec<String>,
    ) {
        for (enabled, flag) in [
            (args.balanced_partitions, "--balanced-partitions"),
            (args.random_payload, "--random-payload"),
            (args.json_payload, "--json-payload"),
            (args.live_readers, "--live-readers"),
            (args.binary_payload, "--binary-payload"),
        ] {
            if enabled {
                arguments.push(flag.into());
            }
        }

        #[cfg(feature = "comparisons")]
        Self::external_arguments(args, arguments);
        args.control.append(arguments);
        args.native.append(arguments);
        #[cfg(feature = "comparisons")]
        if args.external_system.is_none() {
            arguments.extend(["--system".into(), args.system.name().into()]);
        }
        #[cfg(not(feature = "comparisons"))]
        arguments.extend(["--system".into(), args.system.name().into()]);
        Self::resource_arguments(args, resources, arguments);
        Self::schedule_arguments(args, arguments);
    }

    /// Storage, I/O, client batch and reader sizing.
    fn resource_arguments(args: &Args, resources: Resources, arguments: &mut Vec<String>) {
        arguments.extend([
            "--storage-dir".into(),
            resources.storage_dir,
            "--segment-decoded-mib".into(),
            args.segment_decoded_mib.to_string(),
            "--segment-mib".into(),
            args.segment_mib.to_string(),
            "--direct-io".into(),
            args.direct_io.to_string(),
            "--io-backend".into(),
            args.io_backend.name().into(),
            "--aio-depth".into(),
            args.aio_depth.to_string(),
            "--writer-batch-records".into(),
            args.writer_batch_records.to_string(),
            "--writer-batch-target-kib".into(),
            args.writer_batch_target_kib.to_string(),
            "--writer-inflight-appends".into(),
            args.writer_inflight_appends.to_string(),
            "--reader-records".into(),
            args.reader_records.to_string(),
            "--reader-payload-mib".into(),
            args.reader_payload_mib.to_string(),
            "--io-threads".into(),
            resources.io_threads.to_string(),
            "--app-threads".into(),
            args.app_threads.to_string(),
        ]);
    }

    /// The measured duration with its reader layout, and the offered load.
    fn schedule_arguments(args: &Args, arguments: &mut Vec<String>) {
        if let Some(duration) = args.duration {
            arguments.extend([
                "--duration".into(),
                duration.to_string(),
                "--warmup".into(),
                args.warmup.to_string(),
                "--reader-workers".into(),
                args.reader_workers().to_string(),
                "--readers-per-partition".into(),
                args.readers_per_partition.to_string(),
                "--history-operations".into(),
                args.history_operations.to_string(),
                "--drain-timeout-secs".into(),
                args.drain_timeout_secs.to_string(),
            ]);
        }
        if let Some(rate) = args.records_per_second {
            arguments.extend(["--records-per-second".into(), rate.to_string()]);
        }
        if let Some(ramp) = &args.ramp {
            arguments.extend(["--ramp".into(), ramp.to_string()]);
        }
    }

    /// Ingress, streaming, payload, storage medium and transport switches.
    fn mode_arguments(args: &Args, arguments: &mut Vec<String>) {
        if args.network_ingress {
            arguments.push("--network-ingress".into());
        }
        if args.streaming {
            arguments.extend([
                "--streaming".into(),
                "--request-records".into(),
                args.request_records.to_string(),
            ]);
        }
        if args.payload_compression == PayloadCompression::Off {
            arguments.extend(["--payload-compression".into(), "off".into()]);
        }
        if args.network_ingress {
            arguments.push("--processes".into());
        }
        if args.memory_storage {
            arguments.extend([
                "--memory-storage".into(),
                "--memory-mib".into(),
                args.memory_mib.to_string(),
            ]);
        }
        if args.inproc {
            arguments.push("--inproc".into());
        }
        if args.transport != ozzy_bench::native::Transport::Tcp {
            arguments.extend(["--transport".into(), args.transport.flag().into()]);
        }
        for (name, value) in [
            ("--producer-workers", args.producer_workers),
            ("--window", args.window),
        ] {
            if let Some(value) = value {
                arguments.extend([name.into(), value.to_string()]);
            }
        }
    }

    /// An external system needs its policy and endpoint too.
    #[cfg(feature = "comparisons")]
    fn check_external(args: &Args) -> Result<()> {
        let incomplete = args.external_system.is_some()
            && (args.external_policy.is_none() || args.external_endpoint.is_none());
        if incomplete {
            return Err(error(
                "--external-system requires --external-policy and --external-endpoint",
            ));
        }
        Ok(())
    }

    /// The external target, if any; `arguments` checked it is complete.
    #[cfg(feature = "comparisons")]
    fn external_arguments(args: &Args, arguments: &mut Vec<String>) {
        if let (Some(system), Some(policy), Some(endpoint)) = (
            args.external_system,
            args.external_policy,
            &args.external_endpoint,
        ) {
            arguments.extend([
                "--external-system".into(),
                system.name().into(),
                "--external-policy".into(),
                policy.name().into(),
                "--external-endpoint".into(),
                endpoint.clone(),
            ]);
        }
    }
}

/// Launch a worker at its placement. A remote worker is terminated once it
/// outlives `lifetime`, in case its controller and SSH link died.
pub(super) fn placed_command(
    placement: &Placement,
    arguments: Vec<String>,
    lifetime: Duration,
) -> Result<Command> {
    let command = if let Some((host, executable)) = &placement.remote {
        let mut shell = format!(
            "exec timeout --signal=TERM --kill-after={} {} {}{}",
            timeout_arg(EXIT_GRACE),
            timeout_arg(lifetime),
            placement
                .cpu_list()
                .map_or_else(String::new, |cpus| format!(
                    "taskset --cpu-list {} ",
                    quote(&cpus)
                )),
            quote(
                executable
                    .to_str()
                    .ok_or_else(|| error("non-UTF8 executable"))?
            )
        );
        for argument in &arguments {
            shell.push(' ');
            shell.push_str(&quote(argument));
        }
        let mut command = ozzy_bench::placement::ssh(host);
        command.arg(shell);
        command
    } else {
        let executable = std::env::current_exe()?;
        let mut command = if let Some(cpus) = placement.cpu_list() {
            let mut command = Command::new("taskset");
            command.args(["--cpu-list", &cpus]).arg(executable);
            command
        } else {
            Command::new(executable)
        };
        command.args(arguments);
        command
    };
    Ok(command)
}
