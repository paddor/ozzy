//! Worker command lines for one comparison case.
use super::{Args, default_segment_mib};
use crate::automation::{Result, artifact_root, json_field, json_file};
use serde_json::{Value, json};
use std::path::Path;

impl Args {
    /// Build the worker command for one case with its storage and endpoint bindings.
    pub fn command(
        &self,
        binary: &Path,
        case: &Value,
        endpoint: Option<&str>,
    ) -> Result<Vec<String>> {
        let mode = json_field(case, "mode", Value::as_str)?;
        let external = case["impl"] != "ozzy";
        if !external && mode == "buffered" {
            return Err("Ozzy buffered mode is no longer a live comparison".into());
        }
        let cluster = crate::automation::cluster_mode(mode);
        let size = json_field(case, "size", Value::as_u64)?;
        let mut argv = Argv(vec![
            "taskset".into(),
            "-c".into(),
            self.client_cpus
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
            binary.display().to_string(),
            "--processes".into(),
            "--network-ingress".into(),
            "--streaming".into(),
        ]);
        argv.add(
            if external {
                "--external-policy"
            } else {
                "--system"
            },
            if external || cluster {
                mode.to_owned()
            } else {
                format!("single-{mode}")
            },
        );
        self.common_flags(&mut argv, size, self.request_records_for(case))?;
        if external {
            self.external_flags(&mut argv, case, endpoint, size)?;
        } else {
            argv.add("--partitions", self.partitions.to_string());
            self.feed_flags(&mut argv)?;
            self.io_flags(&mut argv, cluster)?;
        }
        if let Some(ramp) = &self.ramp {
            argv.add("--ramp", ramp.to_string());
        }
        if let Some(rate) = self.records_per_second {
            argv.add("--records-per-second", rate.to_string());
        }
        if !external {
            self.switches(&mut argv, cluster);
        }
        match case["pattern"].as_str() {
            Some("events") if size == 16 => argv.push("--binary-payload"),
            Some("events" | "json") => argv.push("--json-payload"),
            Some("random") => argv.push("--random-payload"),
            _ => (),
        }
        Ok(argv.0)
    }

    /// Record shape, windows, worker counts, timing, and storage location.
    fn common_flags(&self, argv: &mut Argv, size: u64, batch: u64) -> Result<()> {
        for (flag, value) in [
            ("--record-bytes", size.to_string()),
            (
                "--window",
                self.feeds
                    .writers(usize::try_from(self.partitions)?)
                    .to_string(),
            ),
            ("--request-records", batch.to_string()),
            ("--reader-records", self.reader_records.to_string()),
            ("--reader-payload-mib", self.reader_payload_mib.to_string()),
            (
                "--writer-batch-records",
                batch
                    .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS as u64)
                    .to_string(),
            ),
            // Each writer process owns two partition-targeted SDK writers.
            ("--producer-workers", self.writer_processes().to_string()),
            ("--reader-workers", self.readers().to_string()),
            (
                "--readers-per-partition",
                self.readers_per_partition.to_string(),
            ),
            ("--duration", self.duration.to_string()),
            ("--warmup", self.warmup.to_string()),
            (
                "--segment-mib",
                self.segment_mib
                    .unwrap_or_else(|| default_segment_mib(size))
                    .to_string(),
            ),
            ("--storage-dir", bench_root()),
        ] {
            argv.add(flag, value);
        }
        Ok(())
    }

    /// An external system's history bounds, partitions, and endpoint.
    fn external_flags(
        &self,
        argv: &mut Argv,
        case: &Value,
        endpoint: Option<&str>,
        size: u64,
    ) -> Result<()> {
        argv.add("--history-mib", self.disk_history_mib.to_string());
        argv.add("--history-operations", 262_144.to_string());
        argv.add(
            "--segment-decoded-mib",
            default_segment_mib(size).to_string(),
        );
        argv.add("--partitions", self.partitions.to_string());
        argv.add(
            "--external-system",
            case["impl"]
                .as_str()
                .ok_or("missing implementation")?
                .into(),
        );
        argv.add("--external-storage-dir", bench_root());
        argv.add(
            "--external-endpoint",
            endpoint.ok_or("missing external endpoint")?.into(),
        );
        Ok(())
    }

    /// Redundant and skewed feeds, consumer groups, deduplication limits, and
    /// delay.
    fn feed_flags(&self, argv: &mut Argv) -> Result<()> {
        if self.feeds.redundant_producers != 1 {
            argv.add(
                "--redundant-producers",
                self.feeds.redundant_producers.to_string(),
            );
        }
        if self.feeds.hot_partition_percent != 0 {
            argv.add(
                "--hot-partition-percent",
                self.feeds.hot_partition_percent.to_string(),
            );
        }
        if self.feeds.redundant_feeds != 0 {
            argv.add("--redundant-feeds", self.feeds.redundant_feeds.to_string());
        }
        if self.groups != 0 {
            argv.add("--groups", self.groups.to_string());
            argv.add("--group-members", self.group_members.to_string());
            argv.add("--ack-window", self.ack_window.to_string());
        }
        if let Some(window) = self.dedup_window_secs {
            argv.add("--dedup-window-secs", window.to_string());
            let entries = self.dedup_max_entries.ok_or("missing dedup entry limit")?;
            argv.add("--dedup-max-entries", entries.to_string());
            let bytes = self.dedup_max_bytes.ok_or("missing dedup byte limit")?;
            argv.add("--dedup-max-bytes", bytes.to_string());
        }
        if let Some(millis) = self.delay_millis {
            argv.add("--delay-millis", millis.to_string());
            if let Some(entries) = self.delay_max_entries {
                argv.add("--delay-max-entries", entries.to_string());
            }
            argv.add("--drain-timeout-secs", self.drain_secs().to_string());
        }
        Ok(())
    }

    /// Broker threads, placements, writer pipelining, and storage backend.
    fn io_flags(&self, argv: &mut Argv, cluster: bool) -> Result<()> {
        if let Some(threads) = self.broker_io_threads {
            argv.add("--broker-io-threads", threads.to_string());
        }
        if let Some(path) = &self.placements {
            argv.add("--placements", path.display().to_string());
            let bind = self
                .control_bind
                .clone()
                .ok_or("placements need a control bind")?;
            argv.add("--control-bind", bind);
        }
        argv.add(
            "--writer-inflight-appends",
            self.writer_inflight_appends.to_string(),
        );
        argv.add(
            "--writer-batch-target-kib",
            self.writer_batch_target_kib.to_string(),
        );
        if let Some(mib) = self.shard_resident_mib {
            argv.add("--shard-resident-mib", mib.to_string());
        }
        if let Some(direct) = self.direct_io {
            argv.add("--direct-io", direct.to_string());
        }
        if let Some(backend) = &self.io_backend {
            argv.add("--io-backend", backend.clone());
        }
        if let Some(depth) = self.aio_depth {
            argv.add("--aio-depth", depth.to_string());
        }
        if let Some(mib) = self.memory_mib {
            argv.add("--memory-mib", mib.to_string());
        }
        if !cluster {
            argv.add("--app-threads", self.local_shards().to_string());
        }
        Ok(())
    }

    /// Ozzy-only switches: memory storage, inproc, transport, and readers.
    fn switches(&self, argv: &mut Argv, cluster: bool) {
        if self.memory_storage {
            argv.push("--memory-storage");
        }
        if self.inproc {
            argv.push("--inproc");
        }
        if self.transport != "tcp" {
            argv.add("--transport", self.transport.clone());
        }
        if !cluster {
            argv.push("--balanced-partitions");
        }
        if self.live_readers {
            argv.push("--live-readers");
        }
        if self.payload_compression == "off" {
            argv.add("--payload-compression", "off".into());
        }
    }
    /// Native brokers get explicit process masks; clients inherit their own pool.
    pub fn case_command(
        &self,
        binary: &Path,
        case: &Value,
        directory: &Path,
        endpoint: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut command = self.command(binary, case, endpoint)?;
        if case["impl"] == "ozzy" && self.placements.is_none() {
            let brokers =
                if crate::automation::cluster_mode(json_field(case, "mode", Value::as_str)?) {
                    3
                } else {
                    1
                };
            let cpus = self.broker_cpus.brokers(brokers);
            let placements: Vec<_> = (0..3)
                .map(|index| {
                    json!({"bind":"127.0.0.1", "storage_dir":bench_root(),
                    "cpus":cpus[index.min(brokers - 1)]})
                })
                .collect();
            let path = directory.join("broker-placement.json");
            json_file(&path, &json!(placements))?;
            command.extend(["--placements".into(), path.display().to_string()]);
        }
        Ok(command)
    }
}

/// A worker command line under construction.
struct Argv(Vec<String>);

impl Argv {
    fn add(&mut self, flag: &str, value: String) {
        self.0.extend([flag.into(), value]);
    }

    fn push(&mut self, flag: &str) {
        self.0.push(flag.into());
    }
}

/// Where benchmark workers keep their storage.
pub(super) fn bench_root() -> String {
    artifact_root()
        .join("ozzy-bench")
        .to_string_lossy()
        .into_owned()
}
