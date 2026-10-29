//! The recorded comparison configuration and its case matrix.
use super::Args;
use crate::automation::{Result, read_json};
use serde_json::{Value, json};

impl Args {
    /// Serialize the complete comparison configuration for result provenance.
    pub fn configuration(&self) -> Result<Value> {
        let mut config = json!({"impl":self.implementation,"sizes":self.sizes,"modes":self.modes,"codecs":["raw"],"patterns":self.patterns,"repetitions":self.repetitions,"duration":self.duration,"warmup":self.warmup,"disk_history_mib":self.disk_history_mib,"partitions":self.partitions,"request_records":self.request_records,"reader_records":self.reader_records,"reader_payload_mib":self.reader_payload_mib,"writer_inflight_appends":self.writer_inflight_appends,"compression_workers":0,"broker_cpus":self.broker_cpus.pool(),"broker_cpu_sets":self.broker_cpus.sets(),"client_cpus":self.client_cpus,"durable_segment_io":"odsync","segment_mib":self.segment_mib,"records_per_second":self.records_per_second,"ramp":self.ramp.as_ref().map(ToString::to_string),"no_build":self.no_build,"check_only":self.check_only});
        self.payload_settings(&mut config);
        self.layer_settings(&mut config)?;
        self.feed_settings(&mut config);
        self.deployment_settings(&mut config)?;
        self.native_settings(&mut config);
        Ok(config)
    }

    /// Payload compression and the native reader API.
    fn payload_settings(&self, config: &mut Value) {
        if self.payload_compression == "off" {
            config["payload_compression"] = json!("none");
            config["payload_compression_threshold"] = Value::Null;
        } else {
            config["payload_compression"] = json!("adaptive-lz4");
            config["payload_compression_threshold"] =
                json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD);
        }
        config["native_reader_api"] = json!("decoded-records");
    }

    /// Consumer groups and the storage, worker, and transport layers.
    fn layer_settings(&self, config: &mut Value) -> Result<()> {
        if self.groups != 0 {
            config["consumer_groups"] = json!(self.groups);
            config["members_per_group"] = json!(self.group_members);
            config["ack_window"] = json!(self.ack_window);
            config["readers_per_partition"] = json!(self.groups);
        }
        // Recorded only when selected, so layers never match disk baselines.
        if self.memory_storage {
            config["broker_storage"] = json!("memory");
            config["memory_mib"] = json!(self.memory_mib.unwrap_or(2048));
        }
        if self.inproc {
            config["worker_placement"] = json!("inproc-threads");
        }
        if self.transport != "tcp" {
            let transport =
                <crate::native::Transport as clap::ValueEnum>::from_str(&self.transport, true)?;
            config["transport"] = json!(transport.scheme().as_str());
        }
        Ok(())
    }

    /// Deduplication limits, delay, and redundant or skewed feeds.
    fn feed_settings(&self, config: &mut Value) {
        if let Some(window) = self.dedup_window_secs {
            config["dedup_window_secs"] = json!(window);
            config["dedup_max_entries_per_partition"] = json!(self.dedup_max_entries);
            config["dedup_max_bytes_per_partition"] = json!(self.dedup_max_bytes);
        }
        if let Some(millis) = self.delay_millis {
            config["delay_millis"] = json!(millis);
            config["delay_max_entries_per_partition"] = json!(self.delay_max_entries);
        }
        if self.feeds.enabled() {
            config["redundant_producers"] = json!(self.feeds.redundant_producers);
            config["hot_partition_percent"] = json!(self.feeds.hot_partition_percent);
            if self.feeds.redundant_feeds != 0 {
                config["redundant_feeds"] = json!(self.feeds.redundant_feeds);
            }
        }
    }

    /// The placement file and its topology label.
    fn deployment_settings(&self, config: &mut Value) -> Result<()> {
        let Some(path) = &self.placements else {
            return Ok(());
        };
        let deployment = read_json(path)?;
        let placements = deployment
            .as_array()
            .ok_or("placement file is not a list")?;
        config["deployment_kind"] = json!(if placements.iter().all(|p| p.get("ssh").is_none()) {
            "all-local"
        } else {
            "two-local-one-remote"
        });
        config["deployment"] = deployment;
        Ok(())
    }

    /// Native shard, writer, reader, and disk controls.
    fn native_settings(&self, config: &mut Value) {
        if let Some(shards) = self.shards {
            config["native_shards"] = json!(shards);
        }
        config["native_batch_target_bytes"] = json!(u64::from(self.writer_batch_target_kib) * 1024);
        config["writer_batching"] = json!(true);
        config["writer_protocol"] = json!("peer-appends");
        if self.readers_per_partition != 1 {
            config["readers_per_partition"] = json!(self.readers_per_partition);
        }
        if let Some(threads) = self.broker_io_threads {
            config["broker_io_threads"] = json!(threads);
        }
        if self.live_readers {
            config["live_readers"] = json!(true);
        }
        config["native_max_record_bytes"] = json!(1024 * 1024);
        if let Some(direct) = self.direct_io {
            config["disk_direct_io"] = json!(direct);
        }
        if let Some(backend) = &self.io_backend {
            config["disk_io_backend"] = json!(backend);
        }
        if let Some(depth) = self.aio_depth {
            config["disk_aio_depth"] = json!(depth);
        }
        if let Some(mib) = self.shard_resident_mib {
            config["shard_resident_mib"] = json!(mib);
        }
    }

    pub(super) fn request_records_for(&self, _case: &Value) -> u64 {
        self.request_records
    }

    /// Actual per-implementation controls, including the explicit Iggy ceiling.
    pub fn case_configuration(&self, case: &Value) -> Result<Value> {
        let mut config = self.configuration()?;
        config["request_records"] = json!(self.request_records_for(case));
        if case["impl"] != "ozzy" {
            config["payload_compression"] = json!("none");
            config["payload_compression_threshold"] = Value::Null;
        }
        Ok(config)
    }
    fn case(&self, implementation: &str, mode: &str, size: u64, pattern: &str) -> Value {
        let mut case = json!({"impl":implementation,"mode":mode,"size":size,"codec":"raw","pattern":pattern,"rate":self.records_per_second});
        if let Some(ramp) = &self.ramp {
            case["ramp"] = json!(ramp.to_string());
        }
        case
    }
    /// Expand modes, sizes, payload patterns, and repetitions into serial cases.
    pub fn cases(&self) -> Vec<Value> {
        let mut cases = vec![];
        for mode in &self.modes {
            for size in &self.sizes {
                for pattern in &self.patterns {
                    if matches!(self.implementation.as_str(), "all" | "iggy") {
                        cases.push(self.case("iggy", mode, *size, pattern));
                    }
                    if matches!(self.implementation.as_str(), "all" | "redpanda") {
                        cases.push(self.case("redpanda", mode, *size, pattern));
                    }
                    if mode != "buffered" && matches!(self.implementation.as_str(), "all" | "ozzy")
                    {
                        cases.push(self.case("ozzy", mode, *size, pattern));
                    }
                }
            }
        }
        cases
    }
}
