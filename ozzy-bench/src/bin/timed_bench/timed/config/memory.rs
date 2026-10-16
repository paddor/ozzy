//! Memory the run reserves before it starts: client windows, broker
//! payload arenas and history, and the memory storage image.

use super::super::super::{Result, error};
use super::Config;

impl Config {
    pub(in super::super) fn preflight_memory(&self) -> Result<()> {
        let mem = std::fs::read_to_string("/proc/meminfo")?;
        let available: u64 = mem
            .lines()
            .find_map(|line| line.strip_prefix("MemAvailable:"))
            .and_then(|line| line.split_whitespace().next())
            .ok_or_else(|| error("missing available memory"))?
            .parse()?;
        let required = self.memory_reservation_bytes();
        if required > available * 1024 {
            return Err(error(format!(
                "timed native workload reserves {required} bytes including bounded history, client windows and safety reserve; only {} available",
                available * 1024
            )));
        }
        Ok(())
    }

    /// Memory media bound per broker. One file holds at most a segment or a
    /// checkpoint image; the total counts dirty and durable copies.
    pub(in super::super) fn memory_image(&self) -> Result<ozzy_io::simulation::ImageLimits> {
        const MIB: usize = 1024 * 1024;
        let total = usize::try_from(self.args.memory_mib)? * MIB;
        let file = (self.args.segment_mib * MIB).max(256 * MIB);
        if file > total {
            return Err(error("broker memory image smaller than one segment"));
        }
        Ok(ozzy_io::simulation::ImageLimits {
            nodes: 4096,
            directory_entries: 16384,
            file_bytes: file,
            total_bytes: total,
        })
    }

    fn memory_media_bytes(&self) -> u64 {
        if self.args.memory_storage {
            self.args.system.brokers() as u64 * self.args.memory_mib * 1024 * 1024
        } else {
            0
        }
    }

    pub(super) fn memory_reservation_bytes(&self) -> u64 {
        if let Some(native) = &self.native {
            return native.reservation_bytes + self.memory_media_bytes();
        }
        let clients = self.client_reservation_bytes();
        #[cfg(feature = "comparisons")]
        if self.args.external_system.is_some() {
            // The external broker already exists when MemAvailable is sampled.
            return clients;
        }
        clients + self.broker_reservation_bytes()
    }

    /// Writer request copies, reader windows, one body pool per lane, and a
    /// fixed safety reserve.
    fn client_reservation_bytes(&self) -> u64 {
        // In-flight requests plus one preparation slot per APPEND slot.
        let request_copies = 2 * self.args.writer_inflight_appends as u64;
        #[cfg(feature = "comparisons")]
        let request_copies = if self.args.external_system.is_some() {
            1
        } else {
            request_copies
        };
        self.writers as u64
            * self.args.request_records.max(1024) as u64
            * self.args.record_bytes as u64
            * 8
            * request_copies
            + self.reader_slots() as u64
                * self.reader_records() as u64
                * self.args.record_bytes as u64
                * 8
            // Every writer and reader lane builds its own body pool.
            + (self.writers + self.reader_slots()) as u64 * super::super::producer::payload_pool_bytes(self)
            + 1024 * 1024 * 1024
    }

    /// Broker payload arenas, replication windows, reader arenas, proposals
    /// and history descriptors over every journal owner.
    fn broker_reservation_bytes(&self) -> u64 {
        let owners = if self.args.system.brokers() == 3 {
            3
        } else {
            self.args.app_threads as u64
        };
        // Disk history is a file-count allowance, not a touched RAM arena.
        // Reserve active/predecessor payloads plus operation backing held by
        // admitted reads. Charge conservative descriptor/state headroom for
        // this benchmark's known one-part, fixed-size records.
        let payloads = owners
            * (2 * self.decode_limits().max_segment_decoded_body_bytes as u64
                + self.args.read_depth as u64 * self.history.operation.max_body_bytes as u64);
        // Every replicated disk broker retains its live window in RAM.
        let backlog = if self.args.system.brokers() == 3 {
            let pipeline = self.disk_pipeline();
            owners * (3 * pipeline.max_body_bytes as u64 + pipeline.max_operations as u64 * 512)
        } else {
            0
        };
        let replay = if self.args.system.brokers() == 3 {
            let cache = self.disk_pipeline();
            owners * (2 * cache.max_body_bytes as u64 + cache.max_operations as u64 * 1024)
        } else {
            0
        };
        // Each additional reader owns one disk and one transport arena per
        // broker. So does each published partition.
        let lanes = if self.args.live_readers {
            self.writers
        } else {
            0
        };
        let readers = owners
            * (self.reader_slots() - self.writers + lanes) as u64
            * 2
            * self.reader_limits().envelope.max_payload_bytes as u64;
        // Each broker reserves one APPEND proposal arena per writer slot.
        let proposals = if self.args.system.brokers() == 3 {
            owners
                * (self.writers * self.writer_proposals()) as u64
                * self.history.operation.max_body_bytes as u64
        } else {
            0
        };
        payloads
            + backlog
            + replay
            + proposals
            + readers
            + self.resident_descriptor_reservation(payloads)
            + owners * self.history.max_operations as u64 * 512
    }

    pub(super) fn resident_descriptor_reservation(&self, bodies: u64) -> u64 {
        let payload = self.args.record_bytes as u64;
        if payload > 255 {
            return bodies / payload * 512;
        }
        // These fixed-size, single-part records use the canonical packed layout:
        // 16-byte ID + 1-byte length per record, 76 bytes per nonempty batch.
        // Resident selectors are 24 bytes/record; double for allocation headroom.
        // Charge 768 per batch for its cached operation, batch summary, shared
        // ownership, hash buckets and growing index runs. Count even one-record
        // batches; transparent SDK batching is not a minimum-size guarantee.
        let record = payload + 17;
        bodies.div_ceil(record) * 48 + bodies.div_ceil(record + 76) * 768
    }
}
