//! Driver-owned packing buffers. Final transport release only schedules reclaim.

use super::super::{WriterConfig, state::Admission};
use crate::signal::DataSignal;
use omq_tokio::message::{Payload, PayloadOwner};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub(in super::super) struct PayloadPool {
    // Local writers use route zero; fixed groups have exactly three routes.
    // Old OMQ byte views may outlive a session without blocking another broker.
    slots: [Vec<Arc<Slot>>; 3],
    route: usize,
}

#[derive(Debug)]
struct Slot {
    bytes: Vec<u8>,
    work: Arc<DataSignal>,
}

impl PayloadPool {
    pub(in super::super) fn new(config: &WriterConfig, work: &Arc<DataSignal>) -> Self {
        let mut pool = Self {
            slots: std::array::from_fn(|_| Vec::new()),
            route: 0,
        };
        pool.select_route(0, config, work);
        pool
    }

    pub(in super::super) fn select_route(
        &mut self,
        route: usize,
        config: &WriterConfig,
        work: &Arc<DataSignal>,
    ) {
        self.route = route;
        if !self.slots[route].is_empty() {
            return;
        }
        let count = if config.limits.max_records == 1 {
            0
        } else {
            config.inflight_appends
        };
        self.slots[route] = (0..count)
            .map(|_| {
                Arc::new(Slot {
                    bytes: Vec::with_capacity(
                        config.limits.envelope.max_payload_bytes.min(1024 * 1024),
                    ),
                    work: work.clone(),
                })
            })
            .collect();
    }

    pub(in super::super) fn pack(
        &mut self,
        admission: &mut Admission,
        records: Range<usize>,
    ) -> Option<Payload> {
        if records.len() == 1 {
            return Some(admission.payload(records.start).clone());
        }
        for slot in &mut self.slots[self.route] {
            if let Some(output) = Arc::get_mut(slot) {
                output.bytes.clear();
                for record in admission.records.range(records) {
                    output.bytes.extend_from_slice(record.body.bytes());
                }
                return Some(Payload::from_shared_owner(slot.clone()));
            }
        }
        None
    }
}

impl AsRef<[u8]> for Slot {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl PayloadOwner for Slot {
    fn release(self: Arc<Self>) {
        let work = self.work.clone();
        drop(self);
        // Publish reference release before waking the driver. If it has already
        // stopped, dropping the final Arc also drops the buffer.
        work.mark();
    }
}
