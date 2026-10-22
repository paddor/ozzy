//! Work accounting: the slots a writer or client still owns.

use ozzy_proto::NodeId;

use super::{BatchState, NativeIntake, Slot};

impl NativeIntake {
    /// Requests of this client's writer that still own a proposal or a reply,
    /// including a refusal in the client's rejection slot. Other writers on
    /// the partition do not count.
    pub fn writer_work(&self, node: NodeId, producer: ozzy_proto::ProducerId) -> usize {
        let stride = self.stride();
        let requests = self.writers.assigned(node, producer).map_or(0, |writer| {
            self.slots[writer * stride..(writer + 1) * stride]
                .iter()
                .filter(|slot| slot.active())
                .count()
        });
        requests + usize::from(self.rejection_active(node))
    }

    /// Writer opens and refusals of this client that still own a slot.
    /// APPENDs count against their writer instead.
    pub fn client_work(&self, node: NodeId) -> usize {
        let stride = self.stride();
        let opens = self
            .writers
            .assigned_to(node)
            .filter(|&writer| self.slots[writer * stride].active())
            .count();
        opens + usize::from(self.rejection_active(node)) + self.progress_work(node)
    }

    /// Whether the client's rejection slot owns a proposal or a reply.
    fn rejection_active(&self, node: NodeId) -> bool {
        self.writers
            .assigned_rejection(node, self.stride())
            .is_some_and(|slot| self.slots.get(slot).is_some_and(Slot::active))
    }

    /// Pending proposals and replies remain bounded by startup slots.
    pub fn has_work(&self) -> bool {
        debug_assert_eq!(
            self.busy,
            self.slots
                .iter()
                .filter(|slot| slot.active() || slot.batch.is_some())
                .count()
        );
        debug_assert!(self.slots.iter().enumerate().all(|(index, slot)| {
            slot.active() == (self.active[index / 64] & 1 << (index % 64) != 0)
        }));
        debug_assert_eq!(
            self.progress_held,
            self.slots
                .iter()
                .filter(|slot| {
                    slot.batch
                        .as_ref()
                        .is_some_and(|batched| matches!(batched.state, BatchState::Held { .. }))
                })
                .count()
        );
        self.busy != 0
    }
}
