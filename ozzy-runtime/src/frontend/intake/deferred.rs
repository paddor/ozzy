//! Busy actor input stays within its original shard admission reservation.

use super::{IntakeError, IntakeMessage, Links, RoutingTable, ShardIntake};

impl ShardIntake {
    /// Retain a dequeued message from this owner when its actor reports busy.
    /// A message occupies its original bounded reservation. No new grant or
    /// canonical allowance is issued while this request is deferred.
    pub fn defer(&mut self, message: IntakeMessage) -> Result<(), IntakeError> {
        let reservation = self
            .reservations
            .get_mut(message.reservation)
            .and_then(Option::as_mut)
            .filter(|reservation| {
                reservation.request == message.request
                    && reservation.dequeued != 0
                    && reservation.control_window.is_none()
                    && reservation.waiting.len() < reservation.window
            })
            .ok_or(IntakeError::Invariant)?;
        reservation.waiting.push_back(message);
        Ok(())
    }

    /// Retry one fixed reservation slot. Scan a bounded rotating subset per
    /// shard turn. Recheck session and routing before actor admission; stale
    /// input is returned with `current == false` so callers discard it.
    pub fn retry_deferred(
        &mut self,
        slot: usize,
        links: &Links,
        routes: &RoutingTable,
    ) -> Result<Option<IntakeMessage>, IntakeError> {
        let Some(reservation) = self.reservations.get_mut(slot).and_then(Option::as_mut) else {
            return Ok(None);
        };
        let Some(mut message) = reservation.waiting.pop_front() else {
            return Ok(None);
        };
        if reservation.request != message.request || message.reservation != slot {
            return Err(IntakeError::Invariant);
        }
        message.current = links
            .get(message.request.binding.peer)
            .map(|link| link.binding)
            == Some(message.request.binding)
            && routes.route(&message.message, message.request.binding).ok()
                == Some(message.request.route);
        Ok(Some(message))
    }
}
