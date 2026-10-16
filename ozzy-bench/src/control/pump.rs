//! Ordered, acknowledged control transport. Runs on the application runtime.

use super::{
    BenchResult, CHUNK, DEADLINE, Frame, Kind, MAX_REPORT_BYTES, Monitor, Outgoing, Route, Socket,
    Value, bench_error, mpsc,
};
use omq_tokio::{MonitorEvent, MonitorStream};

pub(super) async fn pump(
    socket: &Socket,
    route: Route,
    outgoing: mpsc::Receiver<Outgoing>,
    incoming: mpsc::Sender<BenchResult<Value>>,
    monitor: &Monitor,
) -> BenchResult<()> {
    let events = socket.monitor();
    socket
        .wait_connected(1, DEADLINE)
        .await
        .map_err(|cause| bench_error(format!("control handshake: {cause}")))?;
    let link = Link { socket, route };
    let (acks, acknowledged) = mpsc::channel(1);
    tokio::try_join!(
        link.send_reports(outgoing, acknowledged),
        link.receive_reports(events, acks, incoming, monitor),
    )?;
    Ok(())
}

/// The connected control socket and the identities that fence its frames.
#[derive(Clone, Copy)]
struct Link<'a> {
    socket: &'a Socket,
    route: Route,
}

impl Link<'_> {
    /// Send queued reports in chunks; each waits for the peer acknowledgment
    /// of its sequence before its completion fires.
    async fn send_reports(
        self,
        mut outgoing: mpsc::Receiver<Outgoing>,
        mut acknowledged: mpsc::Receiver<u64>,
    ) -> BenchResult<()> {
        let mut sequence = 0_u64;
        while let Some(out) = outgoing.recv().await {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| bench_error("control sequence overflow"))?;
            let bytes = serde_json::to_vec(&out.value)?;
            if bytes.len() > MAX_REPORT_BYTES {
                return Err(bench_error("oversized benchmark report"));
            }
            for (index, chunk) in bytes.chunks(CHUNK).enumerate() {
                let frame = Frame::chunk(out.kind, sequence, index * CHUNK, bytes.len(), chunk);
                self.send(frame, "control send").await?;
            }
            let ack = tokio::time::timeout(DEADLINE, acknowledged.recv())
                .await
                .map_err(|cause| {
                    bench_error(format!(
                        "control acknowledgment sequence={sequence}: {cause}"
                    ))
                })?
                .ok_or_else(|| bench_error("control acknowledgment channel closed"))?;
            if ack != sequence {
                return Err(bench_error("stale control acknowledgment"));
            }
            if let Some(completion) = out.completion {
                let _ = completion.send(Ok(()));
            }
        }
        Ok(())
    }

    /// Reassemble and deliver peer reports, acknowledging each, and pass
    /// acknowledgments of our own reports to the sender.
    async fn receive_reports(
        self,
        mut events: MonitorStream,
        acks: mpsc::Sender<u64>,
        incoming: mpsc::Sender<BenchResult<Value>>,
        monitor: &Monitor,
    ) -> BenchResult<()> {
        let mut assembly = Assembly::new();
        let mut input = Some(incoming);
        loop {
            let message = tokio::select! {
                message = self.socket.recv() => message?,
                event = events.recv() => {
                    if let MonitorEvent::Disconnected { reason, .. } = event? {
                        return Err(bench_error(format!("benchmark control peer disconnected: {reason:?}")));
                    }
                    continue;
                }
            };
            let frame = Frame::decode(&message, self.route)?;
            if frame.kind == Kind::Ack {
                acks.try_send(frame.sequence)
                    .map_err(|_| bench_error("unexpected control acknowledgment"))?;
                continue;
            }
            if !assembly.push(&frame)? {
                continue;
            }
            deliver(assembly.kind, &assembly.bytes, &mut input, monitor)?;
            let ack = Frame::ack(assembly.sequence);
            self.send(ack, "control reply acknowledgment").await?;
            assembly.next()?;
        }
    }

    async fn send(self, frame: Frame, purpose: &str) -> BenchResult<()> {
        let sequence = frame.sequence;
        tokio::time::timeout(DEADLINE, self.socket.send(frame.encode(self.route)))
            .await
            .map_err(|cause| bench_error(format!("{purpose} sequence={sequence}: {cause}")))??;
        Ok(())
    }
}

/// One peer report being reassembled from its ordered chunks.
struct Assembly {
    sequence: u64,
    kind: Kind,
    total: usize,
    bytes: Vec<u8>,
}

impl Assembly {
    fn new() -> Self {
        Self {
            sequence: 1,
            kind: Kind::Data,
            total: 0,
            bytes: Vec::new(),
        }
    }

    /// Append the next chunk of the current report; true once it is whole.
    fn push(&mut self, frame: &Frame) -> BenchResult<bool> {
        if frame.sequence != self.sequence || frame.offset != self.bytes.len() {
            return Err(bench_error("stale or discontinuous control frame"));
        }
        if self.bytes.is_empty() {
            self.kind = frame.kind;
            self.total = frame.total;
        }
        if frame.kind != self.kind || frame.total != self.total {
            return Err(bench_error("control report changed during transfer"));
        }
        self.bytes.extend_from_slice(&frame.payload);
        Ok(self.bytes.len() == frame.total)
    }

    /// Expect the next report once this one is delivered and acknowledged.
    fn next(&mut self) -> BenchResult<()> {
        self.bytes.clear();
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| bench_error("control sequence overflow"))?;
        Ok(())
    }
}

fn deliver(
    kind: Kind,
    payload: &[u8],
    input: &mut Option<mpsc::Sender<BenchResult<Value>>>,
    monitor: &Monitor,
) -> BenchResult<()> {
    match kind {
        Kind::Data => {
            let value = serde_json::from_slice(payload)?;
            input
                .as_ref()
                .ok_or_else(|| bench_error("command after shutdown"))?
                .try_send(Ok(value))
                .map_err(|_| bench_error("benchmark control receive queue full or closed"))?;
        }
        Kind::Shutdown => {
            input.take();
        }
        Kind::Finished => monitor.stop(),
        Kind::Failed => {
            let cause: String = serde_json::from_slice(payload)?;
            let cause = format!("benchmark worker failed: {cause}");
            monitor.report_failure(cause.clone());
            if let Some(input) = input {
                // The monitor retains the cause even if a full report queue
                // or completed shutdown has no receiver.
                let _ = input.try_send(Err(bench_error(cause)));
            }
        }
        Kind::Ack => return Err(bench_error("control acknowledgment carried a report")),
    }
    Ok(())
}
