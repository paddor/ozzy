//! Ordered, acknowledged control transport. Runs on the application runtime.

use super::{
    AtomicBool, BenchResult, CHUNK, DEADLINE, Frame, Kind, MAX_REPORT_BYTES, Ordering, Outgoing,
    Route, Socket, Value, bench_error, mpsc,
};
use std::sync::Mutex;

pub(super) async fn pump(
    socket: &Socket,
    route: Route,
    mut outgoing: mpsc::Receiver<Outgoing>,
    incoming: mpsc::Sender<BenchResult<Value>>,
    live: &AtomicBool,
    failure: &Mutex<Option<String>>,
) -> BenchResult<()> {
    let mut monitor = socket.monitor();
    socket
        .wait_connected(1, DEADLINE)
        .await
        .map_err(|cause| bench_error(format!("control handshake: {cause}")))?;
    let (acks, mut acknowledged) = mpsc::channel(1);
    let send = async {
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
                let message = route.packet(out.kind, sequence, index * CHUNK, bytes.len(), chunk);
                tokio::time::timeout(DEADLINE, socket.send(message))
                    .await
                    .map_err(|cause| {
                        bench_error(format!("control send sequence={sequence}: {cause}"))
                    })??;
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
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let receive = async {
        let mut sequence = 1_u64;
        let mut assembly = Vec::new();
        let mut expected_kind = Kind::Data;
        let mut expected_total = 0;
        let mut input = Some(incoming);
        loop {
            let message = tokio::select! {
                message = socket.recv() => message?,
                event = monitor.recv() => {
                    if let omq_tokio::MonitorEvent::Disconnected { reason, .. } = event? {
                        return Err::<(), _>(bench_error(format!("benchmark control peer disconnected: {reason:?}")));
                    }
                    continue;
                }
            };
            let frame = Frame::decode(&message, route)?;
            if frame.kind == Kind::Ack {
                acks.try_send(frame.sequence)
                    .map_err(|_| bench_error("unexpected control acknowledgment"))?;
                continue;
            }
            if frame.sequence != sequence || frame.offset != assembly.len() {
                return Err(bench_error("stale or discontinuous control frame"));
            }
            if assembly.is_empty() {
                expected_kind = frame.kind;
                expected_total = frame.total;
            }
            if frame.kind != expected_kind || frame.total != expected_total {
                return Err(bench_error("control report changed during transfer"));
            }
            assembly.extend_from_slice(&frame.payload);
            if assembly.len() != frame.total {
                continue;
            }
            deliver(frame.kind, &assembly, &mut input, live, failure)?;
            assembly.clear();
            let ack = route.packet(Kind::Ack, sequence, 0, 0, &[]);
            tokio::time::timeout(DEADLINE, socket.send(ack))
                .await
                .map_err(|cause| {
                    bench_error(format!(
                        "control reply acknowledgment sequence={sequence}: {cause}"
                    ))
                })??;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| bench_error("control sequence overflow"))?;
        }
    };
    tokio::try_join!(send, receive)?;
    Ok(())
}

fn deliver(
    kind: Kind,
    payload: &[u8],
    input: &mut Option<mpsc::Sender<BenchResult<Value>>>,
    live: &AtomicBool,
    failure: &Mutex<Option<String>>,
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
        Kind::Finished => {
            live.store(false, Ordering::Release);
        }
        Kind::Failed => {
            let cause: String = serde_json::from_slice(payload)?;
            let cause = format!("benchmark worker failed: {cause}");
            *failure.lock().expect("control failure") = Some(cause.clone());
            if let Some(input) = input {
                // The monitor retains the cause even if a full report queue
                // or completed shutdown has no receiver.
                let _ = input.try_send(Err(bench_error(cause)));
            }
        }
        Kind::Ack => unreachable!(),
    }
    Ok(())
}
