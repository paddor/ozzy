//! Native fixed-rate admission. Confirmations are observed even with a sparse
//! window; cancellation never discards an admitted confirmation or a pending send.

use super::super::pacing::{self, Plan};
use super::{
    Config, Counts, GroupId, PendingRecord, ReceiptCheck, RecordInput, Result, VecDeque, Window,
    Writer, metrics, record_input, turn_records, validate_receipt,
};

struct Pending {
    record: PendingRecord,
    sequence: u64,
    start: u64,
    due: u64,
}

pub(super) async fn produce(
    writer: &mut Writer,
    config: &Config,
    group: GroupId,
    lane: usize,
    window: Window,
    counts: &mut Counts,
    sequence: &mut u64,
) -> Result<ReceiptCheck> {
    let plan = Plan::new(config, window, lane)?.expect("scheduled workload");
    let mut timer = pacing::Timer::new()?;
    counts.enable_schedule(plan.bounds());
    let mut check = writer.receipt_check();
    let mut pending = VecDeque::with_capacity(config.args.request_records);
    let mut scratch = Vec::with_capacity(RecordInput::INLINE_BYTES);
    let turn_records = turn_records(config);
    // An overloaded ramp lowers the limit to the arrivals of earlier stages.
    let mut limit = plan.total();
    while *sequence < limit {
        let due = plan.due(*sequence)?;
        let now = metrics::monotonic_ns();
        if limit == plan.total()
            && let Some(stage) = plan.check_backlog(*sequence, now)?
        {
            counts.overload(stage);
            limit = plan.total_before(stage)?;
            continue;
        }
        if now < due {
            let release = timer.wait_until(due);
            tokio::pin!(release);
            loop {
                tokio::select! {
                    biased;
                    result = observe_front(&mut pending, counts, config, group, lane, &mut check), if !pending.is_empty() => result?,
                    result = &mut release => { result?; break; },
                }
            }
            continue;
        }
        let start = now;
        let input = record_input(config, group, lane, *sequence, start, &mut scratch)?;
        counts.record_bytes(
            input.message_id,
            input.parts().next().expect("single-part workload"),
        );
        // Retain this exact send across confirmation observation. Dropping a
        // future before admission is allowed by the SDK but loses this arrival.
        let send = writer.send(input);
        tokio::pin!(send);
        let record = loop {
            tokio::select! {
                biased;
                result = observe_front(&mut pending, counts, config, group, lane, &mut check), if !pending.is_empty() => result?,
                result = &mut send => break result?,
            }
        };
        counts.submission(start, 1);
        pending.push_back(Pending {
            record,
            sequence: *sequence,
            start,
            due,
        });
        *sequence += 1;
        if sequence.is_multiple_of(turn_records) {
            tokio::task::yield_now().await;
        }
        if limit == plan.total()
            && let Some(stage) = plan.check_backlog(*sequence, metrics::monotonic_ns())?
        {
            counts.overload(stage);
            limit = plan.total_before(stage)?;
        }
    }
    while !pending.is_empty() {
        observe_front(&mut pending, counts, config, group, lane, &mut check).await?;
    }
    Ok(check)
}

async fn observe_front(
    pending: &mut VecDeque<Pending>,
    counts: &mut Counts,
    config: &Config,
    group: GroupId,
    lane: usize,
    check: &mut ReceiptCheck,
) -> Result<()> {
    let first = pending.front().expect("nonempty confirmation queue");
    let mut receipt = Some(first.record.confirmed().await);
    let finished = metrics::monotonic_ns();
    // One APPEND confirms many records together. Read the clock once and use
    // ready receipts directly, as in saturation, instead of selecting per record.
    for _ in 0..turn_records(config) {
        let Some(confirmed) = receipt else {
            return Ok(());
        };
        let first = pending.front().expect("nonempty confirmation queue");
        validate_receipt(&confirmed?, first.sequence, config, group, lane, check)?;
        counts.scheduled_complete(first.due, first.start, finished)?;
        pending.pop_front();
        receipt = pending
            .front()
            .and_then(|first| first.record.try_confirmed());
    }
    tokio::task::yield_now().await;
    Ok(())
}
