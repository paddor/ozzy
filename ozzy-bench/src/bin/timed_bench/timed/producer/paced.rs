//! Native fixed-rate admission. Confirmations are observed even with a sparse
//! window; cancellation never discards an admitted confirmation or a pending send.

use super::super::pacing::{self, Arrivals, Plan};
use super::{
    Config, LaneRun, PendingRecord, RecordInput, Result, VecDeque, Window, Writer, error, metrics,
    turn_records,
};
use std::task::Poll;

struct Pending {
    record: PendingRecord,
    sequence: u64,
    start: u64,
    due: u64,
}

/// One lane's arrivals and, for redundant feeds, the earliest copy's plan.
struct Schedule {
    arrivals: Arrivals,
    /// The earliest redundant copy's plan, which fixes every copy's event clock.
    canonical: Option<Plan>,
}

impl Schedule {
    fn new(config: &Config, window: Window, lane: usize) -> Result<Self> {
        let plan = Plan::new(config, window, lane)?
            .ok_or_else(|| error("paced writer lane has no schedule"))?;
        let canonical = config
            .args
            .native
            .feeds
            .enabled()
            .then(|| {
                Plan::new(
                    config,
                    window,
                    config.args.native.feeds.primary(config.logical_lane(lane)),
                )
            })
            .transpose()?
            .flatten();
        Ok(Self {
            arrivals: Arrivals::new(&plan),
            canonical,
        })
    }
}

pub(super) async fn produce(
    writer: &mut Writer,
    run: &mut LaneRun<'_>,
    window: Window,
) -> Result<()> {
    let config = run.lane.config;
    let mut schedule = Schedule::new(config, window, run.lane.index)?;
    let mut timer = pacing::Timer::new()?;
    run.counts
        .enable_schedule(schedule.arrivals.plan().bounds());
    let mut pending = VecDeque::with_capacity(config.args.request_records);
    let mut scratch = Vec::with_capacity(RecordInput::INLINE_BYTES);
    let turn_records = turn_records(config);
    while !schedule.arrivals.exhausted(run.sequence) {
        let due = schedule.arrivals.plan().due(run.sequence)?;
        let now = metrics::monotonic_ns();
        if schedule
            .arrivals
            .lower_on_backlog(&mut run.counts, run.sequence, || now)?
        {
            continue;
        }
        if now < due {
            wait_until(&mut timer, due, &mut pending, run).await?;
            continue;
        }
        let start = now;
        // All redundant occurrences contain the same original event clock and
        // bytes, independently generated from the earliest copy's schedule.
        let event_clock = schedule
            .canonical
            .map_or(Ok(start), |plan| plan.due(run.sequence))?;
        let input = run
            .lane
            .logical_input(run.sequence, event_clock, &mut scratch)?;
        let input = run.hold(input, metrics::unix_ns);
        let Some(body) = input.parts().next() else {
            return Err(error("paced record has no payload part"));
        };
        run.counts.record_bytes(input.message_id, body);
        let record = admit(writer, input, &mut pending, run).await?;
        run.counts.submission(start, 1);
        pending.push_back(Pending {
            record,
            sequence: run.sequence,
            start,
            due,
        });
        run.sequence += 1;
        if run.sequence.is_multiple_of(turn_records) {
            tokio::task::yield_now().await;
        }
        // The next arrival checks its backlog at the top of the loop.
    }
    while !pending.is_empty() {
        observe_front(&mut pending, run).await?;
    }
    Ok(())
}

/// Sleep until `due`, completing confirmations that arrive meanwhile.
async fn wait_until(
    timer: &mut pacing::Timer,
    due: u64,
    pending: &mut VecDeque<Pending>,
    run: &mut LaneRun<'_>,
) -> Result<()> {
    let release = timer.wait_until(due);
    tokio::pin!(release);
    loop {
        tokio::select! {
            biased;
            result = observe_front(pending, run), if !pending.is_empty() => result?,
            result = &mut release => return result,
        }
    }
}

/// Send `input`, completing confirmations while admission waits. Retain this
/// exact send across confirmation observation: dropping a future before
/// admission is allowed by the SDK but loses this arrival.
async fn admit(
    writer: &mut Writer,
    input: RecordInput,
    pending: &mut VecDeque<Pending>,
    run: &mut LaneRun<'_>,
) -> Result<PendingRecord> {
    // Complete ready confirmations without registering a waiter. Admission
    // is usually immediate; only a waiting send observes confirmations.
    if pending
        .front()
        .is_some_and(|first| first.record.try_confirmed().is_some())
    {
        observe_front(pending, run).await?;
    }
    let send = writer.send(input);
    tokio::pin!(send);
    if let Poll::Ready(result) =
        std::future::poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await
    {
        return result;
    }
    loop {
        tokio::select! {
            biased;
            result = observe_front(pending, run), if !pending.is_empty() => result?,
            result = &mut send => return result,
        }
    }
}

async fn observe_front(pending: &mut VecDeque<Pending>, run: &mut LaneRun<'_>) -> Result<()> {
    let Some(first) = pending.front() else {
        return Ok(());
    };
    let mut receipt = Some(first.record.confirmed().await);
    let finished = metrics::monotonic_ns();
    // One APPEND confirms many records together. Read the clock once and use
    // ready receipts directly, as in saturation, instead of selecting per record.
    for _ in 0..turn_records(run.lane.config) {
        let (Some(confirmed), Some(first)) = (receipt, pending.front()) else {
            return Ok(());
        };
        run.lane
            .validate_receipt(&confirmed?, first.sequence, &mut run.check)?;
        run.counts
            .scheduled_complete(first.due, first.start, finished)?;
        pending.pop_front();
        receipt = pending
            .front()
            .and_then(|first| first.record.try_confirmed());
    }
    tokio::task::yield_now().await;
    Ok(())
}
