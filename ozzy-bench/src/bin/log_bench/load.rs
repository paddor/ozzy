//! The writer: appends at the next offset, a window of them in flight.
//! Records arrive at the offered rate; an append takes every record that
//! arrived while the window was full, up to the batch bound.

use super::{Options, Report, Result};
use bytes::Bytes;
use ozzy_bench::measure::cpu;
use ozzy_broker::log::{AppendError, Appended, Log, LogRecord, Pending, SubmitError};
use ozzy_proto::{MessageId, Offset};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// An append in flight: its first record's number and its record count.
struct Sent {
    first: u64,
    count: u64,
    at: Instant,
    pending: Pending<Appended, AppendError>,
}

struct Writer<'a> {
    options: &'a Options,
    log: &'a Log,
    view: u64,
    /// Offset of record 0.
    base: u64,
    /// Records submitted so far.
    next: u64,
    started: Instant,
    payload: Bytes,
    sent: VecDeque<Sent>,
}

impl Writer<'_> {
    /// When record `number` arrives. At once without an offered rate.
    fn arrival(&self, number: u64) -> Option<Instant> {
        (self.options.rate > 0).then(|| {
            self.started + Duration::from_secs_f64(number as f64 / self.options.rate as f64)
        })
    }

    /// Records that arrived and are not submitted, up to one batch.
    fn ready(&self) -> u64 {
        let batch = self.options.batch as u64;
        if self.options.rate == 0 {
            return batch;
        }
        let arrived = self.started.elapsed().as_secs_f64() * self.options.rate as f64;
        (arrived as u64).saturating_sub(self.next).min(batch)
    }

    /// Queue one append of the records that are ready. `false` when none
    /// is, the window is full or the log's queue is.
    fn submit(&mut self) -> Result<bool> {
        let count = self.ready();
        if count == 0 || self.sent.len() >= self.options.window {
            return Ok(false);
        }
        let first = self.next;
        let records = (first..first + count)
            .map(|number| LogRecord {
                id: MessageId::from_bytes((u128::from(number) + 1).to_le_bytes()),
                parts: std::iter::once(self.payload.clone()).collect(),
            })
            .collect();
        let expected = Offset::new(self.base + first);
        let pending = match self.log.append(self.view, expected, records) {
            Ok(pending) => pending,
            Err(SubmitError::Busy) if !self.sent.is_empty() => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        self.sent.push_back(Sent {
            first,
            count,
            at: Instant::now(),
            pending,
        });
        self.next += count;
        Ok(true)
    }

    /// Wait for the oldest append, or for the next record when the window
    /// has room. `Ok(None)` once the log is full.
    async fn wait(&mut self) -> Result<Option<Option<Sent>>> {
        let next = self.arrival(self.next);
        let room = self.sent.len() < self.options.window;
        let Some(oldest) = self.sent.front_mut() else {
            if let Some(next) = next {
                tokio::time::sleep_until(next.into()).await;
            }
            return Ok(Some(None));
        };
        let arrives = async {
            match next {
                Some(next) if room => tokio::time::sleep_until(next.into()).await,
                _ => std::future::pending().await,
            }
        };
        let result = tokio::select! {
            biased;
            result = &mut oldest.pending => result,
            () = arrives => return Ok(Some(None)),
        };
        match result {
            Ok(_) => Ok(Some(self.sent.pop_front())),
            Err(AppendError::Full) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Confirmation latency of every record of `sent`, in microseconds.
    fn latencies(&self, sent: &Sent, micros: &mut Vec<u32>) {
        let now = Instant::now();
        micros.extend((sent.first..sent.first + sent.count).map(|number| {
            let arrived = self.arrival(number).unwrap_or(sent.at);
            now.saturating_duration_since(arrived).as_micros() as u32
        }));
    }
}

pub(crate) async fn run(options: &Options, log: &Log) -> Result<Report> {
    let status = *log.status().borrow();
    let mut writer = Writer {
        options,
        log,
        view: status.view,
        base: status.committed_end.get(),
        next: 0,
        started: Instant::now(),
        payload: Bytes::from(vec![0x5A; options.size]),
        sent: VecDeque::with_capacity(options.window),
    };
    let warm = writer.started + Duration::from_secs(options.warmup);
    let end = warm + Duration::from_secs(options.seconds);
    let mut before = None;
    let (mut micros, mut appends, mut full) = (Vec::new(), 0, false);
    while Instant::now() < end && !full {
        while writer.submit()? {}
        match writer.wait().await? {
            Some(Some(sent)) if sent.at >= warm => {
                before = before.or_else(|| Some(cpu()));
                writer.latencies(&sent, &mut micros);
                appends += 1;
            }
            Some(_) => {}
            None => full = true,
        }
    }
    let after = cpu();
    Ok(Report {
        elapsed: Instant::now().saturating_duration_since(warm),
        appends,
        records: micros.len() as u64,
        micros,
        cpu: after.saturating_sub(before.unwrap_or(after)),
        full,
    })
}
