//! Run and worker fencing plus fixed-size control message chunks.

use super::{BenchResult, Bytes, CHUNK, MAX_REPORT_BYTES, Message, Uuid, bench_error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Kind {
    Data = 1,
    Shutdown = 2,
    Finished = 3,
    Ack = 4,
    Failed = 5,
}

#[derive(Clone, Copy)]
pub(super) struct Route {
    pub(super) run: Uuid,
    pub(super) worker: Uuid,
    pub(super) peer: [u8; 16],
}

pub(super) struct Frame {
    pub(super) kind: Kind,
    pub(super) sequence: u64,
    pub(super) offset: usize,
    pub(super) total: usize,
    pub(super) payload: Bytes,
}

impl Frame {
    /// One chunk of report `sequence`, starting at `offset` of `total` bytes.
    pub(super) fn chunk(
        kind: Kind,
        sequence: u64,
        offset: usize,
        total: usize,
        payload: &[u8],
    ) -> Self {
        Self {
            kind,
            sequence,
            offset,
            total,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    /// The acknowledgment of report `sequence`.
    pub(super) fn ack(sequence: u64) -> Self {
        Self {
            kind: Kind::Ack,
            sequence,
            offset: 0,
            total: 0,
            payload: Bytes::new(),
        }
    }

    /// Address the frame to `route.peer`, fenced by the run and worker.
    pub(super) fn encode(self, route: Route) -> Message {
        let Route { run, worker, peer } = route;
        let mut header = Vec::with_capacity(50);
        header.extend_from_slice(run.as_bytes());
        header.extend_from_slice(worker.as_bytes());
        header.extend_from_slice(&[1, self.kind as u8]);
        header.extend_from_slice(&self.sequence.to_be_bytes());
        header.extend_from_slice(&(self.offset as u32).to_be_bytes());
        header.extend_from_slice(&(self.total as u32).to_be_bytes());
        Message::multipart([
            Bytes::copy_from_slice(&peer),
            Bytes::from(header),
            self.payload,
        ])
    }

    pub(super) fn decode(message: &Message, route: Route) -> BenchResult<Self> {
        let Route { run, worker, peer } = route;
        let mut parts = message.iter();
        let route = parts
            .next()
            .ok_or_else(|| bench_error("missing control route"))?;
        let bytes = parts
            .next()
            .ok_or_else(|| bench_error("missing control header"))?;
        let payload = parts
            .next()
            .ok_or_else(|| bench_error("missing control payload"))?;
        let addressed_to_us = parts.next().is_none()
            && route.as_ref() == peer
            && bytes.len() == 50
            && &bytes[..16] == run.as_bytes()
            && &bytes[16..32] == worker.as_bytes()
            && bytes[32] == 1;
        if !addressed_to_us {
            return Err(bench_error("invalid benchmark control identity or version"));
        }
        let kind = match bytes[33] {
            1 => Kind::Data,
            2 => Kind::Shutdown,
            3 => Kind::Finished,
            4 => Kind::Ack,
            5 => Kind::Failed,
            _ => return Err(bench_error("unknown benchmark control kind")),
        };
        let sequence = u64::from_be_bytes(bytes[34..42].try_into()?);
        let offset = u32::from_be_bytes(bytes[42..46].try_into()?) as usize;
        let total = u32::from_be_bytes(bytes[46..50].try_into()?) as usize;
        let chunk_in_report = offset
            .checked_add(payload.len())
            .is_some_and(|end| end <= total);
        let shaped_for_kind = if kind == Kind::Ack {
            offset == 0 && total == 0
        } else {
            total != 0 && !payload.is_empty()
        };
        let bounded = sequence != 0
            && total <= MAX_REPORT_BYTES
            && payload.len() <= CHUNK
            && chunk_in_report
            && shaped_for_kind;
        if !bounded {
            return Err(bench_error("invalid benchmark control frame bounds"));
        }
        Ok(Self {
            kind,
            sequence,
            offset,
            total,
            payload,
        })
    }
}
