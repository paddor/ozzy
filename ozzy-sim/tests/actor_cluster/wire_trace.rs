//! `OZZY_ACTOR_SIM_WIRE` prints each replica message of group 0 as its
//! broker receives it: election, installation transfer, flow setup, commit,
//! and receipts. Diagnostic only; it reads frames and changes no schedule.

use crate::broker::GROUPS;
use omq_tokio::Message;
use ozzy_proto::{EnvelopeLimits, GroupId, Opcode};
use std::sync::OnceLock;

/// Group, epoch, view, voter, and configuration digest.
const SCOPE_BYTES: usize = 80;
const PREFIX_BYTES: usize = 40;

pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("OZZY_ACTOR_SIM_WIRE").is_some())
}

/// A routed message `from` delivered to broker `to`'s shard for `group`.
pub(crate) fn receive(
    tick: u64,
    from: usize,
    to: usize,
    group: GroupId,
    message: &Message,
    limits: EnvelopeLimits,
) {
    if !enabled() || group != GROUPS[0] {
        return;
    }
    eprintln!("wire {tick} {from}->{to} {}", describe(message, limits));
}

fn describe(message: &Message, limits: EnvelopeLimits) -> String {
    if message.len() != 4 {
        return "RECEIPT".into();
    }
    let frames: [&[u8]; 3] =
        std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
    let Ok(packet) = ozzy_proto::decode_packet(&frames, limits) else {
        return "undecodable".into();
    };
    let envelope = packet.envelope;
    let meta = Fields(packet.metadata);
    let view = meta.u64(24);
    let rest = SCOPE_BYTES;
    let request = envelope
        .request_id
        .map_or(String::new(), |id| format!(" req={id:?}"));
    let body = match envelope.opcode {
        Opcode::DoViewChange => format!(
            "gen={} last_normal={} accepted={} committed={}",
            meta.short(rest, 16),
            meta.u64(rest + 16),
            meta.op(rest + 24),
            meta.op(rest + 24 + PREFIX_BYTES),
        ),
        Opcode::StartView => format!(
            "gen={} accepted={} committed={}",
            meta.short(rest, 16),
            meta.op(rest + 16),
            meta.op(rest + 16 + PREFIX_BYTES),
        ),
        Opcode::Commit => format!("committed={}", meta.op(rest)),
        Opcode::FetchOps => format!(
            "source={} gen={} accepted={} after={} max={}",
            meta.voter(rest),
            meta.short(rest + 16, 16),
            meta.op(rest + 32),
            meta.op(rest + 32 + PREFIX_BYTES),
            meta.u32(rest + 32 + 2 * PREFIX_BYTES),
        ),
        Opcode::Ops => format!(
            "source={} accepted={} after={} bytes={}",
            meta.voter(rest),
            meta.op(rest + 32),
            meta.op(rest + 32 + PREFIX_BYTES),
            packet.payload.len(),
        ),
        Opcode::ReplicaOpen => format!(
            "tail={} available={}",
            meta.op(rest),
            meta.u64(rest + PREFIX_BYTES)
        ),
        Opcode::ReplicaState => format!(
            "epoch={} revision={} base={} received={} repair_limit={}",
            meta.short(rest, 16),
            meta.u64(rest + 16),
            meta.op(rest + 24),
            meta.op(rest + 24 + PREFIX_BYTES),
            match meta.u64(rest + 32 + 2 * PREFIX_BYTES) {
                u64::MAX => "none".into(),
                op => op.to_string(),
            },
        ),
        Opcode::PrepareFlow => format!(
            "first={} committed={} bytes={}",
            meta.u64(rest + 16),
            meta.op(rest + 56),
            packet.payload.len()
        ),
        Opcode::PreparePub | Opcode::Prepare => format!(
            "first={} committed={} bytes={}",
            meta.u64(rest),
            meta.op(rest + 40),
            packet.payload.len()
        ),
        Opcode::PrepareOk => format!("durable={}", meta.op(rest)),
        _ => String::new(),
    };
    let response = if envelope.response { " response" } else { "" };
    format!("{:?} v{view}{response}{request} {body}", envelope.opcode)
}

/// Bounds-checked big-endian reads; a short frame prints `?`.
struct Fields<'a>(&'a [u8]);

impl Fields<'_> {
    fn get<const N: usize>(&self, at: usize) -> Option<[u8; N]> {
        self.0.get(at..at + N)?.try_into().ok()
    }

    fn u64(&self, at: usize) -> u64 {
        self.get(at).map_or(u64::MAX, u64::from_be_bytes)
    }

    fn u32(&self, at: usize) -> String {
        self.get(at)
            .map_or("?".into(), |bytes| u32::from_be_bytes(bytes).to_string())
    }

    /// The op number of a prefix; its digest is left out.
    fn op(&self, at: usize) -> String {
        self.get(at)
            .map_or("?".into(), |bytes| u64::from_be_bytes(bytes).to_string())
    }

    /// Simulated node IDs repeat `broker + 1` in every byte.
    fn voter(&self, at: usize) -> String {
        self.get::<16>(at)
            .map_or("?".into(), |bytes| (i32::from(bytes[0]) - 1).to_string())
    }

    /// The low bytes of a long identity, enough to tell generations apart.
    fn short(&self, at: usize, len: usize) -> String {
        self.0.get(at..at + len).map_or("?".into(), |bytes| {
            let [a, b, c] = [bytes[len - 3], bytes[len - 2], bytes[len - 1]];
            format!("{a:02x}{b:02x}{c:02x}")
        })
    }
}
