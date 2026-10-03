//! Private commands carry bytes, never typed Rust values or pointers.

use super::retention::{QueueSlot, Retention};
use super::{Command, PortError, ReplyError, RouteError, RouteState, ServiceError};
use crate::dispatch::Class;
use crate::frontend::PublicationError;
use bytes::Bytes;
use omq_tokio::{Message, message::Payload};
use ozzy_proto::{Envelope, EnvelopeLimits, LinkSessionId, Opcode, RequestId, directory};

const HEADER_BYTES: usize = 26;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Key {
    pub(super) generation: RequestId,
    pub(super) id: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Status {
    Reply(Result<(), ReplyError>),
    Publication(Result<(), PublicationError>),
    Route(RouteStatus),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum RouteStatus {
    Unchanged,
    Changed,
    Destination,
    Watch,
}

impl Status {
    pub(super) fn kind(self) -> u8 {
        match self {
            Self::Reply(_) => 0,
            Self::Publication(_) => 1,
            Self::Route(_) => 2,
        }
    }
}

pub(super) enum Action {
    Reply(Class, Message),
    Publication(Message),
    Route(RouteState),
}
pub(super) struct Incoming {
    pub(super) identity: Bytes,
    pub(super) key: Key,
    pub(super) action: Action,
}

struct Header {
    bytes: [u8; HEADER_BYTES],
    _slot: QueueSlot,
}
impl AsRef<[u8]> for Header {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) fn command(
    key: Key,
    class: Class,
    command: &Command,
    slot: QueueSlot,
    retention: &Retention,
) -> Message {
    let mut header = [0; HEADER_BYTES];
    header[..16].copy_from_slice(key.generation.as_bytes());
    header[16..24].copy_from_slice(&key.id.to_be_bytes());
    header[24] = command.kind();
    header[25] = u8::from(class == Class::Control);
    let mut body = match command {
        Command::Reply { message, .. } | Command::Publication { message, .. } => {
            retention.message(message.clone())
        }
        Command::Route { route, .. } => route_message(route),
    };
    let header = Payload::from_bytes_with_retained_size(
        Bytes::from_owner(Header {
            bytes: header,
            _slot: slot,
        }),
        super::COMMAND_BYTES,
    );
    Message::multipart_payloads(
        std::iter::once(header).chain(std::iter::from_fn(|| body.pop_front_payload())),
    )
}

pub(super) fn decode(
    mut message: Message,
    generation: RequestId,
    class: Class,
) -> Result<Incoming, PortError> {
    let identity = message
        .pop_front_payload()
        .ok_or(PortError::Frames)?
        .as_bytes();
    let header = message.pop_front_payload().ok_or(PortError::Frames)?;
    let bytes: [u8; HEADER_BYTES] = header
        .as_slice()
        .try_into()
        .map_err(|_| PortError::Frames)?;
    let key = Key {
        generation: RequestId::from_bytes(bytes[..16].try_into().unwrap()),
        id: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
    };
    if key.generation != generation
        || key.id == 0
        || bytes[25] != u8::from(class == Class::Control)
        || !matches!(
            (bytes[24], class),
            (0 | 1, Class::Data) | (0 | 2, Class::Control)
        )
    {
        return Err(PortError::Frames);
    }
    let action = match bytes[24] {
        0 if matches!(message.len(), 2 | 4) => Action::Reply(class, message),
        1 if message.len() == 4 => Action::Publication(message),
        2 => Action::Route(decode_route(&message)?),
        _ => return Err(PortError::Frames),
    };
    Ok(Incoming {
        identity,
        key,
        action,
    })
}

fn route_message(route: &RouteState) -> Message {
    let limits = EnvelopeLimits {
        max_metadata_bytes: 1024,
        max_payload_bytes: 0,
    };
    let mut metadata = Vec::with_capacity(256);
    let header = directory::encode_update(
        Envelope {
            opcode: Opcode::StateUpdate,
            response: false,
            request_id: None,
            sender: route.members[0],
            session: Some(LinkSessionId::from_bytes([1; 16])),
        },
        &directory::Update {
            watch: RequestId::from_bytes([1; 16]),
            route: route.clone(),
        },
        &mut metadata,
        limits,
        directory::Limits::default(),
    )
    .expect("validated route");
    Message::multipart([
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ])
}

fn decode_route(message: &Message) -> Result<RouteState, PortError> {
    let limits = EnvelopeLimits {
        max_metadata_bytes: 1024,
        max_payload_bytes: 0,
    };
    let parts = [
        message.part_slice(0).ok_or(PortError::Frames)?,
        message.part_slice(1).ok_or(PortError::Frames)?,
        message.part_slice(2).ok_or(PortError::Frames)?,
    ];
    if message.len() != 3 {
        return Err(PortError::Frames);
    }
    let packet = ozzy_proto::decode_packet(&parts, limits).map_err(|_| PortError::Frames)?;
    directory::decode_update(packet, limits, directory::Limits::default())
        .map(|update| update.route)
        .map_err(|_| PortError::Frames)
}

pub(super) fn completion(key: Key, status: Status) -> Message {
    let mut bytes = [0; 26];
    bytes[..16].copy_from_slice(key.generation.as_bytes());
    bytes[16..24].copy_from_slice(&key.id.to_be_bytes());
    bytes[24] = status.kind();
    bytes[25] = match status {
        Status::Reply(Ok(()))
        | Status::Publication(Ok(()))
        | Status::Route(RouteStatus::Unchanged) => 0,
        Status::Reply(Err(error)) => match error {
            ReplyError::Peer => 1,
            ReplyError::Frames => 2,
            ReplyError::Session => 3,
            ReplyError::Class => 4,
            ReplyError::Size => 5,
            ReplyError::Full => 6,
        },
        Status::Publication(Err(error)) => match error {
            PublicationError::Source => 1,
            PublicationError::Destination => 2,
            PublicationError::Stale => 3,
            PublicationError::Full => 4,
        },
        Status::Route(RouteStatus::Changed) => 1,
        Status::Route(RouteStatus::Destination) => 2,
        Status::Route(RouteStatus::Watch) => 3,
    };
    Message::single(Bytes::copy_from_slice(&bytes))
}

pub(super) fn decode_completion(message: &Message) -> Result<(Key, Status), PortError> {
    if message.len() != 1 {
        return Err(PortError::Frames);
    }
    let bytes: [u8; 26] = message
        .part_slice(0)
        .ok_or(PortError::Frames)?
        .try_into()
        .map_err(|_| PortError::Frames)?;
    let key = Key {
        generation: RequestId::from_bytes(bytes[..16].try_into().unwrap()),
        id: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
    };
    let status = match (bytes[24], bytes[25]) {
        (0, 0) => Status::Reply(Ok(())),
        (0, 1) => Status::Reply(Err(ReplyError::Peer)),
        (0, 2) => Status::Reply(Err(ReplyError::Frames)),
        (0, 3) => Status::Reply(Err(ReplyError::Session)),
        (0, 4) => Status::Reply(Err(ReplyError::Class)),
        (0, 5) => Status::Reply(Err(ReplyError::Size)),
        (0, 6) => Status::Reply(Err(ReplyError::Full)),
        (1, 0) => Status::Publication(Ok(())),
        (1, 1) => Status::Publication(Err(PublicationError::Source)),
        (1, 2) => Status::Publication(Err(PublicationError::Destination)),
        (1, 3) => Status::Publication(Err(PublicationError::Stale)),
        (1, 4) => Status::Publication(Err(PublicationError::Full)),
        (2, 0) => Status::Route(RouteStatus::Unchanged),
        (2, 1) => Status::Route(RouteStatus::Changed),
        (2, 2) => Status::Route(RouteStatus::Destination),
        (2, 3) => Status::Route(RouteStatus::Watch),
        _ => return Err(PortError::Frames),
    };
    Ok((key, status))
}

pub(super) fn route_result(result: RouteStatus) -> super::RouteResult {
    match result {
        RouteStatus::Unchanged => Ok(false),
        RouteStatus::Changed => Ok(true),
        RouteStatus::Destination => Err(RouteError::Destination),
        RouteStatus::Watch => Err(RouteError::Watch(ServiceError::Watch)),
    }
}
