use crate::state::{Open, Request, text};
use jni::{
    JNIEnv,
    objects::{JByteArray, JLongArray, JObjectArray, JString},
};
use ozzy::{
    BrokerAddress, MessageId, NodeId, Offset, ProducerIdentity, ReaderStart, RecordInput,
    TopicCheckpoint, TopicId,
};
use std::time::Duration;

pub(crate) fn longs(env: &JNIEnv<'_>, values: &JLongArray<'_>) -> Result<Vec<i64>, String> {
    if values.is_null() {
        return Ok(Vec::new());
    }
    let mut result = vec![0; env.get_array_length(values).map_err(text)? as usize];
    env.get_long_array_region(values, 0, &mut result)
        .map_err(text)?;
    Ok(result)
}
pub(crate) fn id(hi: u64, lo: u64) -> [u8; 16] {
    let mut id = [0; 16];
    id[..8].copy_from_slice(&hi.to_be_bytes());
    id[8..].copy_from_slice(&lo.to_be_bytes());
    id
}
pub(crate) fn config(
    env: &mut JNIEnv<'_>,
    endpoints: &JObjectArray<'_>,
    ids: &JLongArray<'_>,
    settings: &JLongArray<'_>,
) -> Result<ozzy_bindings::Config, String> {
    let ids = longs(env, ids)?;
    let settings = longs(env, settings)?;
    if settings.len() != 12
        || ids.is_empty()
        || !ids.len().is_multiple_of(2)
        || env.get_array_length(endpoints).map_err(text)? as usize != ids.len()
    {
        return Err("invalid client configuration shape".into());
    }
    let mut brokers = Vec::new();
    for (i, pair) in ids.as_chunks::<2>().0.iter().enumerate() {
        let peer: JString<'_> = env
            .get_object_array_element(endpoints, i32::try_from(2 * i).map_err(text)?)
            .map_err(text)?
            .into();
        let data: JString<'_> = env
            .get_object_array_element(endpoints, i32::try_from(2 * i + 1).map_err(text)?)
            .map_err(text)?
            .into();
        let peer: String = env.get_string(&peer).map_err(text)?.into();
        let data: String = env.get_string(&data).map_err(text)?.into();
        brokers.push(BrokerAddress {
            node: NodeId::from_bytes(id(pair[0] as u64, pair[1] as u64)),
            endpoint: peer.parse().map_err(text)?,
            data_endpoint: data.parse().map_err(text)?,
        });
    }
    let mut bounds = [0; 6];
    let mut capacity = [0; 4];
    for (out, value) in bounds
        .iter_mut()
        .chain(capacity.iter_mut())
        .zip(&settings[..10])
    {
        *out = usize::try_from(*value).map_err(text)?;
    }
    let timeout = u64::try_from(settings[10]).map_err(text)?;
    ozzy_bindings::Config::new(
        brokers,
        bounds,
        capacity,
        Duration::from_nanos(timeout),
        settings[11] != 0,
    )
}
pub(crate) struct Inputs<'a> {
    pub(crate) topic: JString<'a>,
    pub(crate) options: JLongArray<'a>,
    pub(crate) parts: JObjectArray<'a>,
    pub(crate) key: JByteArray<'a>,
    pub(crate) id: JLongArray<'a>,
    pub(crate) identity: JByteArray<'a>,
}
/// `open` carries the client's shared links for producer and consumer opens.
pub(crate) fn request(
    env: &mut JNIEnv<'_>,
    op: i32,
    input: &Inputs<'_>,
    open: Option<Open>,
) -> Result<Request, String> {
    let opts = longs(env, &input.options)?;
    let open = || open.ok_or("not a client");
    match op {
        1 => {
            let identity = if input.identity.is_null() {
                None
            } else {
                let bytes = env
                    .convert_byte_array(&input.identity)
                    .map_err(text)?
                    .try_into()
                    .map_err(|_| "identity must be 32 bytes")?;
                Some(ProducerIdentity::from_bytes(bytes).map_err(text)?)
            };
            let takeover = opts.first() == Some(&1);
            if takeover && identity.is_none() {
                return Err("takeover requires identity".into());
            }
            Ok(Request::Producer(
                open()?,
                env.get_string(&input.topic).map_err(text)?.into(),
                identity,
                takeover,
            ))
        }
        2 => {
            let (start, partitions) = reader(&opts)?;
            Ok(Request::Consumer(
                open()?,
                env.get_string(&input.topic).map_err(text)?.into(),
                start,
                partitions,
            ))
        }
        3 => send(env, input, &opts),
        5 => Ok(Request::Next),
        7 => Ok(Request::Close),
        _ => Err("unknown operation".into()),
    }
}
fn reader(opts: &[i64]) -> Result<(ReaderStart, Option<Vec<u32>>), String> {
    let mut values = opts.iter().copied();
    let kind = values.next().ok_or("missing reader start")?;
    let count = values.next().ok_or("missing partition selection")?;
    let partitions = if count < 0 {
        None
    } else {
        let count = usize::try_from(count).map_err(text)?;
        if count > opts.len() {
            return Err("invalid partition selection".into());
        }
        Some(
            values
                .by_ref()
                .take(count)
                .map(|v| u32::try_from(v).map_err(text))
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let start = match kind {
        0 => ReaderStart::Earliest,
        1 => ReaderStart::Latest,
        2 => {
            let hi = values.next().ok_or("missing checkpoint topic")? as u64;
            let lo = values.next().ok_or("missing checkpoint topic")? as u64;
            let remaining: Vec<_> = values.collect();
            if !remaining.len().is_multiple_of(2) {
                return Err("invalid checkpoint".into());
            }
            let positions = remaining
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| Ok((u32::try_from(p[0]).map_err(text)?, Offset::new(p[1] as u64))))
                .collect::<Result<Vec<_>, String>>()?;
            ReaderStart::Checkpoint(TopicCheckpoint {
                topic: TopicId::from_bytes(id(hi, lo)),
                positions,
            })
        }
        _ => return Err("unknown reader start".into()),
    };
    Ok((start, partitions))
}

fn send(env: &mut JNIEnv<'_>, input: &Inputs<'_>, opts: &[i64]) -> Result<Request, String> {
    let route = *opts.first().ok_or("missing route")?;
    let ids = longs(env, &input.id)?;
    let id = match ids.as_slice() {
        [] => MessageId::new(),
        [hi, lo] if *hi != 0 || *lo != 0 => MessageId::from_bytes(id(*hi as u64, *lo as u64)),
        _ => return Err("message ID must be a nonzero UUID".into()),
    };
    let count = env.get_array_length(&input.parts).map_err(text)?;
    if count <= 0 {
        return Err("record needs at least one part".into());
    }
    let mut parts = Vec::new();
    for i in 0..count {
        let part: JByteArray<'_> = env
            .get_object_array_element(&input.parts, i)
            .map_err(text)?
            .into();
        parts.push(bytes::Bytes::from(
            env.convert_byte_array(&part).map_err(text)?,
        ));
        env.delete_local_ref(part).map_err(text)?;
    }
    let record = RecordInput::multipart(id, parts);
    let key = if route == -2 {
        Some(env.convert_byte_array(&input.key).map_err(text)?)
    } else {
        None
    };
    Ok(Request::Send(
        record,
        key,
        (route >= 0).then_some(route as u32),
    ))
}
