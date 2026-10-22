//! Static broker facts of a Broker reply.

use super::{Broker, PartitionName, count, list, read_text, text};
use crate::{GroupId, NodeId, data::CodecError, layout::Reader};

pub(super) fn encode_broker(out: &mut Vec<u8>, broker: &Broker) -> Result<(), CodecError> {
    text(out, &broker.name)?;
    out.extend_from_slice(broker.id.as_bytes());
    out.extend_from_slice(&broker.pid.to_be_bytes());
    out.extend_from_slice(&broker.uptime_ms.to_be_bytes());
    out.extend_from_slice(&broker.max_clients.to_be_bytes());
    out.extend_from_slice(&broker.shards.to_be_bytes());
    count(out, broker.brokers.len())?;
    for (name, id) in &broker.brokers {
        text(out, name)?;
        out.extend_from_slice(id.as_bytes());
    }
    count(out, broker.partitions.len())?;
    for partition in &broker.partitions {
        out.extend_from_slice(partition.group.as_bytes());
        text(out, &partition.topic)?;
        out.extend_from_slice(&partition.partition.to_be_bytes());
        out.extend_from_slice(&partition.shard.to_be_bytes());
    }
    Ok(())
}

pub(super) fn decode_broker(reader: &mut Reader<'_>) -> Result<Broker, CodecError> {
    Ok(Broker {
        name: read_text(reader)?,
        id: NodeId::from_bytes(reader.array()?),
        pid: reader.u32()?,
        uptime_ms: reader.u64()?,
        max_clients: reader.u32()?,
        shards: reader.u32()?,
        brokers: list(reader, |reader| {
            Ok((read_text(reader)?, NodeId::from_bytes(reader.array()?)))
        })?,
        partitions: list(reader, |reader| {
            Ok(PartitionName {
                group: GroupId::from_bytes(reader.array()?),
                topic: read_text(reader)?,
                partition: reader.u32()?,
                shard: reader.u32()?,
            })
        })?,
    })
}
