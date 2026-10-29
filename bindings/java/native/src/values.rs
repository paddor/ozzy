use crate::state::{Handle, Value};
use jni::{
    JNIEnv,
    objects::{JObject, JValue},
};
use ozzy::{AppendDisposition, Policy, SharedTopicPendingRecord, SharedTopicReceipt, TopicRecord};
use std::sync::Arc;

pub(crate) fn uuid(bytes: &[u8; 16]) -> [u64; 2] {
    let (a, b) = bytes.split_at(8);
    // Fixed halves of a UUID are always eight bytes.
    [
        a.iter().fold(0, |n, b| (n << 8) | u64::from(*b)),
        b.iter().fold(0, |n, b| (n << 8) | u64::from(*b)),
    ]
}
pub(crate) fn record(record: &TopicRecord) -> [u64; 16] {
    let mut words = [0; 16];
    words[..2].copy_from_slice(&uuid(record.topic.as_bytes()));
    words[2] = u64::from(record.partition);
    words[3..5].copy_from_slice(&uuid(record.incarnation.as_bytes()));
    words[5] = record.offset.get();
    words[6..8].copy_from_slice(&uuid(record.message_id.as_bytes()));
    words[8] = record.payload.len() as u64;
    words
}
pub(crate) fn receipt(receipt: &SharedTopicReceipt) -> [u64; 16] {
    let mut words = [0; 16];
    words[..2].copy_from_slice(&uuid(receipt.topic.as_bytes()));
    words[2] = u64::from(receipt.partition);
    let r = &receipt.record;
    words[3..5].copy_from_slice(&uuid(r.partition.as_bytes()));
    words[5] = r.offset;
    words[6..8].copy_from_slice(&uuid(r.message_id.as_bytes()));
    words[8..10].copy_from_slice(&uuid(r.key.producer_id.as_bytes()));
    words[10] = r.key.producer_epoch;
    words[11] = r.owner_epoch;
    words[12] = r.key.first_sequence;
    words[13] = match r.disposition {
        AppendDisposition::Duplicate => 1,
        AppendDisposition::Scheduled => 2,
        _ => 0,
    };
    words[14] = match r.policy {
        Policy::LocalDurable => 1,
        Policy::QuorumDurable => 2,
    };
    words
}
pub(crate) fn pending(pending: &SharedTopicPendingRecord) -> [u64; 16] {
    let mut words = [0; 16];
    words[..2].copy_from_slice(&uuid(pending.topic().as_bytes()));
    words[2] = u64::from(pending.partition());
    words[3] = pending.sequence();
    words
}

pub(crate) fn deliver(
    env: &mut JNIEnv<'_>,
    future: &JObject<'_>,
    result: Result<Value, String>,
) -> jni::errors::Result<()> {
    // Kinds 1-3 carry an owned box, 4 one strong observer count; Java owns
    // either pointer once completeNative runs, including its failures.
    let (kind, words, owned, record, error) = match result {
        Ok(Value::Unit) => (0, None, None, None, None),
        Ok(Value::Handle(handle)) => {
            let kind = match handle.as_ref() {
                Handle::Client(_) => 1,
                Handle::Producer(_) => 2,
                Handle::Consumer(_) => 3,
            };
            (kind, None, Some(Owned::Handle(handle)), None, None)
        }
        Ok(Value::Pending(value)) => (
            4,
            Some(pending(&value)),
            Some(Owned::Pending(value)),
            None,
            None,
        ),
        Ok(Value::Receipt(value)) => (5, Some(receipt(&value)), None, None, None),
        Ok(Value::Record(value)) => (6, Some(record(&value)), None, Some(value), None),
        Err(error) => (0, None, None, None, Some(error)),
    };
    let data = words.map_or(Ok(JObject::null()), |words| metadata(env, words))?;
    let parts = record
        .as_ref()
        .map_or(Ok(JObject::null()), |record| payload(env, record))?;
    let error = error.map(|s| env.new_string(s)).transpose()?;
    let error = error.map_or(JObject::null(), JObject::from);
    let pointer = match owned {
        Some(Owned::Handle(handle)) => Box::into_raw(handle) as i64,
        Some(Owned::Pending(pending)) => Arc::into_raw(pending) as i64,
        None => 0,
    };
    env.call_method(
        future,
        "completeNative",
        "(IJ[J[[BLjava/lang/String;)V",
        &[
            JValue::Int(kind),
            JValue::Long(pointer),
            JValue::Object(&data),
            JValue::Object(&parts),
            JValue::Object(&error),
        ],
    )?;
    Ok(())
}

enum Owned {
    Handle(Box<Handle>),
    Pending(Arc<SharedTopicPendingRecord>),
}

fn metadata<'local>(
    env: &mut JNIEnv<'local>,
    words: [u64; 16],
) -> jni::errors::Result<JObject<'local>> {
    let data = env.new_long_array(16)?;
    env.set_long_array_region(&data, 0, &words.map(u64::cast_signed))?;
    Ok(JObject::from(data))
}

fn payload<'local>(
    env: &mut JNIEnv<'local>,
    record: &TopicRecord,
) -> jni::errors::Result<JObject<'local>> {
    let parts = env.new_object_array(jsize(record.payload.len())?, "[B", JObject::null())?;
    for (i, part) in record.payload.iter().enumerate() {
        let bytes = env.byte_array_from_slice(part)?;
        env.set_object_array_element(&parts, jsize(i)?, &bytes)?;
        env.delete_local_ref(bytes)?;
    }
    Ok(JObject::from(parts))
}

fn jsize(value: usize) -> jni::errors::Result<i32> {
    i32::try_from(value)
        .map_err(|_| jni::errors::Error::JniCall(jni::errors::JniError::InvalidArguments))
}
