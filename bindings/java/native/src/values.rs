use crate::state::{Handle, Value};
use jni::{
    JNIEnv,
    objects::{JObject, JValue},
};
use ozzy::{AppendDisposition, Policy, SharedTopicReceipt, TopicRecord};

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
    words[13] = u64::from(r.disposition == AppendDisposition::Duplicate);
    words[14] = match r.policy {
        Policy::LocalDurable => 1,
        Policy::QuorumDurable => 2,
    };
    words
}
pub(crate) fn pending(handle: &Handle) -> [u64; 16] {
    let mut words = [0; 16];
    if let Handle::Pending(pending) = handle {
        words[..2].copy_from_slice(&uuid(pending.topic().as_bytes()));
        words[2] = u64::from(pending.partition());
        words[3] = pending.sequence();
    }
    words
}

pub(crate) fn deliver(
    env: &mut JNIEnv<'_>,
    future: &JObject<'_>,
    result: Result<Value, String>,
) -> jni::errors::Result<()> {
    let mut pointer = 0_i64;
    let (kind, words, payload, error) = match result {
        Ok(Value::Unit) => (0, [0; 16], None, None),
        Ok(Value::Handle(handle)) => {
            let kind = match handle.as_ref() {
                Handle::Client(_) => 1,
                Handle::Producer(_) => 2,
                Handle::Consumer(_) => 3,
                Handle::Pending(_) => 4,
            };
            let words = pending(&handle);
            (kind, words, Some(Ok(handle)), None)
        }
        Ok(Value::Receipt(value)) => (5, receipt(&value), None, None),
        Ok(Value::Record(value)) => (6, record(&value), Some(Err(value)), None),
        Err(error) => (0, [0; 16], None, Some(error)),
    };
    let data = env.new_long_array(16)?;
    env.set_long_array_region(&data, 0, &words.map(u64::cast_signed))?;
    let count = match &payload {
        Some(Err(record)) => record.payload.len(),
        _ => 0,
    };
    let parts = env.new_object_array(jsize(count)?, "[B", JObject::null())?;
    if let Some(Err(record)) = &payload {
        for (i, part) in record.payload.iter().enumerate() {
            let bytes = env.byte_array_from_slice(part)?;
            env.set_object_array_element(&parts, jsize(i)?, &bytes)?;
            env.delete_local_ref(bytes)?;
        }
    }
    let error = error.map(|s| env.new_string(s)).transpose()?;
    let error = error.map_or(JObject::null(), JObject::from);
    if let Some(Ok(handle)) = payload {
        pointer = Box::into_raw(handle) as i64;
    }
    // completeNative consumes the handle, including cancellation and construction errors.
    env.call_method(
        future,
        "completeNative",
        "(IJ[J[[BLjava/lang/String;)V",
        &[
            JValue::Int(kind),
            JValue::Long(pointer),
            JValue::Object(data.as_ref()),
            JValue::Object(parts.as_ref()),
            JValue::Object(&error),
        ],
    )?;
    Ok(())
}

fn jsize(value: usize) -> jni::errors::Result<i32> {
    i32::try_from(value)
        .map_err(|_| jni::errors::Error::JniCall(jni::errors::JniError::InvalidArguments))
}
