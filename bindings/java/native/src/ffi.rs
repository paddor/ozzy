//! Unsafe boundary only. Java owns exclusive mutable leases and reachability fences.
//! Handles are opaque Box pointers. Input/output views live for the downcall;
//! record payload views live until the next exclusive consumer call.
#![allow(non_snake_case)]
#![allow(unsafe_code)]
use crate::{
    execution::{self, Lease},
    input::{self, Inputs},
    state::{Handle, Request, text},
    values,
};
use jni::{
    JNIEnv,
    objects::{JByteArray, JClass, JLongArray, JObject, JObjectArray, JString},
    sys::{jbyteArray, jlong, jlongArray},
};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    task::{Context, Poll, Waker},
};
#[cfg(not(target_pointer_width = "64"))]
compile_error!("Ozzy Java requires a 64-bit JVM");

fn boundary<T: Default>(
    env: &mut JNIEnv<'_>,
    work: impl FnOnce(&mut JNIEnv<'_>) -> Result<T, String>,
) -> T {
    match catch_unwind(AssertUnwindSafe(|| work(env))) {
        Ok(Ok(value)) => value,
        result => {
            let message = match result {
                Ok(Err(e)) => e,
                _ => "native adapter panicked".into(),
            };
            if !env.exception_check().unwrap_or(true) {
                let _ = env.throw_new("io/ozzy/OzzyException", message);
            }
            T::default()
        }
    }
}

#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_open(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
    future: JObject<'_>,
    endpoints: JObjectArray<'_>,
    ids: JLongArray<'_>,
    settings: JLongArray<'_>,
) -> jlong {
    boundary(&mut env, |env| {
        let config = input::config(env, &endpoints, &ids, &settings)?;
        let future = env.new_global_ref(future).map_err(text)?;
        execution::start(
            Lease::new(env, future, None)?,
            Request::Connect(Box::new(config)),
        )
    })
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_start(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
    pointer: jlong,
    op: i32,
    future: JObject<'_>,
    topic: JString<'_>,
    options: JLongArray<'_>,
    parts: JObjectArray<'_>,
    key: JByteArray<'_>,
    id: JLongArray<'_>,
    identity: JByteArray<'_>,
) -> jlong {
    boundary(&mut env, |env| {
        let request = if op == 6 {
            // SAFETY: Pending is immutable; Java keeps it reachable through this call.
            match unsafe { &*(pointer as *const Handle) } {
                Handle::Pending(value) => Request::Confirm(value.clone()),
                _ => return Err("not a pending record".into()),
            }
        } else {
            input::request(
                env,
                op,
                &Inputs {
                    topic,
                    options,
                    parts,
                    key,
                    id,
                    identity,
                },
            )?
        };
        let future = env.new_global_ref(future).map_err(text)?;
        // SAFETY: Java's atomic lease excludes close, GC, and other mutable calls.
        let handle = if op == 6 {
            None
        } else {
            Some(unsafe { Box::from_raw(pointer as *mut Handle) })
        };
        execution::start(Lease::new(env, future, handle)?, request)
    })
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_cancel(
    _: JNIEnv<'_>,
    _: JClass<'_>,
    pointer: jlong,
) {
    if pointer != 0 {
        // SAFETY: NativeFuture exchanges its token with zero exactly once.
        unsafe { Box::from_raw(pointer as *mut tokio::task::AbortHandle) }.abort();
    }
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_release(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
    pointer: jlong,
) {
    boundary(&mut env, |_| {
        if pointer != 0 {
            // SAFETY: Java consumes the pointer once after exclusive operations release it.
            drop(unsafe { Box::from_raw(pointer as *mut Handle) });
        }
        Ok(())
    });
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_addresses(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
) -> jlongArray {
    boundary(&mut env, |env| {
        let addresses = [
            try_send as *const () as i64,
            try_next as *const () as i64,
            try_confirmed as *const () as i64,
        ];
        let array = env.new_long_array(3).map_err(text)?;
        env.set_long_array_region(&array, 0, &addresses)
            .map_err(text)?;
        Ok(array.into_raw())
    })
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_inspect(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
    pointer: jlong,
    kind: i32,
) -> jlongArray {
    boundary(&mut env, |env| {
        // SAFETY: Exclusive Java lease spans this immutable inspection.
        let handle = unsafe { &*(pointer as *const Handle) };
        let words = match (handle, kind) {
            (Handle::Producer(Some(writer)), 0) => {
                vec![i64::try_from(writer.metadata().partition_count()).map_err(text)?]
            }
            (Handle::Consumer(c), 1) => {
                let checkpoint = c.reader.checkpoint();
                let mut words = values::uuid(checkpoint.topic.as_bytes())
                    .map(u64::cast_signed)
                    .to_vec();
                for (part, offset) in checkpoint.positions {
                    words.extend([i64::from(part), offset.get().cast_signed()]);
                }
                words
            }
            (Handle::Consumer(c), 2) => {
                let s = c.reader.stats();
                vec![
                    s.live_records.cast_signed(),
                    s.replayed_records.cast_signed(),
                    s.live_gaps.cast_signed(),
                ]
            }
            _ => return Err("closed or incompatible native handle".into()),
        };
        let array = env
            .new_long_array(i32::try_from(words.len()).map_err(text)?)
            .map_err(text)?;
        env.set_long_array_region(&array, 0, &words).map_err(text)?;
        Ok(array.into_raw())
    })
}
#[unsafe(no_mangle)]
pub(crate) extern "system" fn Java_io_ozzy_Native_identity(
    mut env: JNIEnv<'_>,
    _: JClass<'_>,
    pointer: jlong,
) -> jbyteArray {
    boundary(&mut env, |env| {
        // SAFETY: Exclusive Java lease spans this inspection.
        let Handle::Producer(Some(writer)) = (unsafe { &*(pointer as *const Handle) }) else {
            return Err("producer is closed".into());
        };
        env.byte_array_from_slice(&writer.identity().to_bytes())
            .map(jni::objects::JPrimitiveArray::into_raw)
            .map_err(text)
    })
}

fn fast(output: *mut [u64; 16], work: impl FnOnce() -> Result<i32, String>) -> i32 {
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(Ok(value)) => value,
        error => {
            let message = match error {
                Ok(Err(message)) => message,
                _ => "native scalar operation panicked".into(),
            };
            let mut count = message.len().min(124);
            while !message.is_char_boundary(count) {
                count -= 1;
            }
            // SAFETY: Each private scalar entry receives an aligned 128-byte output.
            // Copy into caller-owned storage: virtual threads may change OS threads.
            unsafe {
                output.cast::<u32>().write(count as u32);
                std::ptr::copy_nonoverlapping(message.as_ptr(), output.cast::<u8>().add(4), count);
            }
            -1
        }
    }
}
unsafe fn slice<'a>(pointer: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: Java's heap segment remains pinned for this critical downcall.
        unsafe { std::slice::from_raw_parts(pointer, len) }
    }
}
unsafe extern "C" fn try_send(
    pointer: i64,
    body: *const u8,
    len: usize,
    key: *const u8,
    key_len: usize,
    route: i32,
    hi: u64,
    lo: u64,
    generate: i32,
    output: *mut [u64; 16],
) -> i32 {
    fast(output, || {
        // SAFETY: Java holds the exclusive lease and both input segments are pinned.
        let (handle, body, key) = unsafe {
            (
                &mut *(pointer as *mut Handle),
                slice(body, len),
                slice(key, key_len),
            )
        };
        let Handle::Producer(Some(writer)) = handle else {
            return Err("producer is closed".into());
        };
        if generate == 0 && hi == 0 && lo == 0 {
            return Err("message ID must be nonzero".into());
        }
        let id = if generate != 0 {
            ozzy::MessageId::new()
        } else {
            ozzy::MessageId::from_bytes(input::id(hi, lo))
        };
        let record = ozzy::RecordInput::copy_from_slice(id, body);
        let key = (route == -2).then_some(key);
        let mut work = std::pin::pin!(crate::state::send(
            writer,
            record,
            key,
            (route >= 0).then_some(route as u32)
        ));
        match work.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Pending => Ok(0),
            Poll::Ready(result) => {
                let handle = Box::new(Handle::Pending(std::sync::Arc::new(result?)));
                let mut words = values::pending(&handle);
                words[4] = Box::into_raw(handle) as u64;
                // SAFETY: Output is a live aligned 128-byte Java native segment.
                unsafe {
                    output.write(words);
                }
                Ok(1)
            }
        }
    })
}
unsafe extern "C" fn try_next(pointer: i64, output: *mut [u64; 16]) -> i32 {
    fast(output, || {
        // SAFETY: Java holds an exclusive lease until payload copies finish.
        let Handle::Consumer(consumer) = (unsafe { &mut *(pointer as *mut Handle) }) else {
            return Err("not a consumer".into());
        };
        consumer.last = None;
        if let Some(record) = consumer.reader.try_next().map_err(text)? {
            consumer.parts.clear();
            consumer.parts.extend(
                record
                    .payload
                    .iter()
                    .map(|p| [p.as_ptr() as u64, p.len() as u64]),
            );
            let mut words = values::record(&record);
            words[9] = consumer.parts.as_ptr() as u64;
            consumer.last = Some(record);
            // SAFETY: Output is aligned and live; payloads remain owned by consumer.last.
            unsafe {
                output.write(words);
            }
            Ok(1)
        } else {
            Ok(0)
        }
    })
}
unsafe extern "C" fn try_confirmed(pointer: i64, output: *mut [u64; 16]) -> i32 {
    fast(output, || {
        // SAFETY: Immutable pending observers are Sync; Java fences reachability.
        let Handle::Pending(pending) = (unsafe { &*(pointer as *const Handle) }) else {
            return Err("not a pending record".into());
        };
        match pending.try_confirmed() {
            Some(result) => {
                let words = values::receipt(&result.map_err(text)?);
                // SAFETY: Output belongs exclusively to this Java caller.
                unsafe {
                    output.write(words);
                }
                Ok(1)
            }
            None => Ok(0),
        }
    })
}
