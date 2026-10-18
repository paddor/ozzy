//! Real inproc brokers and the shipped Java API in one embedded JVM.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines
)]
use jni::{
    InitArgsBuilder, JNIVersion, JavaVM, NativeMethod,
    objects::{JObject, JValue},
};
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::{Cluster, Control};
use std::sync::{Arc, Mutex, OnceLock};
type Fixture = Option<(ozzy::Context, Vec<Arc<Control>>)>;
static FIXTURE: Mutex<Fixture> = Mutex::new(None);
static RUN: Mutex<()> = Mutex::new(());
static JVM: OnceLock<JavaVM> = OnceLock::new();
pub(crate) fn context() -> Option<ozzy::Context> {
    FIXTURE.lock().unwrap().as_ref().map(|f| f.0.clone())
}
extern "system" fn hold(_: jni::JNIEnv<'_>, _: jni::objects::JClass<'_>, held: jni::sys::jboolean) {
    if let Some((_, controls)) = FIXTURE.lock().unwrap().as_ref() {
        for control in controls {
            control.hold_record_writes(held != 0);
        }
    }
}
extern "system" fn held(_: jni::JNIEnv<'_>, _: jni::objects::JClass<'_>) -> i32 {
    FIXTURE.lock().unwrap().as_ref().map_or(0, |(_, c)| {
        c.iter()
            .map(|c| i32::try_from(c.held_writes()).unwrap())
            .sum()
    })
}
fn register(env: &mut jni::JNIEnv<'_>) {
    use crate::ffi::*;
    macro_rules! method {
        ($name:literal, $sig:literal, $f:ident) => {
            NativeMethod {
                name: $name.into(),
                sig: $sig.into(),
                fn_ptr: $f as *mut std::ffi::c_void,
            }
        };
    }
    env.register_native_methods(
        "io/ozzy/Native",
        &[
            method!(
                "open",
                "(Lio/ozzy/NativeFuture;[Ljava/lang/String;[J[J)J",
                Java_io_ozzy_Native_open
            ),
            method!(
                "start",
                "(JILio/ozzy/NativeFuture;Ljava/lang/String;[J[[B[B[J[B)J",
                Java_io_ozzy_Native_start
            ),
            method!("cancel", "(J)V", Java_io_ozzy_Native_cancel),
            method!("release", "(J)V", Java_io_ozzy_Native_release),
            method!("addresses", "()[J", Java_io_ozzy_Native_addresses),
            method!("inspect", "(JI)[J", Java_io_ozzy_Native_inspect),
            method!("identity", "(J)[B", Java_io_ozzy_Native_identity),
        ],
    )
    .unwrap();
    env.register_native_methods(
        "io/ozzy/Harness",
        &[
            method!("holdWrites", "(Z)V", hold),
            method!("heldWrites", "()I", held),
        ],
    )
    .unwrap();
}
fn scenario(mode: DeploymentMode, codec: &str, asynchronous: bool, case: &str) {
    let _exclusive = RUN.lock().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster = runtime.block_on(Cluster::configured(mode, None, |d| {
        d.topics.get_mut("orders").unwrap().deduplication =
            (case == "identities").then_some(ozzy_config::TopicDeduplication {
                window_millis: 300_000,
                max_entries: 128,
                max_bytes: 4 * 1024 * 1024,
            });
    }));
    *FIXTURE.lock().unwrap() = Some((cluster.runtime.context().clone(), cluster.controls.clone()));
    let vm = JVM.get_or_init(|| {
        JavaVM::new(
            InitArgsBuilder::new()
                .version(JNIVersion::V8)
                .option(format!(
                    "-Djava.class.path={}",
                    env!("OZZY_JAVA_TEST_CLASSES")
                ))
                .option("-Dozzy.native.registered=true")
                .option("--enable-native-access=ALL-UNNAMED")
                .option("-Xcheck:jni")
                .option("-Xmx256m")
                .option("-XX:+UseSerialGC")
                .option("-XX:ActiveProcessorCount=2")
                .option("-Djava.util.concurrent.ForkJoinPool.common.parallelism=1")
                .build()
                .unwrap(),
        )
        .unwrap()
    });
    let mut env = vm.attach_current_thread().unwrap();
    register(&mut env);
    let addresses = env
        .new_object_array(
            i32::try_from(cluster.configs.len() * 3).unwrap(),
            "java/lang/String",
            JObject::null(),
        )
        .unwrap();
    for (i, (checked, identity)) in cluster.configs.iter().enumerate() {
        let endpoints = &checked.deployment.deployment().brokers[&checked.plan.name].endpoints;
        for (j, value) in [
            identity.broker.to_string(),
            endpoints.peer.clone(),
            endpoints.data_peer.clone(),
        ]
        .iter()
        .enumerate()
        {
            let value = env.new_string(value).unwrap();
            env.set_object_array_element(&addresses, i32::try_from(i * 3 + j).unwrap(), value)
                .unwrap();
        }
    }
    let codec = env.new_string(codec).unwrap();
    let case = env.new_string(case).unwrap();
    let passed = env
        .call_static_method(
            "io/ozzy/Scenarios",
            "run",
            "([Ljava/lang/String;Ljava/lang/String;ZLjava/lang/String;)V",
            &[
                JValue::Object(addresses.as_ref()),
                JValue::Object(codec.as_ref()),
                JValue::Bool(u8::from(asynchronous)),
                JValue::Object(case.as_ref()),
            ],
        )
        .is_ok();
    if !passed {
        env.exception_describe().unwrap();
        env.exception_clear().unwrap();
    }
    for c in &cluster.controls {
        c.hold_record_writes(false);
    }
    *FIXTURE.lock().unwrap() = None;
    runtime.block_on(cluster.shutdown());
    assert!(passed, "Java SDK scenario failed");
}
macro_rules! cases {
    ($mode:expr, $codec:expr, $asynchronous:expr; $($case:ident),+ $(,)?) => { $(
        #[test] fn $case() { scenario($mode, $codec, $asynchronous, stringify!($case)); }
    )+ };
}
macro_rules! matrix {
    ($mode:expr) => {
        mod blocking_raw { use super::*; cases!($mode, "raw", false; roundtrip, payloads, routing, checkpoints, identities, validation, lifetimes, mixed); }
        mod blocking_lz4 { use super::*; cases!($mode, "lz4", false; roundtrip, payloads, routing, checkpoints, identities, validation, lifetimes, mixed); }
        mod async_raw { use super::*; cases!($mode, "raw", true; roundtrip, payloads, routing, checkpoints, identities, validation, lifetimes, mixed, cancellation, pressure, observers, callbacks, cancel_race, completion_race, close_cancel, concurrency); }
        mod async_lz4 { use super::*; cases!($mode, "lz4", true; roundtrip, payloads, routing, checkpoints, identities, validation, lifetimes, mixed, cancellation, pressure, observers, callbacks, cancel_race, completion_race, close_cancel, concurrency); }
    };
}
mod single {
    use super::*;
    matrix!(DeploymentMode::Single);
}
mod quorum {
    use super::*;
    matrix!(DeploymentMode::Three);
}
