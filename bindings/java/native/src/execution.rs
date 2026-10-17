//! Scheduling only: SDK futures own all admission, routing, and retry state.
use crate::{
    state::{Handle, Request},
    values,
};
use futures::FutureExt;
use jni::{JNIEnv, JavaVM, objects::GlobalRef};
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
    task::{Context, Poll, Waker},
};
use tokio::runtime::Runtime;

static EXECUTOR: OnceLock<Result<Runtime, String>> = OnceLock::new();

/// A moved owner is restored before Java can observe completion or cancellation.
/// Construct this guard eagerly, including before the task's first poll.
pub(crate) struct Lease {
    pub(crate) handle: Option<Box<Handle>>,
    future: GlobalRef,
    vm: Arc<JavaVM>,
    released: bool,
}
impl Lease {
    pub(crate) fn new(
        env: &JNIEnv<'_>,
        future: GlobalRef,
        handle: Option<Box<Handle>>,
    ) -> Result<Self, String> {
        let vm = match env.get_java_vm() {
            Ok(vm) => vm,
            Err(error) => {
                if let Some(handle) = handle {
                    let _ = Box::into_raw(handle);
                }
                return Err(error.to_string());
            }
        };
        Ok(Self {
            handle,
            future,
            vm: Arc::new(vm),
            released: false,
        })
    }
    fn restore(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = Box::into_raw(handle);
        }
    }
    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.restore();
        if let Ok(mut env) = self.vm.attach_current_thread_as_daemon() {
            let _ = env.call_method(self.future.as_obj(), "released", "()V", &[]);
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) fn start(mut lease: Lease, request: Request) -> Result<i64, String> {
    let executor = EXECUTOR
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("ozzy/java")
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)?;
    let mut work = Box::pin(async move {
        let result = AssertUnwindSafe(crate::state::run(&mut lease.handle, request))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("native SDK task panicked".into()));
        lease.restore();
        if let Ok(mut env) = lease.vm.attach_current_thread_as_daemon()
            && let Err(error) = env.with_local_frame(64, |env| {
                values::deliver(env, lease.future.as_obj(), result)
            })
        {
            let _ = env.exception_clear();
            if let Ok(message) = env.new_string(error.to_string()) {
                let _ = env.call_method(
                    lease.future.as_obj(),
                    "failed",
                    "(Ljava/lang/String;)V",
                    &[jni::objects::JValue::Object(message.as_ref())],
                );
            }
        }
        lease.release();
    });
    match work.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(()) => Ok(0),
        Poll::Pending => {
            let task = executor.spawn(work);
            Ok(Box::into_raw(Box::new(task.abort_handle())) as i64)
        }
    }
}
