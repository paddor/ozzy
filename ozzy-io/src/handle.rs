use std::any::Any;
use std::fmt;
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::task::Waker;

/// A reference to backend-owned state, never a file descriptor. Dropping the
/// last reference only wakes its backend. It performs no file close or wait.
#[derive(Clone, Debug)]
pub struct Handle(Arc<Lease>);

#[derive(Debug)]
struct Lease {
    owner: Arc<()>,
    key: u64,
    direct_io: bool,
    wake: Waker,
    resource: OnceLock<Weak<dyn Any + Send + Sync>>,
    live: AtomicBool,
}

impl Handle {
    /// Whether the backend opened this handle for direct I/O. No descriptor is exposed.
    pub fn is_direct(&self) -> bool {
        self.0.direct_io
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.wake.wake_by_ref();
    }
}

/// Backend-side identity. Handles from another backend cannot resolve here.
#[derive(Clone, Debug, Default)]
pub struct HandleOwner(Arc<()>);

impl HandleOwner {
    /// Keys must never be reused by this owner, even after closing a file.
    pub fn create(&self, key: u64, wake: Waker) -> (Handle, HandleToken) {
        self.create_with_direct_io(key, wake, false)
    }

    /// Immutable routing hint. The backend still validates the handle and write.
    pub fn create_with_direct_io(
        &self,
        key: u64,
        wake: Waker,
        direct_io: bool,
    ) -> (Handle, HandleToken) {
        let lease = Arc::new(Lease {
            owner: Arc::clone(&self.0),
            key,
            direct_io,
            wake,
            resource: OnceLock::new(),
            live: AtomicBool::new(true),
        });
        let token = HandleToken(Arc::downgrade(&lease));
        (Handle(lease), token)
    }

    /// Return a handle key only when this backend owns it.
    pub fn key(&self, handle: &Handle) -> Option<u64> {
        Arc::ptr_eq(&self.0, &handle.0.owner).then_some(handle.0.key)
    }

    /// Bind a backend resource without transferring its ownership to the
    /// application handle. Only backend workers may upgrade this weak link.
    pub fn bind<T: Any + Send + Sync>(&self, handle: &Handle, resource: &Arc<T>) {
        assert!(self.key(handle).is_some(), "foreign handle binding");
        let erased: Arc<dyn Any + Send + Sync> = resource.clone();
        handle
            .0
            .resource
            .set(Arc::downgrade(&erased))
            .expect("handle resource bound once");
    }

    /// Resolve a still-open backend resource. The second live check fences a
    /// close that raced with weak-reference upgrade.
    pub fn resource<T: Any + Send + Sync>(&self, handle: &Handle) -> Option<Arc<T>> {
        self.key(handle)?;
        if !handle.0.live.load(Ordering::Acquire) {
            return None;
        }
        let resource = handle.0.resource.get()?.upgrade()?.downcast().ok()?;
        handle.0.live.load(Ordering::Acquire).then_some(resource)
    }

    /// Fence new resolutions before the backend drops its strong resource.
    pub fn invalidate(&self, handle: &Handle) {
        assert!(self.key(handle).is_some(), "foreign handle invalidation");
        handle.0.live.store(false, Ordering::Release);
    }
}

/// Weak backend-side lifetime observation. Queued and running operations must
/// retain a strong `Handle` until physical completion.
pub struct HandleToken(Weak<Lease>);

impl HandleToken {
    /// Whether an application, queued job, or running job still retains the handle.
    pub fn is_alive(&self) -> bool {
        self.0.strong_count() != 0
    }
}

impl fmt::Debug for HandleToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleToken")
            .field("alive", &self.is_alive())
            .finish()
    }
}
