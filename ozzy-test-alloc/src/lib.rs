//! Allocation budget tests. Linking this crate installs a global allocator
//! that counts allocations on the measuring thread only; other test threads
//! stay outside every measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountedAllocator;

#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

fn count() {
    // Ignore other threads and allocations during thread-local teardown.
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Some(value) = counter.get() {
            counter.set(Some(value.saturating_add(1)));
        }
    });
}

// SAFETY: Every operation forwards its unchanged pointer/layout to System.
// Accounting uses allocation-free thread-local cells and never touches memory.
unsafe impl GlobalAlloc for CountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the allocator's required valid layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the allocator's required valid layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: All allocations came from System with the same layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: The caller supplies System's original allocation and valid size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

/// Run `operation` and count the allocations it makes on this thread,
/// reallocations included.
///
/// # Panics
///
/// Panics when called inside another measurement on the same thread.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|counter| counter.set(None));
        }
    }
    ALLOCATIONS.with(|counter| assert!(counter.replace(Some(0)).is_none(), "nested measurement"));
    let reset = Reset;
    let output = operation();
    let allocations = ALLOCATIONS.with(Cell::get).unwrap_or_default();
    drop(reset);
    (output, allocations)
}
