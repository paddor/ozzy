//! Thread-local allocation accounting for synchronous production actor rounds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountedAllocator;

#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

fn count() {
    // Worker and other test threads remain outside the measured actor round.
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Some(value) = counter.get() {
            counter.set(Some(value.saturating_add(1)));
        }
    });
}

// SAFETY: All operations forward unchanged pointers/layouts to System. The
// allocation-free thread-local counter neither reads nor changes user memory.
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
        // SAFETY: The caller supplies the original System allocation/layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the original allocation and valid new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

pub(super) fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|counter| counter.set(None));
        }
    }
    ALLOCATIONS.with(|counter| assert!(counter.replace(Some(0)).is_none()));
    let reset = Reset;
    let output = operation();
    let allocations = ALLOCATIONS.with(|counter| counter.get().unwrap());
    drop(reset);
    (output, allocations)
}
