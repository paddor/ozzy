//! Count allocations on the current test thread only.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

struct Counted;

#[global_allocator]
static ALLOCATOR: Counted = Counted;

fn count() {
    let _ = COUNT.try_with(|counter| {
        if let Some(value) = counter.get() {
            counter.set(Some(value.saturating_add(1)));
        }
    });
}

// SAFETY: Forward every unchanged allocation/pointer/layout to System. The
// allocation-free thread-local counter neither dereferences nor retains pointers.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the required valid layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the required valid layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: All allocations came from System with this same layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: The caller supplies System's allocation and valid new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

pub(crate) fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNT.with(|counter| counter.set(None));
        }
    }
    COUNT.with(|counter| assert!(counter.replace(Some(0)).is_none()));
    let reset = Reset;
    let output = operation();
    let allocations = COUNT.with(|counter| counter.get().unwrap());
    drop(reset);
    (output, allocations)
}
