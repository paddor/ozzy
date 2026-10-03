use std::time::Duration;

use serde_json::{Value, json};

#[derive(Debug)]
pub(super) struct Meter {
    started_ns: u64,
    cpu: Duration,
}

impl Meter {
    pub(super) fn start() -> Self {
        #[cfg(feature = "allocation-counting")]
        allocation::start();
        Self {
            started_ns: monotonic_ns(),
            cpu: cpu(),
        }
    }

    pub(super) fn finish(self) -> Value {
        self.stop().report()
    }

    /// Freeze counters without formatting or allocating a report. Parallel
    /// writers defer that work until every process has stopped its meter.
    pub(super) fn stop(self) -> Measurement {
        #[cfg(feature = "allocation-counting")]
        let allocations = allocation::snapshot();
        let cpu_seconds = cpu()
            .checked_sub(self.cpu)
            .expect("process CPU clock regressed")
            .as_secs_f64();
        Measurement {
            started_ns: self.started_ns,
            finished_ns: monotonic_ns(),
            cpu_seconds,
            #[cfg(feature = "allocation-counting")]
            allocations,
        }
    }
}

pub(super) struct Measurement {
    started_ns: u64,
    finished_ns: u64,
    cpu_seconds: f64,
    #[cfg(feature = "allocation-counting")]
    allocations: allocation::Snapshot,
}

impl Measurement {
    pub(super) fn report(self) -> Value {
        let elapsed = (self.finished_ns - self.started_ns) as f64 / 1e9;
        let cpu_seconds = self.cpu_seconds;
        #[cfg(feature = "allocation-counting")]
        let allocations = self.allocations.report();
        #[cfg(not(feature = "allocation-counting"))]
        let allocations = Value::Null;
        let row = json!({"elapsed_seconds": elapsed, "cpu_seconds": cpu_seconds,
            "started_monotonic_ns": self.started_ns, "finished_monotonic_ns": self.finished_ns,
            "average_cpu_cores": cpu_seconds / elapsed,
            "allocation_counted": cfg!(feature = "allocation-counting"),
            "allocations": allocations,
            "cpu_scope": "whole worker process; all application, transport, and storage threads"});
        row
    }
}

/// Shared only between local writer processes, never between broker hosts.
pub(super) fn monotonic_ns() -> u64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    u64::try_from(now.tv_sec).expect("positive monotonic clock") * 1_000_000_000
        + u64::try_from(now.tv_nsec).expect("positive nanoseconds")
}

impl Drop for Meter {
    fn drop(&mut self) {
        #[cfg(feature = "allocation-counting")]
        allocation::disable();
    }
}

fn cpu() -> Duration {
    let value = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::new(
        value.tv_sec.try_into().unwrap(),
        value.tv_nsec.try_into().unwrap(),
    )
}

#[cfg(feature = "allocation-counting")]
mod allocation {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::{Value, json};

    static GATE: Gate = Gate::new();
    static CALLS: AtomicU64 = AtomicU64::new(0);
    static BYTES: AtomicU64 = AtomicU64::new(0);
    static REALLOCS: AtomicU64 = AtomicU64::new(0);
    // Exact small-object sizes, then power-of-two upper bounds for large buffers.
    // Static counters keep the allocator instrumentation itself allocation-free.
    const SMALL: usize = 1024;
    static SIZES: [AtomicU64; SMALL + 65] = [const { AtomicU64::new(0) }; SMALL + 65];

    // One atomic admission word closes the bracket without losing an admitted
    // counter update. Snapshot/reset waits for those updates, not allocations.
    struct Gate(AtomicU64);

    impl Gate {
        const ENABLED: u64 = 1 << 63;
        const ACTIVE: u64 = Self::ENABLED - 1;

        const fn new() -> Self {
            Self(AtomicU64::new(0))
        }

        fn enable(&self) {
            assert_eq!(self.0.swap(Self::ENABLED, Ordering::AcqRel), 0);
        }

        fn enter(&self) -> Option<Update<'_>> {
            let mut state = self.0.load(Ordering::Acquire);
            while state & Self::ENABLED != 0 {
                match self.0.compare_exchange_weak(
                    state,
                    state + 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(Update(self)),
                    Err(current) => state = current,
                }
            }
            None
        }

        fn disable(&self) {
            self.0.fetch_and(Self::ACTIVE, Ordering::AcqRel);
            while self.0.load(Ordering::Acquire) != 0 {
                std::hint::spin_loop();
            }
        }
    }

    struct Update<'a>(&'a Gate);

    impl Drop for Update<'_> {
        fn drop(&mut self) {
            self.0.0.fetch_sub(1, Ordering::Release);
        }
    }

    #[derive(Debug)]
    struct Counted;

    #[global_allocator]
    static ALLOCATOR: Counted = Counted;

    fn record(size: usize, realloc: bool) {
        if let Some(_update) = GATE.enter() {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size as u64, Ordering::Relaxed);
            let bucket = if size <= SMALL {
                size
            } else {
                SMALL + (usize::BITS - (size - 1).leading_zeros()) as usize
            };
            SIZES[bucket].fetch_add(1, Ordering::Relaxed);
            if realloc {
                REALLOCS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // SAFETY: All unchanged allocator arguments/pointers are forwarded to System.
    // Counters allocate nothing and never inspect or retain allocation pointers.
    unsafe impl GlobalAlloc for Counted {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), false);
            // SAFETY: GlobalAlloc caller provides a valid layout.
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), false);
            // SAFETY: GlobalAlloc caller provides a valid layout.
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            record(size, true);
            // SAFETY: Caller provides System's allocation/layout and valid size.
            unsafe { System.realloc(pointer, layout, size) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: Allocation came from System with the supplied layout.
            unsafe { System.dealloc(pointer, layout) }
        }
    }

    pub(super) fn start() {
        assert_eq!(GATE.0.load(Ordering::Acquire), 0);
        CALLS.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        REALLOCS.store(0, Ordering::Relaxed);
        for count in &SIZES {
            count.store(0, Ordering::Relaxed);
        }
        GATE.enable();
    }

    pub(super) fn disable() {
        GATE.disable();
    }

    pub(super) struct Snapshot {
        calls: u64,
        bytes: u64,
        reallocs: u64,
        sizes: [u64; SMALL + 65],
    }

    pub(super) fn snapshot() -> Snapshot {
        disable();
        Snapshot {
            calls: CALLS.load(Ordering::Relaxed),
            bytes: BYTES.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
            sizes: std::array::from_fn(|bucket| SIZES[bucket].load(Ordering::Relaxed)),
        }
    }

    impl Snapshot {
        pub(super) fn report(self) -> Value {
            let sizes: Vec<_> = self.sizes.iter().enumerate().filter(|(_, count)| **count != 0).map(|(bucket, &count)| {
            json!({
                "bytes_upper_bound": if bucket <= SMALL { bucket as u64 } else { 1_u64.checked_shl((bucket - SMALL) as u32).unwrap_or(u64::MAX) },
                "exact_size": bucket <= SMALL, "attempts": count
            })
        }).collect();
            json!({"attempts": self.calls,
            "requested_bytes": self.bytes,
            "reallocations": self.reallocs,
            "size_histogram": sizes,
            "scope": "Rust allocator on all process threads inside measurement bracket; realloc counts as an attempt; foreign-library allocations excluded"})
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::AtomicBool;

        #[test]
        #[ignore = "process-wide allocator bracket; run alone with --test-threads=1"]
        fn tiny_input_payload_and_message_allocate_nothing() {
            use omq_tokio::{Message, message::Payload};
            use ozzy_proto::MessageId;
            use ozzy_runtime::replicated::RecordInput;
            use std::hint::black_box;

            start();
            for _ in 0..8192 {
                let input = RecordInput::copy_from_slice(MessageId::from_bytes([2; 16]), &[7; 16]);
                let copied = black_box(input.clone());
                let body = copied.parts().next().unwrap();
                let message = black_box(Message::from_slice(body));
                let cloned = black_box(message.clone());
                let retained = Payload::from_slice(cloned.part_slice(0).unwrap());
                assert_eq!(black_box(retained.clone()).as_slice(), &[7; 16]);
            }
            let counts = snapshot();
            assert_eq!(counts.calls, 0, "tiny payload/message allocation");
            assert_eq!(counts.bytes, 0);

            // Prove the bracket catches heap allocation rather than optimizing
            // away the instrumentation or leaving its gate disabled.
            start();
            drop(black_box(Vec::<u8>::with_capacity(53)));
            assert!(snapshot().calls > 0);
            println!("8192 SDK inputs + tiny payload frames + inline retention: 0 allocations");
        }

        #[test]
        fn snapshot_gate_waits_for_admitted_updates_and_rejects_late_updates() {
            let gate = Gate::new();
            assert!(gate.enter().is_none());
            gate.enable();
            let update = gate.enter().unwrap();
            let complete = AtomicBool::new(false);
            std::thread::scope(|scope| {
                let closing = scope.spawn(|| {
                    gate.disable();
                    complete.store(true, Ordering::Release);
                });
                while gate.0.load(Ordering::Acquire) & Gate::ENABLED != 0 {
                    std::thread::yield_now();
                }
                assert!(gate.enter().is_none());
                assert!(!complete.load(Ordering::Acquire));
                drop(update);
                closing.join().unwrap();
                assert!(complete.load(Ordering::Acquire));
            });
            gate.enable();
            drop(gate.enter().unwrap());
            gate.disable();
            assert!(gate.enter().is_none());
        }
    }
}
