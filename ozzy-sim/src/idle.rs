//! Whether this process's threads have work, so simulated time can wait for
//! them. The kernel marks a woken thread runnable before it runs, so a message
//! in flight between threads also counts as work.

use std::{collections::BTreeMap, sync::Mutex};

/// Threads that advance simulated time, with how often each entered.
static ADVANCERS: Mutex<BTreeMap<u32, usize>> = Mutex::new(BTreeMap::new());

/// While held, the current thread advances simulated time and never counts as
/// busy. Hold it only in a future that stays on one thread.
#[derive(Debug)]
pub struct Advancing {
    thread: Option<u32>,
}

impl Advancing {
    /// Mark the current thread as one that advances simulated time.
    pub fn enter() -> Self {
        let thread = current();
        if let Some(thread) = thread {
            *ADVANCERS.lock().unwrap().entry(thread).or_default() += 1;
        }
        Self { thread }
    }
}

impl Drop for Advancing {
    fn drop(&mut self) {
        let Some(thread) = self.thread else {
            return;
        };
        let mut advancers = ADVANCERS.lock().unwrap();
        if let Some(entered) = advancers.get_mut(&thread) {
            *entered -= 1;
            if *entered == 0 {
                advancers.remove(&thread);
            }
        }
    }
}

/// Whether no thread of this process, other than one advancing simulated
/// time, can run now or waits in the kernel. Without `/proc`, always true.
pub fn quiescent() -> bool {
    let Ok(threads) = std::fs::read_dir("/proc/self/task") else {
        return true;
    };
    let advancers = ADVANCERS.lock().unwrap();
    for thread in threads.flatten() {
        let Some(id) = (thread.file_name().to_str()).and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if advancers.contains_key(&id) {
            continue;
        }
        // A thread that exited meanwhile has no work.
        let Ok(stat) = std::fs::read_to_string(thread.path().join("stat")) else {
            continue;
        };
        if matches!(state(&stat), Some('R' | 'D')) {
            return false;
        }
    }
    true
}

/// The state letter of one `/proc` stat line. It follows the parenthesized
/// thread name, which may itself hold spaces and parentheses.
fn state(stat: &str) -> Option<char> {
    let (_, rest) = stat.rsplit_once(')')?;
    rest.trim_start().chars().next()
}

/// The current thread's kernel ID.
fn current() -> Option<u32> {
    let link = std::fs::read_link("/proc/thread-self").ok()?;
    link.file_name()?.to_str()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_state_follows_the_last_parenthesis() {
        assert_eq!(state("12 (a (b) c) S 1 2"), Some('S'));
        assert_eq!(state("12 (dispatcher) R 1"), Some('R'));
        assert_eq!(state("garbled"), None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn a_spinning_thread_is_busy() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let _advancing = Advancing::enter();
        let stop = Arc::new(AtomicBool::new(false));
        let spinning = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        };
        assert!(!quiescent());
        stop.store(true, Ordering::Relaxed);
        spinning.join().unwrap();
    }
}
