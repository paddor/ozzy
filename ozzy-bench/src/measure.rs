//! Measurements the benchmark binaries share.

use std::time::Duration;

/// CPU time this process used so far, on every thread.
pub fn cpu() -> Duration {
    // Process CPU time never reads negative, and nanoseconds stay below 1e9.
    let value = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::new(value.tv_sec as u64, value.tv_nsec as u32)
}

/// The nearest-rank percentile of ascending `sorted` values, with the rank
/// in thousandths: 500 is the median. `None` for no values.
pub fn percentile<T: Copy>(sorted: &[T], per_mille: usize) -> Option<T> {
    let rank = sorted.len().saturating_mul(per_mille).div_ceil(1000);
    sorted.get(rank.saturating_sub(1)).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_the_nearest_rank() {
        let values = [1, 2, 3, 4];
        assert_eq!(percentile(&values, 0), Some(1));
        assert_eq!(percentile(&values, 250), Some(1));
        assert_eq!(percentile(&values, 500), Some(2));
        assert_eq!(percentile(&values, 501), Some(3));
        assert_eq!(percentile(&values, 990), Some(4));
        assert_eq!(percentile(&values, 1000), Some(4));
        assert_eq!(percentile::<u32>(&[], 500), None);
    }

    #[test]
    fn cpu_time_does_not_run_backward() {
        let before = cpu();
        assert!(cpu() >= before);
    }
}
