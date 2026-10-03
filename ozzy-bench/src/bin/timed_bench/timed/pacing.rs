//! Exact scheduled arrivals, bounded lateness, and absolute monotonic timers.

use ozzy_bench::schedule::{Load, MAX_STAGES, Schedule, assigned};
use rustix::fd::OwnedFd;
use rustix::time::{
    Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec, timerfd_create,
    timerfd_settime,
};
use tokio::io::unix::AsyncFd;

use super::super::{Result, error, metrics};
use super::{Config, Window};

/// An arrival not admitted this long after its due time means the system did
/// not sustain the offered load. Unadmitted arrivals are ordinals, not memory.
pub(super) const MAX_ADMISSION_LAG_NS: u64 = 1_000_000_000;

#[derive(Clone, Copy)]
pub(super) struct Plan {
    schedule: Schedule,
    window: Window,
    lanes: usize,
    lane: usize,
    total: u64,
    /// Absolute start of each measured stage, then the window end.
    bounds: [u64; MAX_STAGES + 1],
    stages: usize,
    ramp: bool,
}

impl Plan {
    pub(super) fn new(config: &Config, window: Window, lane: usize) -> Result<Option<Self>> {
        let mut bounds = [window.end; MAX_STAGES + 1];
        bounds[0] = window.measured;
        let (schedule, stages) = if let Some(ramp) = &config.args.ramp {
            let mut start = window.measured;
            for (index, (_, ns)) in ramp.stages().iter().enumerate() {
                bounds[index] = start;
                start += ns;
            }
            if start != window.end {
                return Err(error("ramp stages differ from the measurement window"));
            }
            (
                ramp.plan(window.measured - window.start)?,
                ramp.stages().len(),
            )
        } else {
            let Some(schedule) = (Load {
                records_per_second: config.args.records_per_second,
                ..Load::default()
            })
            .plan(1)?
            else {
                return Ok(None);
            };
            (schedule, 1)
        };
        bounds[stages] = window.end;
        Ok(Some(Self {
            schedule,
            window,
            lanes: config.writers,
            lane,
            total: assigned(
                schedule.count(window.end - window.start)?,
                config.writers,
                lane..lane + 1,
            ) as u64,
            bounds,
            stages,
            ramp: config.args.ramp.is_some(),
        }))
    }

    /// Absolute measured stage starts followed by the window end.
    pub(super) fn bounds(&self) -> &[u64] {
        &self.bounds[..=self.stages]
    }

    /// Stage of an arrival; warmup belongs to the first stage.
    fn stage(self, due: u64) -> usize {
        self.bounds[1..self.stages]
            .iter()
            .take_while(|start| **start <= due)
            .count()
    }

    /// This lane's arrivals due before `stage`. An overloaded lane still
    /// admits these, late but with their original clocks, so every earlier
    /// stage keeps a complete cohort.
    pub(super) fn total_before(self, stage: usize) -> Result<u64> {
        if stage == 0 {
            return Ok(0);
        }
        Ok(assigned(
            self.schedule
                .count(self.bounds[stage] - self.window.start)?,
            self.lanes,
            self.lane..self.lane + 1,
        ) as u64)
    }

    /// This lane's arrivals due inside one measured stage.
    pub(super) fn stage_total(self, stage: usize) -> Result<u64> {
        let count = |at: u64| -> Result<u64> {
            Ok(assigned(
                self.schedule.count(at - self.window.start)?,
                self.lanes,
                self.lane..self.lane + 1,
            ) as u64)
        };
        Ok(count(self.bounds[stage + 1])? - count(self.bounds[stage])?)
    }

    pub(super) fn total(self) -> u64 {
        self.total
    }

    pub(super) fn due(self, sequence: u64) -> Result<u64> {
        if sequence >= self.total() {
            return Err(error("record outside scheduled arrival cohort"));
        }
        let ordinal = sequence
            .checked_mul(self.lanes as u64)
            .and_then(|n| n.checked_add(self.lane as u64))
            .ok_or_else(|| error("scheduled ordinal overflow"))?;
        self.schedule.due(self.window.start, ordinal)
    }

    /// Never drop arrivals or move their original clock when admission is
    /// late. Once the oldest unadmitted arrival is `MAX_ADMISSION_LAG_NS` late,
    /// a fixed rate fails and a ramp returns that arrival's stage. Every earlier
    /// stage was admitted completely; the lane then admits nothing more.
    pub(super) fn check_backlog(self, admitted: u64, now: u64) -> Result<Option<usize>> {
        if admitted < self.total() {
            let due = self.due(admitted)?;
            if due.saturating_add(MAX_ADMISSION_LAG_NS) <= now {
                if self.ramp {
                    return Ok(Some(self.stage(due)));
                }
                return Err(error(format!(
                    "scheduled backlog exceeded: lane={} admitted={admitted} lag_ns={}",
                    self.lane,
                    now - due,
                )));
            }
        }
        Ok(None)
    }
}

/// One reusable timer per writer lane. No millisecond rounding, busy waiting,
/// or blocking the Tokio thread while confirmations need observation.
pub(super) struct Timer(AsyncFd<OwnedFd>);

impl Timer {
    pub(super) fn new() -> Result<Self> {
        Ok(Self(AsyncFd::new(timerfd_create(
            TimerfdClockId::Monotonic,
            TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC,
        )?)?))
    }

    pub(super) async fn wait_until(&mut self, at: u64) -> Result<()> {
        if metrics::monotonic_ns() >= at {
            return Ok(());
        }
        timerfd_settime(
            self.0.get_ref(),
            TimerfdTimerFlags::ABSTIME,
            &Itimerspec {
                it_interval: Timespec::default(),
                it_value: Timespec {
                    tv_sec: (at / 1_000_000_000).try_into()?,
                    tv_nsec: (at % 1_000_000_000).try_into()?,
                },
            },
        )?;
        loop {
            let mut ready = self.0.readable_mut().await?;
            let read = ready.try_io(|fd| {
                rustix::io::read(fd.get_ref(), &mut [0_u8; 8]).map_err(std::io::Error::from)
            });
            match read {
                Ok(Ok(8)) => return Ok(()),
                Ok(Ok(_)) => return Err(error("incomplete timer expiration")),
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => (), // Stale readiness after rearming; try_io clears it.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn plan(lane: usize) -> Plan {
        Plan {
            schedule: Load {
                records_per_second: Some(1001),
                ..Load::default()
            }
            .plan(1)
            .unwrap()
            .unwrap(),
            window: Window {
                start: 10_000_000,
                measured: 10_000_000,
                end: 1_010_000_000,
            },
            lanes: 3,
            lane,
            total: assigned(1001, 3, lane..lane + 1) as u64,
            bounds: [1_010_000_000; MAX_STAGES + 1],
            stages: 1,
            ramp: false,
        }
    }

    #[test]
    fn startup_count_preserves_empty_lanes_and_warmup_remainders() {
        for lanes in ["1", "3", "16"] {
            let config = Config::new(super::super::super::Args::parse_from([
                "bench",
                "--system",
                "single-durable",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
                "--window",
                lanes,
                "--records-per-second",
                "7",
            ]))
            .unwrap();
            let window = Window {
                start: 10_000_000,
                measured: 510_000_000,
                end: 1_010_000_000,
            };
            for lane in 0..config.writers {
                let plan = Plan::new(&config, window, lane).unwrap().unwrap();
                let expected = (0..7).filter(|n| n % config.writers == lane).count() as u64;
                let warmup = (0..4).filter(|n| n % config.writers == lane).count() as u64;
                assert_eq!(plan.total(), expected);
                assert_eq!(plan.stage_total(0).unwrap(), expected - warmup);
                assert!(plan.due(expected).is_err());
                assert_eq!(plan.check_backlog(expected, u64::MAX).unwrap(), None);
            }
        }
    }

    #[test]
    fn uneven_lanes_preserve_every_half_open_arrival() {
        assert_eq!(
            (0..3).map(|lane| plan(lane).total()).collect::<Vec<_>>(),
            [334, 334, 333]
        );
        let mut arrivals = Vec::new();
        for lane in 0..3 {
            let p = plan(lane);
            for sequence in 0..p.total() {
                let due = p.due(sequence).unwrap();
                assert!(due >= p.window.start && due < p.window.end);
                arrivals.push(due);
            }
            assert!(p.due(p.total()).is_err());
        }
        arrivals.sort_unstable();
        assert_eq!(arrivals.len(), 1001);
        for (ordinal, due) in arrivals.into_iter().enumerate() {
            assert_eq!(
                due,
                plan(0)
                    .schedule
                    .due(plan(0).window.start, ordinal as u64)
                    .unwrap()
            );
        }
    }

    #[test]
    fn lateness_is_bounded_without_resetting_clock_or_skipping_drain() {
        let p = plan(0);
        let due = p.due(0).unwrap();
        assert!(p.check_backlog(0, due + MAX_ADMISSION_LAG_NS - 1).is_ok());
        assert!(p.check_backlog(0, due + MAX_ADMISSION_LAG_NS).is_err());
        let last = p.total() - 1;
        let late = p.due(last).unwrap() + MAX_ADMISSION_LAG_NS;
        assert!(p.check_backlog(last, late - 1).is_ok());
        assert!(p.check_backlog(last, late).is_err());
        // Fully admitted lanes only drain; lateness no longer applies.
        assert_eq!(p.check_backlog(p.total(), u64::MAX).unwrap(), None);
        assert_eq!(p.due(0).unwrap(), due);
        assert!(p.due(last).unwrap() < p.window.end);
    }

    #[test]
    fn ramp_overload_names_its_stage_and_stage_totals_cover_every_arrival() {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--system",
            "single-durable",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "3",
            "--window",
            "2",
            "--request-records",
            "1",
            "--ramp",
            "10:2,1000:1",
        ]))
        .unwrap();
        let window = Window {
            start: 0,
            measured: 1_000_000_000,
            end: 4_000_000_000,
        };
        for lane in 0..2 {
            let plan = Plan::new(&config, window, lane).unwrap().unwrap();
            assert_eq!(plan.bounds(), [1_000_000_000, 3_000_000_000, 4_000_000_000]);
            assert_eq!(plan.stage_total(0).unwrap(), 10);
            assert_eq!(plan.stage_total(1).unwrap(), 500);
            assert_eq!(plan.total(), 515);
            // The oldest late arrival names the stage.
            assert_eq!(plan.check_backlog(0, 3_000_000_000).unwrap(), Some(0));
            assert_eq!(plan.check_backlog(15, 4_500_000_000).unwrap(), Some(1));
            assert_eq!(plan.check_backlog(15, 3_500_000_000).unwrap(), None);
            assert_eq!(plan.check_backlog(0, 0).unwrap(), None);
            assert_eq!(plan.total_before(0).unwrap(), 0);
            assert_eq!(plan.total_before(1).unwrap(), 15);
        }
    }

    #[tokio::test]
    async fn absolute_timer_rearms_after_cancellation_and_never_releases_early() {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut timer = Timer::new().unwrap();
            {
                let wait = timer.wait_until(metrics::monotonic_ns() + 60_000_000_000);
                tokio::pin!(wait);
                assert!(futures::poll!(wait.as_mut()).is_pending());
            }
            // An expired deadline must complete without arming a disarmed (zero)
            // timer. Canceling the old future must not keep its later deadline.
            timer.wait_until(0).await.unwrap();
            for delay in [100_001, 1_700_003, 200_007] {
                let at = metrics::monotonic_ns() + delay;
                timer.wait_until(at).await.unwrap();
                assert!(metrics::monotonic_ns() >= at);
            }
        })
        .await
        .unwrap();
    }
}
