//! Scripted faults: one at a time, each from a tick once a ready leader
//! exists. A fault on the leader or a slow follower link also waits until
//! every broker is ready in its view: the others must replace the leader
//! within the fault, or keep it, and a timeout backed off by earlier views
//! or a copy still catching up would change views on its own.

/// A scripted fault.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Fault {
    /// From tick `at`, once a ready leader exists, cut it off from both
    /// peers for `ticks`. The other two must elect a ready leader before it
    /// rejoins.
    IsolateLeader { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, cut off one of its
    /// followers for `ticks`, a sparse one if any. The other two keep going.
    IsolateFollower { at: u64, ticks: u64 },
    /// As `IsolateLeader`, but the leader also loses power and restarts
    /// over its durable disk image. Its links come back after `ticks`.
    CrashLeader { at: u64, ticks: u64 },
    /// As `IsolateFollower`, with a power loss and restart.
    CrashFollower { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, every broker loses power
    /// at once and restarts over its durable disk image. Links come back at
    /// once. No committed operation may be lost.
    CrashAll { at: u64 },
    /// After the last whole-cluster power loss, every broker loses power
    /// again at a seeded tick within `within`, without waiting for a leader:
    /// mid election, installation, or activation.
    Recrash { within: u64 },
    /// As `CrashAll`, but every disk first stops completing jobs, so writes
    /// queue up. Power fails at a seeded tick within `within`, with those
    /// writes in flight.
    CrashAllMidWrite { at: u64, within: u64 },
    /// From tick `at`, once a ready leader exists, its disk completes no job
    /// for `ticks`. Links stay up and random partitions wait. Another broker
    /// must lead before the disk recovers.
    StallLeader { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, its process runs no turn
    /// for `ticks`, as a VM pause or a stopped process does. Its disk and
    /// links keep going; random partitions wait. Another broker must lead
    /// before it resumes.
    PauseLeader { at: u64, ticks: u64 },
    /// As `IsolateLeader`, but only frames to the leader stop.
    DeafLeader { at: u64, ticks: u64 },
    /// As `IsolateLeader`, but only frames from the leader stop.
    MuteLeader { at: u64, ticks: u64 },
    /// From tick `at`, once every broker is ready in a leader's view, frames
    /// from one of its followers, a sparse one if any, take `SLOW_LINK`
    /// longer for `ticks`. Random partitions and clogs wait. The leader must
    /// keep its view and commit through the other follower.
    SlowFollowerSends { at: u64, ticks: u64 },
    /// As `SlowFollowerSends`, but frames to the follower are slow, so it
    /// hears the leader late.
    SlowFollowerHears { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, every write and sync on
    /// its disk fails. The broker stops on one, as a broker process exits,
    /// restarts over what its disk kept, now healthy, and rejoins `ticks`
    /// after the fault started. Random partitions wait. Another broker must lead meanwhile.
    FailLeaderDisk { at: u64, ticks: u64 },
    /// As `FailLeaderDisk`, on one of its followers, a sparse one if any.
    FailFollowerDisk { at: u64, ticks: u64 },
    /// As `CrashLeader`, but only the process dies. The restart sees what
    /// its disk cached, and queued writes may still land.
    KillLeader { at: u64, ticks: u64 },
    /// As `CrashFollower`, but only the process dies.
    KillFollower { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, its host's wall clock is
    /// `SKEW` ahead for `ticks`. Its guard must hold every release meanwhile;
    /// it may keep leading.
    SkewLeader { at: u64, ticks: u64 },
    /// Negative control. From tick `at`, once a ready leader exists, every
    /// disk keeps a copy of what it holds. After `ticks`, every broker loses
    /// power and restarts over that copy, as if each disk had lied about
    /// every flush since. Confirmed operations are lost.
    Rewind { at: u64, ticks: u64 },
}

impl Fault {
    /// Its start tick and length.
    pub(super) const fn window(self) -> (u64, u64) {
        match self {
            Self::IsolateLeader { at, ticks }
            | Self::IsolateFollower { at, ticks }
            | Self::CrashLeader { at, ticks }
            | Self::CrashFollower { at, ticks }
            | Self::StallLeader { at, ticks }
            | Self::PauseLeader { at, ticks }
            | Self::DeafLeader { at, ticks }
            | Self::MuteLeader { at, ticks }
            | Self::SlowFollowerSends { at, ticks }
            | Self::SlowFollowerHears { at, ticks }
            | Self::FailLeaderDisk { at, ticks }
            | Self::FailFollowerDisk { at, ticks }
            | Self::KillLeader { at, ticks }
            | Self::KillFollower { at, ticks }
            | Self::SkewLeader { at, ticks }
            | Self::Rewind { at, ticks }
            | Self::CrashAllMidWrite { at, within: ticks } => (at, ticks),
            Self::CrashAll { at } => (at, 0),
            Self::Recrash { within } => (0, within),
        }
    }

    pub(super) const fn leader(self) -> bool {
        matches!(
            self,
            Self::IsolateLeader { .. }
                | Self::CrashLeader { .. }
                | Self::StallLeader { .. }
                | Self::PauseLeader { .. }
                | Self::DeafLeader { .. }
                | Self::MuteLeader { .. }
                | Self::FailLeaderDisk { .. }
                | Self::KillLeader { .. }
        )
    }

    /// A slow follower link, under which the leader must keep its view.
    pub(super) const fn slow(self) -> bool {
        matches!(
            self,
            Self::SlowFollowerSends { .. } | Self::SlowFollowerHears { .. }
        )
    }

    pub(super) const fn crash(self) -> bool {
        matches!(
            self,
            Self::CrashLeader { .. }
                | Self::CrashFollower { .. }
                | Self::CrashAll { .. }
                | Self::Recrash { .. }
                | Self::CrashAllMidWrite { .. }
                | Self::KillLeader { .. }
                | Self::KillFollower { .. }
        )
    }
}
