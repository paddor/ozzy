//! The replayed workload: the saved native case with the profile's bounds and
//! storage, behind the profiler the kind selects.
use crate::{Args, Kind};
use ozzy_bench::automation::{self, Result};
use std::path::Path;

/// The command that replays `args.case_dir` under the selected profiler.
pub(crate) fn workload(args: &Args, directory: &Path) -> Result<Vec<String>> {
    let mut command: Vec<String> =
        serde_json::from_value(automation::read_json(&args.case_dir.join("command.json"))?)?;
    require_native_case(&command)?;
    replace(&mut command, "--duration", args.duration.to_string())?;
    replace(&mut command, "--warmup", args.warmup.to_string())?;
    let name = directory
        .file_name()
        .ok_or("profile directory has no name")?;
    replace(
        &mut command,
        "--storage-dir",
        format!(
            "{}/ozzy-profile/{}",
            automation::artifact_root().display(),
            name.to_string_lossy()
        ),
    )?;
    let mut prefix = match args.kind {
        Kind::Stages => Vec::new(),
        Kind::Syscall => strace(directory),
        Kind::Cpu | Kind::CpuKernel => perf(args, directory),
    };
    prefix.append(&mut command);
    Ok(prefix)
}

/// The case ran this checkout's worker against a native system.
fn require_native_case(command: &[String]) -> Result<()> {
    let worker = automation::worker_binary();
    if !command.iter().any(|arg| Path::new(arg) == worker) {
        return Err("case uses another worker build; rerun comparison for this checkout".into());
    }
    let native_system = command
        .windows(2)
        .any(|v| v[0] == "--system" && matches!(v[1].as_str(), "single-durable" | "disk-quorum"));
    if !native_system || command.iter().any(|v| v == "--external-system") {
        return Err("profiling requires a native case".into());
    }
    Ok(())
}

fn replace(command: &mut [String], name: &str, value: String) -> Result<()> {
    let position = command
        .iter()
        .position(|v| v == name)
        .ok_or("missing case argument")?;
    *command
        .get_mut(position + 1)
        .ok_or("missing argument value")? = value;
    Ok(())
}

/// Trace the physical write and kernel AIO syscalls of every process.
fn strace(directory: &Path) -> Vec<String> {
    vec![
        "strace".into(),
        "-ff".into(),
        "--seccomp-bpf".into(),
        "-qq".into(),
        "-ttt".into(),
        "-T".into(),
        "-y".into(),
        "-s".into(),
        "0".into(),
        "-e".into(),
        "trace=openat,fdatasync,fsync,sync_file_range,pwrite64,pwritev,io_submit,io_getevents,io_pgetevents".into(),
        "-o".into(),
        directory.join("trace").display().to_string(),
    ]
}

/// Sample CPU stacks, or only instruction pointers, at the profile's
/// frequency, in user space or also in the kernel.
fn perf(args: &Args, directory: &Path) -> Vec<String> {
    let mut command: Vec<String> = vec![
        "perf".into(),
        "record".into(),
        "-q".into(),
        "-e".into(),
        if args.kind == Kind::CpuKernel {
            "cpu-clock"
        } else {
            "cpu-clock:u"
        }
        .into(),
        "-F".into(),
        args.frequency.to_string(),
        "--clockid".into(),
        "mono".into(),
    ];
    if !args.no_stacks {
        command.extend(["-g".into(), "--call-graph".into(), "dwarf,65528".into()]);
    }
    command.extend([
        // Full Rust async stacks can exceed the default perf ring during
        // short scheduling stalls. Lost samples invalidate the capture.
        "--mmap-pages".into(),
        "8M".into(),
        "-o".into(),
        directory.join("perf.data").display().to_string(),
        "--".into(),
        // perf 6.12 can retain the controller's mapping after a child exec
        // and use its lower ASLR base to unwind the broker's same binary.
        // Fixed addresses avoid that ambiguity only in profiling runs.
        "setarch".into(),
        std::env::consts::ARCH.into(),
        "-R".into(),
    ]);
    command
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flat_profiles_skip_stacks_and_allow_higher_rates() {
        let mut args = Args {
            case_dir: "case".into(),
            kind: Kind::Cpu,
            duration: 3.0,
            warmup: 1.0,
            frequency: 4999,
            no_stacks: false,
            current_build: false,
            counter_trace: false,
        };
        assert!(args.check().is_err());
        args.frequency = 999;
        args.check().unwrap();
        assert!(
            perf(&args, Path::new("out"))
                .iter()
                .any(|v| v == "--call-graph")
        );
        args.no_stacks = true;
        args.frequency = 4999;
        args.check().unwrap();
        assert!(
            !perf(&args, Path::new("out"))
                .iter()
                .any(|v| v == "--call-graph")
        );
        args.kind = Kind::Syscall;
        assert!(args.check().is_err());
    }

    #[test]
    fn replay_requires_the_worker_from_this_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let args = Args {
            case_dir: directory.path().into(),
            kind: Kind::Cpu,
            duration: 3.0,
            warmup: 1.0,
            frequency: 99,
            no_stacks: false,
            current_build: false,
            counter_trace: false,
        };
        let mut command = vec![
            "taskset".into(),
            "-c".into(),
            "0,1".into(),
            "/another/checkout/release/ozzy_timed_bench".into(),
            "--system".into(),
            "single-durable".into(),
            "--duration".into(),
            "10".into(),
            "--warmup".into(),
            "5".into(),
            "--storage-dir".into(),
            "/old/storage".into(),
        ];
        let path = directory.path().join("command.json");
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(
            workload(&args, directory.path())
                .unwrap_err()
                .to_string()
                .contains("another worker build")
        );
        command[3] = automation::worker_binary().display().to_string();
        automation::json_file(&path, &json!(command)).unwrap();
        let replay = workload(&args, directory.path()).unwrap();
        assert!(replay.iter().any(|part| part == &command[3]));
        assert!(replay.windows(2).any(|v| v == ["--duration", "3"]));
        assert!(replay.windows(2).any(|v| v == ["--warmup", "1"]));
        assert_ne!(replay.first(), command.first());
        command[5] = "disk-quorum".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_ok());
        command[5] = "single-buffered".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_err());
        command[5] = "ram-quorum".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_err());
    }
}
