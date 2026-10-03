//! One writer, fresh unwritten extents, and separately measured write/barrier times.
use clap::{Parser, ValueEnum};
use ozzy_bench::automation::{self, Result, isolation, source};
use rustix::fs::{AtFlags, FallocateFlags, FlockOperation, StatxFlags};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, IoSlice, Write},
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MIB: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Buffered,
    Dsync,
    DirectDsync,
}

impl Mode {
    fn flags(self) -> i32 {
        match self {
            Self::Buffered => 0,
            Self::Dsync => libc::O_DSYNC,
            Self::DirectDsync => libc::O_DIRECT | libc::O_DSYNC,
        }
    }
}

#[derive(Debug, Parser)]
struct Args {
    /// Existing disposable regular file. Its contents will be overwritten.
    #[arg(long)]
    path: PathBuf,
    /// Required explicit authorization to truncate the named scratch file.
    #[arg(long, required = true)]
    overwrite: bool,
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value_t = 4096)]
    write_kib: usize,
    #[arg(long, default_value_t = 8192)]
    file_mib: usize,
    /// Synchronize this many bytes at a time, including for `O_DSYNC` controls.
    #[arg(long, default_value_t = 256)]
    segment_mib: usize,
    #[arg(long, default_value_t = 256)]
    warmup_mib: usize,
    /// Include copying prepared payloads into the aligned write buffer.
    #[arg(long)]
    staging_copy: bool,
    /// Append-only JSONL. Defaults to ~/.cache/ozzy/disk-probe.jsonl.
    #[arg(long)]
    results: Option<PathBuf>,
    /// Campaign/repetition label retained with the measurement.
    #[arg(long, default_value = "manual")]
    label: String,
}

impl Args {
    fn validate(&self) -> Result<()> {
        if !self.overwrite
            || !(4..=262_144).contains(&self.write_kib)
            || !self.write_kib.is_multiple_of(4)
            || !(1..=32768).contains(&self.file_mib)
            || !(1..=1024).contains(&self.segment_mib)
            || self.warmup_mib > self.file_mib
            || !self.file_mib.is_multiple_of(self.segment_mib)
            || !(self.segment_mib * 1024).is_multiple_of(self.write_kib)
            || !(self.warmup_mib * 1024).is_multiple_of(self.write_kib)
        {
            return Err("invalid bounds or nonintegral write/segment/file sizes".into());
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Buffer {
    bytes: Vec<u8>,
    start: usize,
    len: usize,
}

impl Buffer {
    fn new(len: usize, alignment: usize) -> Self {
        // Overallocate and borrow an aligned interior slice. No unsafe casts.
        let bytes = vec![0; len + alignment];
        let start = bytes.as_ptr().align_offset(alignment);
        Self { bytes, start, len }
    }

    fn get(&self) -> &[u8] {
        &self.bytes[self.start..self.start + self.len]
    }

    fn get_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[self.start..self.start + self.len]
    }
}

fn write_all(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        match rustix::io::pwritev(file, &[IoSlice::new(bytes)], offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => {
                bytes = &bytes[count..];
                offset += count as u64;
            }
            Err(rustix::io::Errno::INTR) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn allocate(file: &File, bytes: usize) -> Result<()> {
    file.set_len(0)?;
    rustix::fs::fallocate(file, FallocateFlags::empty(), 0, bytes as u64)?;
    file.sync_all()?;
    Ok(())
}

fn fill(bytes: &mut [u8]) {
    let mut state = 0x1234_5678_9abc_def0_u64;
    for chunk in bytes.as_chunks_mut::<8>().0 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
}

fn latencies(values: &mut [u64]) -> Value {
    values.sort_unstable();
    let percentile = |n: usize| {
        (!values.is_empty()).then(|| values[(values.len() * n).div_ceil(100) - 1] as f64 / 1000.0)
    };
    json!({"samples":values.len(),"p50_us":percentile(50),"p99_us":percentile(99),
        "max_us":values.last().map(|v| *v as f64 / 1000.0)})
}

fn verify(file: &File, payload: &[u8], size: usize, segment: usize) -> Result<()> {
    if file.metadata()?.len() != size as u64 {
        return Err("file length changed unexpectedly".into());
    }
    // Aligned head and tail of every segment. This is a probe, not a storage oracle.
    let stat = rustix::fs::statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN)?;
    let length = 4096_usize.max(stat.stx_dio_offset_align as usize);
    let mut sample = Buffer::new(length, 4096_usize.max(stat.stx_dio_mem_align as usize));
    for start in (0..size).step_by(segment) {
        for offset in [start, start + segment - length] {
            file.read_exact_at(sample.get_mut(), offset as u64)?;
            let within = offset % payload.len();
            if sample.get() != &payload[within..within + length] {
                return Err("segment boundary verification failed".into());
            }
        }
    }
    Ok(())
}

fn measure(args: &Args, file: &File, buffer: &mut Buffer) -> Result<Value> {
    let size = args.file_mib * MIB;
    let segment = args.segment_mib * MIB;
    let chunk = args.write_kib * 1024;
    let source = args.staging_copy.then(|| buffer.get().to_vec());
    let mut writes = Vec::with_capacity(size / chunk);
    let mut batches = Vec::with_capacity(size / chunk);
    let mut barriers = Vec::with_capacity(size / segment);
    let start = Instant::now();
    for offset in (0..size).step_by(chunk) {
        automation::check_canceled()?;
        let batch = Instant::now();
        if let Some(source) = &source {
            buffer.get_mut().copy_from_slice(source);
        }
        let write = Instant::now();
        write_all(file, buffer.get(), offset as u64)?;
        writes.push(write.elapsed().as_nanos() as u64);
        batches.push(batch.elapsed().as_nanos() as u64);
        if (offset + chunk).is_multiple_of(segment) {
            let barrier = Instant::now();
            file.sync_data()?;
            barriers.push(barrier.elapsed().as_nanos() as u64);
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    verify(file, buffer.get(), size, segment)?;
    Ok(
        json!({"seconds":seconds,"mib_s":args.file_mib as f64/seconds,
        "write":latencies(&mut writes),"batch_including_copy":latencies(&mut batches),
        "segment_sync":latencies(&mut barriers),"boundary_samples_verified":2*size/segment}),
    )
}

pub(super) fn run() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    automation::install_signals()?;
    isolation::require_idle()?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(args.mode.flags() | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&args.path)?;
    if !file.metadata()?.is_file() {
        return Err("probe requires an existing regular scratch file".into());
    }
    rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive)?;
    let stat = rustix::fs::statx(&file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN)?;
    let alignment = 4096_usize.max(stat.stx_dio_mem_align as usize);
    let offset_alignment = 4096_usize.max(stat.stx_dio_offset_align as usize);
    if !alignment.is_power_of_two() || !(args.write_kib * 1024).is_multiple_of(offset_alignment) {
        return Err("unsupported filesystem direct-I/O alignment".into());
    }
    let mut buffer = Buffer::new(args.write_kib * 1024, alignment);
    fill(buffer.get_mut());
    rustix::fs::syncfs(&file)?;
    allocate(&file, args.file_mib * MIB)?;
    for offset in (0..args.warmup_mib * MIB).step_by(buffer.len) {
        write_all(&file, buffer.get(), offset as u64)?;
    }
    file.sync_data()?;
    allocate(&file, args.file_mib * MIB)?;
    let result = measure(&args, &file, &mut buffer)?;
    isolation::require_idle()?;
    let row = json!({"implementation":"disk-probe","run_ns":SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos().to_string(),
        "mode":format!("{:?}",args.mode),"label":args.label,"path":args.path,"file_mib":args.file_mib,
        "segment_mib":args.segment_mib,"write_kib":args.write_kib,"warmup_mib":args.warmup_mib,
        "staging_copy":args.staging_copy,"payload":"repeated deterministic xorshift block; prepared before timing",
        "layout":"fresh fallocate extents; no EOF growth","alignment":alignment,
        "reported_memory_alignment":stat.stx_dio_mem_align,"reported_offset_alignment":stat.stx_dio_offset_align,
        "open_flags":rustix::fs::fcntl_getfl(&file)?.bits(),
        "filesystem_type":rustix::fs::fstatfs(&file)?.f_type,
        "device":file.metadata()?.dev(),"cpu_affinity":isolation::cpus(None)?,
        "host":fs::read_to_string("/proc/sys/kernel/hostname")?.trim(),
        "kernel":fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
        "executable_sha256":source::sha256(&std::env::current_exe()?)?,
        "scope":"one-file disk probe; includes periodic data barriers; excludes Ozzy framing, metadata roll, network and confirmation",
        "result":result});
    let path = args.results.unwrap_or_else(|| {
        PathBuf::from(std::env::var_os("HOME").expect("HOME")).join(".cache/ozzy/disk-probe.jsonl")
    });
    fs::create_dir_all(path.parent().ok_or("result path needs parent")?)?;
    let mut output = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(output, "{row}")?;
    output.sync_data()?;
    println!("{row}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interior_buffer_has_requested_alignment_and_length() {
        for alignment in [4096, 8192, 65536] {
            let mut buffer = Buffer::new(65536, alignment);
            assert_eq!(buffer.get().as_ptr().align_offset(alignment), 0);
            assert_eq!(buffer.get().len(), 65536);
            fill(buffer.get_mut());
            assert!(buffer.get().iter().any(|byte| *byte != 0));
        }
    }

    #[test]
    fn percentile_uses_nearest_rank_and_empty_is_explicit() {
        let stats = latencies(&mut [1000, 3000, 2000, 4000]);
        assert_eq!(stats["p50_us"], 2.0);
        assert_eq!(stats["p99_us"], 4.0);
        assert!(latencies(&mut [])["p99_us"].is_null());
    }
}
