//! Child lifecycle, protocol barriers, and bounded credit forwarding.
use super::Error;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};
pub(super) struct Worker {
    pub(super) child: Child,
    input: ChildStdin,
    readers: Vec<thread::JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}
impl Worker {
    pub(super) fn spawn(
        role: &str,
        command: &mut Command,
        events: &Sender<(String, Value)>,
    ) -> Result<Self, Error> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().ok_or("missing stdin")?;
        let mut readers = Vec::new();
        for (stderr, pipe) in [
            (
                false,
                Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
            ),
            (true, Box::new(child.stderr.take().unwrap())),
        ] {
            let (role, events) = (role.to_owned(), events.clone());
            readers.push(thread::spawn(move || {
                for line in BufReader::new(pipe).lines() {
                    let value = match line {
                        Ok(line) if !stderr => serde_json::from_str(&line)
                            .unwrap_or_else(|_| json!({"event":"error", "text":line})),
                        line => json!({"event":"error", "text":format!("{line:?}")}),
                    };
                    if events.send((role.clone(), value)).is_err() {
                        break;
                    }
                }
            }));
        }
        Ok(Self {
            child,
            input,
            readers,
        })
    }
    pub(super) fn send(&mut self, command: &Value) -> Result<(), Error> {
        writeln!(self.input, "{command}")?;
        self.input.flush()?;
        Ok(())
    }
    pub(super) fn finish(&mut self) -> Result<(), Error> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if !status.success() {
                    return Err(format!("child failed: {status}").into());
                }
                for reader in self.readers.drain(..) {
                    reader.join().map_err(|_| "output reader panicked")?;
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("child shutdown timed out".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn receive(
    events: &Receiver<(String, Value)>,
    workers: &mut BTreeMap<String, Worker>,
) -> Result<(String, Value), Error> {
    let mut deadline = Instant::now() + Duration::from_secs(45);
    let mut progress = Instant::now();
    let mut credits = 0_u64;
    loop {
        match events.recv_timeout(Duration::from_millis(100)) {
            Ok((role, value)) if value["event"] == "error" => {
                let mut details = format!("{role}: {value}\n");
                let collect_until = Instant::now() + Duration::from_millis(100);
                while let Ok((role, value)) = events.recv_timeout(Duration::from_millis(20)) {
                    writeln!(details, "{role}: {value}")?;
                    if Instant::now() >= collect_until {
                        break;
                    }
                }
                return Err(details.into());
            }
            Ok((role, value)) if role == "consumer" && value["event"] == "credit" => {
                let producer = format!("producer{}", value["producer"]);
                workers
                    .get_mut(&producer)
                    .ok_or("unknown producer credit")?
                    .send(&json!({"command":"credit"}))?;
                deadline = Instant::now() + Duration::from_secs(45);
                credits += 1;
                if progress.elapsed() >= Duration::from_secs(30) {
                    eprintln!(
                        "{}",
                        json!({"event":"progress", "credits":credits, "memory":memory(workers)?})
                    );
                    progress = Instant::now();
                }
            }
            Ok(value) => return Ok(value),
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err("event stream closed".into()),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        for (role, worker) in workers.iter_mut() {
            if let Some(status) = worker.child.try_wait()?
                && !status.success()
            {
                return Err(format!("{role} exited: {status}").into());
            }
        }
        if Instant::now() >= deadline {
            return Err("no protocol progress for 45 seconds".into());
        }
    }
}

pub(super) fn memory(workers: &BTreeMap<String, Worker>) -> Result<BTreeMap<String, Value>, Error> {
    workers
        .iter()
        .map(|(role, worker)| {
            let status = std::fs::read_to_string(format!("/proc/{}/status", worker.child.id()))?;
            let mut fields = BTreeMap::new();
            for line in status.lines() {
                if let Some((key, value)) = line.split_once(':')
                    && matches!(key, "VmRSS" | "VmHWM" | "Threads")
                {
                    fields.insert(key, value.trim());
                }
            }
            Ok((role.clone(), json!(fields)))
        })
        .collect()
}

pub(super) fn barrier(
    events: &Receiver<(String, Value)>,
    workers: &mut BTreeMap<String, Worker>,
    roles: &[String],
    event: &str,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut pending: BTreeSet<_> = roles.iter().cloned().collect();
    let mut values = BTreeMap::new();
    while !pending.is_empty() {
        let (role, value) = receive(events, workers)?;
        if value["event"] != event || !pending.remove(&role) {
            return Err(format!("unexpected {role}: {value}").into());
        }
        values.insert(role, value);
    }
    Ok(values)
}

pub(super) fn broadcast(
    workers: &mut BTreeMap<String, Worker>,
    roles: &[String],
    value: &Value,
) -> Result<(), Error> {
    for role in roles {
        workers.get_mut(role).unwrap().send(value)?;
    }
    Ok(())
}
