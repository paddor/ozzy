//! Opt-in cross-host crash soak. Reuses the process suite's SDK record oracle.

use super::Client;

mod fleet;
mod single;
use fleet::Fleet;
use ozzy_broker::{CheckedConfig, check_config, host_resources, load_deployment};
use std::{
    fs::{self, File},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::{Duration, Instant},
};

struct Broker {
    host: String,
    child: Child,
    pid: u32,
    log: PathBuf,
    executable: PathBuf,
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn remote(host: &str, script: &str) -> Command {
    let mut command = Command::new("ssh");
    command
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            host,
            "bash",
            "-c",
        ])
        .arg(quoted(script));
    command
}

impl Broker {
    async fn start(
        host: &str,
        name: &str,
        directory: &Path,
        logs: &Path,
        recovering: bool,
        round: u64,
        cpu: usize,
    ) -> Self {
        let operation = if recovering { "recover" } else { "serve" };
        let selections = if recovering {
            " --quarantine orders/0 --quarantine orders/1 --quarantine orders/2 --quarantine orders/3"
        } else {
            ""
        };
        let directory_text = quoted(directory.to_str().unwrap());
        let reset = remote(host, &format!("rm -f {directory_text}/broker.pid"))
            .output()
            .unwrap();
        assert!(reset.status.success());
        let memory = std::env::var("OZZY_SOAK_MEMORY").is_ok_and(|value| value == "1");
        let executable = directory.join(if memory {
            "memory-broker"
        } else {
            "ozzy_broker"
        });
        let invocation = if memory {
            format!(
                "env OZZY_MEMORY_CONFIG=deployment.toml OZZY_MEMORY_BROKER={} \
                OZZY_MEMORY_RECOVERING={} ./memory-broker --ignored --exact \
                journals::serving::simulated::churn::host::memory_only_broker_process --nocapture",
                quoted(name),
                usize::from(recovering)
            )
        } else {
            format!(
                "./ozzy_broker --config deployment.toml {operation} --broker {} \
                --identity shared.identity --local-identity local.identity \
                --trusted-transport{selections}",
                quoted(name)
            )
        };
        let script = format!(
            "cd {directory_text} && echo $$ > broker.pid && exec taskset -c {cpu} {invocation}"
        );
        let log = logs.join(format!("{name}-{round}.log"));
        let output = File::create(&log).unwrap();
        let child = remote(host, &script)
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        let mut broker = Self {
            host: host.to_owned(),
            child,
            pid: 0,
            log,
            executable,
        };
        let output = remote(
            host,
            &format!(
                "for attempt in {{1..100}}; do \
                if test -s {directory_text}/broker.pid; then \
                cat {directory_text}/broker.pid; exit; fi; sleep 0.05; done; exit 1"
            ),
        )
        .output()
        .unwrap();
        assert!(output.status.success());
        broker.pid = String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                broker.check();
                if fs::read_to_string(&broker.log)
                    .unwrap()
                    .contains("Serving broker ")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        broker
    }

    fn check(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            panic!(
                "{} exited {status}: {}",
                self.host,
                fs::read_to_string(&self.log).unwrap()
            );
        }
    }

    fn signal(&self, signal: &str) {
        assert!(self.pid > 0);
        let script = format!(
            "test \"$(readlink /proc/{}/exe)\" = {} && kill -{signal} {}",
            self.pid,
            quoted(self.executable.to_str().unwrap()),
            self.pid
        );
        let output = remote(&self.host, &script).output().unwrap();
        assert!(
            output.status.success(),
            "{}: {}",
            self.host,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn stop(mut self, signal: &str) {
        self.signal(signal);
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    if signal == "TERM" {
                        assert!(status.success());
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        self.pid = 0;
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if self.pid != 0 && self.child.try_wait().ok().flatten().is_none() {
            // Only this run's exact executable/PID can be stopped during unwind.
            let script = format!(
                "test \"$(readlink /proc/{}/exe)\" = {} && kill -KILL {}",
                self.pid,
                quoted(self.executable.to_str().unwrap()),
                self.pid
            );
            let _ = remote(&self.host, &script).status();
            let _ = self.child.wait();
        }
    }
}

async fn observe<T>(
    brokers: &mut [Option<Broker>],
    label: &str,
    future: impl Future<Output = T>,
) -> T {
    observe_progress(brokers, label, std::sync::Arc::default(), future).await
}

async fn observe_progress<T>(
    brokers: &mut [Option<Broker>],
    label: &str,
    progress: std::sync::Arc<std::sync::atomic::AtomicU64>,
    future: impl Future<Output = T>,
) -> T {
    let initial = progress.load(std::sync::atomic::Ordering::Relaxed);
    let started = Instant::now();
    let mut next_report = Duration::from_secs(60);
    let mut future = std::pin::pin!(ozzy_sim::client::progress_timeout(
        Duration::from_secs(30),
        progress.clone(),
        future
    ));
    loop {
        tokio::select! {
            result = &mut future => return result.unwrap_or_else(|error| panic!("{label}: {error}")),
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                for broker in brokers.iter_mut().flatten() { broker.check(); }
                if started.elapsed() >= next_report {
                    let count = progress.load(std::sync::atomic::Ordering::Relaxed) - initial;
                    println!("{{\"event\":\"boundary-progress\",\"operation\":\"{label}\",\"verified_records\":{count}}}");
                    next_report = started.elapsed() + Duration::from_secs(60);
                }
            }
        }
    }
}

fn checked(config: &Path, identity: &Path, host: &str) -> CheckedConfig {
    check_config(
        load_deployment(config).unwrap(),
        identity,
        host,
        &host_resources().unwrap(),
    )
    .unwrap()
}

fn duration_secs() -> u64 {
    let seconds = std::env::var("OZZY_SOAK_SECONDS")
        .unwrap_or_else(|_| "3600".into())
        .parse::<u64>()
        .unwrap();
    assert!(seconds > 0);
    seconds
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a freshly provisioned OZZY_SOAK_CONFIG and OZZY_SOAK_DIRS on si-dev/wu-dev/er-dev"]
async fn three_host_crash_soak() {
    run_crash_soak().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires one provisioned durable broker and a remote SDK controller"]
async fn cross_host_single_crash_soak() {
    run_crash_soak().await;
}

async fn run_crash_soak() {
    let seconds = duration_secs();
    let mut fleet = Fleet::start().await;
    let policy = fleet.checked.deployment.deployment().topics["orders"].confirmation;
    let mut ledger = File::create(std::env::var("OZZY_SOAK_RESULTS").unwrap()).unwrap();
    let mut client = observe(
        &mut fleet.brokers,
        "open soak SDK",
        Client::open(&fleet.checked),
    )
    .await;
    let mut reader = observe(
        &mut fleet.brokers,
        "open live soak reader",
        client.reader(false),
    )
    .await;
    let start = Instant::now();
    let mut next_report = Duration::ZERO;
    let mut next_resume = Duration::from_secs(30);
    let mut next_maintenance = Duration::from_secs(120);
    let (mut resumes, mut takeovers) = (0, 0);
    let mut wave = 0;
    let mut verified = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        let elapsed = start.elapsed();
        fleet.churn(elapsed).await;
        if elapsed >= next_resume {
            println!(
                "reopen at {} seconds, takeover={}",
                elapsed.as_secs(),
                fleet.kills % 2 == 1
            );
            client = observe(
                &mut fleet.brokers,
                "resume saved identity",
                Box::pin(client.reopen_producer(fleet.kills % 2 == 1)),
            )
            .await;
            if fleet.kills % 2 == 1 {
                takeovers += 1;
            } else {
                resumes += 1;
            }
            next_resume = elapsed + Duration::from_secs(30);
        }
        if elapsed >= next_maintenance
            && let Some(records) = fleet.maintenance(&mut client, &mut reader, wave).await
        {
            verified += records;
            next_maintenance = start.elapsed() + Duration::from_secs(300);
        }
        let (updated_reader, records) = fleet.verify_wave(&mut client, reader, wave).await;
        reader = updated_reader;
        client.discard_verified();
        wave += 1;
        verified += records;
        if elapsed >= next_report {
            let kills = fleet.kills;
            let record = format!(
                "{{\"policy\":\"{policy:?}\",\"elapsed_secs\":{},\"verified_records\":{},\"kills\":{kills},\"resumes\":{resumes},\"takeovers\":{takeovers},\"consumers\":{},\"retention_gaps\":{},\"slow_consumers\":{},\"replayed_records\":{}}}",
                elapsed.as_secs(),
                verified,
                fleet.consumers,
                fleet.retention_gaps,
                fleet.slow_consumers,
                fleet.replayed_records,
            );
            writeln!(ledger, "{record}").unwrap();
            ledger.flush().unwrap();
            println!("{record}");
            next_report = elapsed + Duration::from_secs(60);
        }
        tokio::time::sleep(Duration::from_millis(if wave % 6 == 0 { 150 } else { 10 })).await;
    }
    observe(&mut fleet.brokers, "close live soak reader", reader.close())
        .await
        .unwrap();
    observe(&mut fleet.brokers, "close soak SDK", client.close()).await;
    let kills = fleet.kills;
    let consumers = fleet.consumers;
    let retention_gaps = fleet.retention_gaps;
    let slow_consumers = fleet.slow_consumers;
    let replayed_records = fleet.replayed_records;
    for broker in fleet.brokers.into_iter().flatten() {
        broker.stop("TERM").await;
    }
    writeln!(
        ledger,
        "{{\"complete\":true,\"elapsed_secs\":{},\"verified_records\":{},\"kills\":{kills},\"resumes\":{resumes},\"takeovers\":{takeovers},\"consumers\":{consumers},\"retention_gaps\":{retention_gaps},\"slow_consumers\":{slow_consumers},\"replayed_records\":{replayed_records}}}",
        start.elapsed().as_secs(),
        verified
    )
    .unwrap();
}
