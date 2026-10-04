//! Seeded, bounded workloads over production SDKs, OMQ inproc and memory storage.

use crate::{broker::Cluster, client::Client};
use futures::FutureExt;
use ozzy_config::Confirmation;
use ozzy_runtime::replicated::SdkClock;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

mod clock;
pub use clock::Time;
mod resources;
pub use resources::Resources;

/// Generated boundaries that can be replayed as a fault-schedule prefix.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Event {
    /// Monotonic wave; record identities derive from this value.
    pub wave: usize,
    /// Selected workload pattern, independent of the fault action.
    pub pattern: usize,
    /// Externally controlled fault or churn boundary.
    pub action: Action,
    /// Protocol-time observation at this boundary, when available in a recording.
    #[serde(default)]
    pub time_millis: u64,
}

/// Full-stack actions. Physical scheduling remains recorded separately.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, clap::ValueEnum)]
pub enum Action {
    /// Live traffic and reader verification.
    Traffic,
    /// A fresh producer shares the live producer's partitions and then departs.
    SharedProducers,
    /// Pause a consumer through a burst, repair gaps and hold returned aliases.
    SlowConsumer,
    /// A paused checkpoint falls behind rolling retention and reopens with an explicit gap.
    RetentionLag,
    /// Stop two voters, retain ambiguous admissions, and restore exact clean images.
    QuorumLoss,
    /// Close/reopen a reader at its independently verified checkpoint.
    Consumer,
    /// Resume a closed producer with its saved identity.
    Resume,
    /// Fence the still-live old producer and begin a new epoch.
    Takeover,
    /// Replace OMQ links and resume with a fresh SDK node identity.
    Reconnect,
    /// Execute physical work while holding its completions independently.
    Completions,
    /// Restart a broker with its retained memory image.
    Restart,
    /// Cut one live memory device while retaining dirty cached bytes.
    ProcessCrash,
    /// Cut one live memory device and restore only modeled durable effects.
    PowerLoss,
    /// Run the shared integration scenario's short-write injection.
    TornWrite,
    /// Fail a physical write and preserve the fail-closed broker evidence.
    /// Supplied through replay for artifact and failure-path qualification.
    FailWrite,
}

/// Controls for a sustained cluster, including an overnight wall-clock duration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Explicit confirmation boundary.
    pub policy: Confirmation,
    /// Workload and fault generator seed.
    pub seed: u64,
    /// Stop generating work after this wall-clock duration.
    pub duration: Duration,
    /// Maximum waves; useful for bounded CI and prefix replay.
    pub waves: usize,
    /// Delay between waves; zero admits naturally batched bursts.
    pub interval: Duration,
    /// Progress deadline for one schedule boundary.
    pub progress_timeout: Duration,
    /// Artifact directory; all files are private to this run.
    pub artifacts: PathBuf,
    /// Optional recorded fault-schedule prefix; threaded delivery is not deterministic.
    pub replay: Option<PathBuf>,
    /// Independently stepped protocol time, bounded by the wall-clock duration.
    pub time: Time,
    /// Owner-local admission and retention limits.
    pub resources: Resources,
    /// Generated fault mix; replay uses its recorded actions instead.
    pub actions: Vec<Action>,
}

/// Coverage evidence for actions actually completed and independently verified.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    /// Verified generated waves.
    pub waves: usize,
    /// Independently confirmed and delivered records.
    pub records: usize,
    /// Consumer close/reopen boundaries.
    pub consumers: usize,
    /// Producer resume or epoch changes.
    pub producers: usize,
    /// Fresh producers confirmed concurrently in shared partitions.
    pub shared_producers: usize,
    /// Paused/slow consumer bursts verified through actual PEER repair.
    pub slow_consumers: usize,
    /// Consumer checkpoints explicitly rejected after crossing a verified retained floor.
    pub retention_gaps: usize,
    /// Loss of two voters followed by restoration; single durable skips this action.
    pub quorum_losses: usize,
    /// Confirmation futures dropped while quorum was absent; admission remained live.
    pub canceled_observations: usize,
    /// New transport identities replacing the previous SDK connections.
    pub reconnects: usize,
    /// Held physical completions observed before their release.
    pub completion_holds: usize,
    /// Broker restarts completed through production recovery.
    pub restarts: usize,
    /// Live memory-device cuts retaining dirty cached bytes.
    pub process_crashes: usize,
    /// Live memory-device cuts restoring only modeled durable effects.
    pub power_losses: usize,
    /// Injected torn-write failures observed by the broker.
    pub torn_writes: usize,
    /// Total storage execution/delivery evidence, including rotated trace windows.
    pub physical_events: u64,
    /// Records verified after authoritative PEER gap repair or replay.
    pub replayed_records: u64,
    /// Records verified directly from live PUB/SUB delivery.
    pub live_records: u64,
    /// Final protocol-time observation; virtual when a manual clock was selected.
    pub time_millis: u64,
    /// Real elapsed run time including startup and shutdown.
    pub elapsed_millis: u64,
}

/// Run until a deadline or wave limit. Save the first failure and its bounded
/// record, fault and physical evidence before returning an error.
pub async fn run(config: &Config) -> Result<Report, String> {
    if config.waves == 0 || config.duration.is_zero() || config.progress_timeout.is_zero() {
        return Err("duration, waves and progress timeout must be positive".into());
    }
    config.time.validate()?;
    config.resources.validate()?;
    if config.actions.is_empty() {
        return Err("fault mix must contain at least one action".into());
    }
    let clock = if config.time.duration.is_some() {
        SdkClock::manual()
    } else {
        SdkClock::default()
    };
    config
        .time
        .drive(&clock, Box::pin(run_cluster(config, &clock)))
        .await
}

async fn run_cluster(config: &Config, clock: &SdkClock) -> Result<Report, String> {
    let started = Instant::now();
    std::fs::create_dir_all(&config.artifacts).map_err(|error| error.to_string())?;
    save_config(config)?;
    let mut schedule = BufWriter::new(create(&config.artifacts.join("schedule.jsonl"))?);
    let mut replay = config
        .replay
        .as_ref()
        .map(|path| File::open(path).map(|file| BufReader::new(file).lines()))
        .transpose()
        .map_err(|error| error.to_string())?;
    let mut cluster = Cluster::configured(
        config.policy,
        config.time.duration.map(|_| clock.clone()),
        |deployment| config.resources.configure(deployment),
    )
    .await;
    let mut client =
        Some(Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()).await);
    let mut reader = Some(client.as_ref().unwrap().reader(true).await);
    let mut report = Report::default();
    let mut recent = VecDeque::with_capacity(128);
    let mut random = config.seed.max(1);
    'waves: for wave in 0..config.waves {
        if started.elapsed() >= config.duration
            || config
                .time
                .duration
                .is_some_and(|duration| clock.now() >= duration)
        {
            break;
        }
        let Some(mut event) = next_event(&mut replay, &mut random, wave, &config.actions)? else {
            break;
        };
        if config.time.duration.is_some() && config.replay.is_some() {
            while clock.now().as_millis() < u128::from(event.time_millis) {
                if started.elapsed() >= config.duration
                    || config
                        .time
                        .duration
                        .is_some_and(|duration| clock.now() >= duration)
                {
                    break 'waves;
                }
                tokio::time::sleep(config.time.tick).await;
            }
        }
        event.time_millis = millis(clock.now());
        serde_json::to_writer(&mut schedule, &event).map_err(|error| error.to_string())?;
        writeln!(schedule).map_err(|error| error.to_string())?;
        schedule.flush().map_err(|error| error.to_string())?;
        if recent.len() == 128 {
            recent.pop_front();
        }
        recent.push_back(event);
        let operation = boundary(&mut cluster, &mut client, &mut reader, event, &mut report);
        let error = boundary_error(config.progress_timeout, event, operation).await;
        if let Some(error) = error {
            report.time_millis = millis(clock.now());
            report.elapsed_millis = millis(started.elapsed());
            report.physical_events = cluster.physical_events();
            save_failure(config, &cluster, client.as_ref(), &recent, &report, &error).await?;
            drop(reader.take());
            drop(client.take());
            stop_after_failure(cluster).await;
            return Err(error);
        }
        report.waves += 1;
        tokio::time::sleep(config.interval).await;
    }
    reader
        .take()
        .unwrap()
        .close()
        .await
        .map_err(|error| error.to_string())?;
    client.take().unwrap().close().await;
    report.physical_events = cluster.physical_events();
    cluster.shutdown().await;
    report.time_millis = millis(clock.now());
    report.elapsed_millis = millis(started.elapsed());
    serde_json::to_writer_pretty(
        BufWriter::new(create(&config.artifacts.join("report.json"))?),
        &report,
    )
    .map_err(|error| error.to_string())?;
    Ok(report)
}

async fn boundary_error(
    deadline: Duration,
    event: Event,
    operation: impl std::future::Future<Output = ()>,
) -> Option<String> {
    match tokio::time::timeout(
        deadline,
        std::panic::AssertUnwindSafe(operation).catch_unwind(),
    )
    .await
    {
        Ok(Ok(())) => None,
        Ok(Err(panic)) => Some(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    panic
                        .downcast_ref::<&str>()
                        .map(|value| (*value).to_owned())
                })
                .unwrap_or_else(|| "simulation panicked".into()),
        ),
        Err(_) => Some(format!("progress deadline at {event:?}")),
    }
}

fn next_event(
    replay: &mut Option<std::io::Lines<BufReader<File>>>,
    random: &mut u64,
    wave: usize,
    actions: &[Action],
) -> Result<Option<Event>, String> {
    let event = if let Some(replay) = replay {
        let Some(line) = replay.next() else {
            return Ok(None);
        };
        serde_json::from_str(&line.map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?
    } else {
        let action = actions[(draw(random) as usize) % actions.len()];
        Event {
            wave,
            pattern: (draw(random) as usize) % 6,
            action,
            time_millis: 0,
        }
    };
    if event.wave != wave || event.pattern >= 6 {
        return Err("replay must contain consecutive waves and workload patterns 0..5".into());
    }
    Ok(Some(event))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn draw(random: &mut u64) -> u64 {
    *random ^= *random << 13;
    *random ^= *random >> 7;
    *random ^= *random << 17;
    *random
}

fn save_config(config: &Config) -> Result<(), String> {
    let evidence = serde_json::json!({
        "seed": config.seed, "mode": format!("{:?}", config.policy),
        "wall_duration": config.duration, "waves": config.waves,
        "interval": config.interval, "progress_timeout": config.progress_timeout,
        "replay": config.replay, "time": config.time, "resources": config.resources,
        "fault_mix": config.actions,
    });
    serde_json::to_writer_pretty(
        BufWriter::new(create(&config.artifacts.join("config.json"))?),
        &evidence,
    )
    .map_err(|error| error.to_string())
}

/// Default weighted mix. Repeating an action increases its selection frequency.
pub fn default_actions() -> Vec<Action> {
    vec![
        Action::Traffic,
        Action::Consumer,
        Action::Resume,
        Action::Takeover,
        Action::Reconnect,
        Action::Completions,
        Action::Restart,
        Action::ProcessCrash,
        Action::PowerLoss,
        Action::TornWrite,
        Action::SharedProducers,
        Action::SlowConsumer,
        Action::RetentionLag,
        Action::QuorumLoss,
    ]
}

async fn boundary(
    cluster: &mut Cluster,
    client: &mut Option<Client>,
    reader: &mut Option<ozzy_runtime::replicated::TopicReader>,
    event: Event,
    report: &mut Report,
) {
    match event.action {
        Action::Consumer | Action::Reconnect => {
            reader.take().unwrap().close().await.unwrap();
            if matches!(event.action, Action::Reconnect) {
                *client = Some(
                    client
                        .take()
                        .unwrap()
                        .reconnect(&cluster.runtime, true)
                        .await,
                );
                report.reconnects += 1;
            }
            *reader = Some(client.as_ref().unwrap().reader(true).await);
            report.consumers += 1;
        }
        Action::Resume | Action::Takeover => {
            *client = Some(
                client
                    .take()
                    .unwrap()
                    .reopen_producer(matches!(event.action, Action::Takeover))
                    .await,
            );
            report.producers += 1;
        }
        Action::Restart => {
            cluster.restart(event.wave % cluster.brokers.len()).await;
            cluster
                .wait_recovered(event.wave % cluster.brokers.len())
                .await;
            report.restarts += 1;
        }
        Action::ProcessCrash | Action::PowerLoss => {
            let index = event.wave % cluster.brokers.len();
            let power_loss = matches!(event.action, Action::PowerLoss);
            cluster.crash(index, power_loss).await;
            cluster.wait_recovered(index).await;
            report.restarts += 1;
            report.power_losses += usize::from(power_loss);
            report.process_crashes += usize::from(!power_loss);
        }
        Action::Traffic
        | Action::SharedProducers
        | Action::SlowConsumer
        | Action::RetentionLag
        | Action::QuorumLoss
        | Action::Completions
        | Action::TornWrite
        | Action::FailWrite => {}
    }
    workload(
        cluster,
        client.as_mut().unwrap(),
        reader.as_mut().unwrap(),
        event,
        report,
    )
    .await;
}

async fn workload(
    cluster: &mut Cluster,
    client: &mut Client,
    reader: &mut ozzy_runtime::replicated::TopicReader,
    event: Event,
    report: &mut Report,
) {
    let before = reader.stats();
    if matches!(event.action, Action::RetentionLag) {
        report.records += client.retention_lag(reader, event.wave).await;
        report.retention_gaps += 1;
    } else if matches!(event.action, Action::SlowConsumer) {
        let (records, replayed, live) = client.slow_consumer(reader, event.wave).await;
        report.records += records;
        report.replayed_records += replayed;
        report.live_records += live;
        report.slow_consumers += 1;
    } else if matches!(event.action, Action::QuorumLoss) && cluster.brokers.len() == 3 {
        let records = cluster.quorum_loss(client, reader, event.wave).await;
        report.records += records;
        report.canceled_observations += records;
        report.quorum_losses += 1;
        report.restarts += 2;
    } else if matches!(event.action, Action::SharedProducers) {
        let positions = client.positions();
        report.records += client.shared_producers(event.wave).await;
        client.read(reader, positions).await;
        report.shared_producers += 1;
    } else if matches!(event.action, Action::FailWrite) {
        cluster.controls[0].fail_next_record_write();
        report.records += cluster
            .verify_wave(client, reader, event.wave * 6 + 1)
            .await;
    } else if matches!(event.action, Action::TornWrite) {
        let before = cluster.faults;
        report.records += cluster
            .verify_wave(client, reader, event.wave * 6 + 2)
            .await;
        report.torn_writes += cluster.faults - before;
    } else {
        let positions = client.positions();
        let release = matches!(event.action, Action::Completions)
            .then(|| delayed_completions(cluster.controls.clone()));
        let pending = client.queue_varied(event.wave * 6 + event.pattern).await;
        if let Some(release) = release {
            assert!(
                release.await.unwrap(),
                "no held physical completion observed"
            );
            report.completion_holds += 1;
        }
        report.records += pending.len();
        client.confirm(pending).await;
        client.read(reader, positions).await;
    }
    // At most one bounded wave of payload evidence survives into the next turn.
    client.discard_verified();
    let after = reader.stats();
    report.replayed_records += after.replayed_records - before.replayed_records;
    report.live_records += after.live_records - before.live_records;
}

async fn stop_after_failure(cluster: Cluster) {
    for control in &cluster.controls {
        control.hold_record_writes(false);
        control.hold_completions(false);
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), async move {
        for broker in cluster.brokers {
            let _ = broker.shutdown().await;
        }
        for image in cluster.images {
            let _ = image.await;
        }
    })
    .await;
}

fn delayed_completions(
    controls: Vec<std::sync::Arc<crate::broker::Control>>,
) -> tokio::task::JoinHandle<bool> {
    for control in &controls {
        control.hold_completions(true);
    }
    tokio::spawn(async move {
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            while controls
                .iter()
                .all(|control| control.pending_completions() == 0)
            {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        })
        .await
        .is_ok();
        for control in &controls {
            control.hold_completions(false);
        }
        observed
    })
}

fn create(path: &Path) -> Result<File, String> {
    File::create(path).map_err(|error| error.to_string())
}

async fn save_failure(
    config: &Config,
    cluster: &Cluster,
    client: Option<&Client>,
    recent: &VecDeque<Event>,
    report: &Report,
    error: &str,
) -> Result<(), String> {
    let evidence = serde_json::json!({
        "seed": config.seed, "mode": format!("{:?}", config.policy),
        "error": error, "events": recent, "coverage": report,
        "time": config.time, "resources": config.resources, "fault_mix": config.actions,
        "record_evidence": client.map(Client::evidence),
        "configuration": cluster.configs.iter().map(|(checked, _)| format!("{checked:?}")).collect::<Vec<_>>(),
        "physical": cluster.storage_controls().iter().map(|(index, control)|
            serde_json::json!({"broker":index,"events":format!("{:?}", control.recent_trace())})).collect::<Vec<_>>(),
    });
    serde_json::to_writer_pretty(
        BufWriter::new(create(&config.artifacts.join("failure.json"))?),
        &evidence,
    )
    .map_err(|error| error.to_string())?;
    for (index, control) in cluster.storage_controls() {
        if let Ok(image) = tokio::time::timeout(Duration::from_secs(2), control.image()).await {
            serde_json::to_writer(
                BufWriter::new(create(
                    &config.artifacts.join(format!("image-{index}.json")),
                )?),
                &image,
            )
            .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}
