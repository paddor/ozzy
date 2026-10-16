//! Group members acknowledge every delivery and verify one full topic per group.

mod state;

use super::super::{measurement::Histogram, native::Setup};
use super::oracle::Record;
use super::{
    Config, GroupId, Result, Value, Window, error, launch, metrics, report, try_join_all, watch,
    workload,
};
use ozzy_proto::{ConsumerMemberId, PartitionIncarnation};
use ozzy_runtime::{
    replicated::{
        BrokerLinks, GroupReader, GroupReaderConfig, GroupRecord, PendingProgress, ProgressError,
        ProgressReceipt,
    },
    topic_metadata::TopicMetadata,
};
use serde_json::json;
use std::{cell::RefCell, collections::VecDeque, fmt::Display, rc::Rc, time::Duration};

/// Member heartbeat interval, well inside the broker membership timeout.
const HEARTBEAT: Duration = Duration::from_millis(100);

/// One group copy's verifier, shared by that copy's member tasks.
type Verifier = Rc<RefCell<state::State>>;

/// A confirmation outcome for one acknowledgment.
type Confirmation = std::result::Result<ProgressReceipt, ProgressError>;

struct Task {
    reader: GroupReader,
    state: Verifier,
    copy: usize,
    member: usize,
}
struct Pending {
    progress: PendingProgress,
    delivered: u64,
}
struct Confirmations {
    latency: Histogram,
    count: u64,
    measured: u64,
    fenced: u64,
}

impl Confirmations {
    fn observe(
        &mut self,
        pending: &Pending,
        receipt: Confirmation,
        state: &Verifier,
        window: Window,
    ) -> Result<()> {
        match receipt {
            Ok(receipt) => {
                let now = metrics::monotonic_ns();
                state
                    .borrow_mut()
                    .confirm(receipt.identity.partition, receipt.cursor.get())?;
                self.count += 1;
                self.measured += u64::from(window.contains(now));
                if window.contains(pending.delivered) {
                    self.latency.record(now.saturating_sub(pending.delivered));
                }
            }
            Err(ProgressError::Fenced) => self.fenced += 1,
            Err(cause) => return Err(cause.into()),
        }
        Ok(())
    }
}

/// What every member task of one reader worker shares.
#[derive(Clone, Copy)]
struct Shared<'a> {
    config: &'a Config,
    metadata: &'a TopicMetadata,
    group: GroupId,
    window: Window,
}

pub(super) async fn run(
    config: Config,
    index: usize,
    value: &Value,
    mut input: tokio::sync::mpsc::Receiver<Result<Value>>,
) -> Result<()> {
    let runtime = super::super::native::sdk_runtime(&config)?;
    let setup = Setup::parse(&config, value)?;
    let links = setup
        .reader_links(&runtime, &config, &value["reader"])
        .await?;
    let metadata = links.topic(&setup.topic).await?;
    if metadata.partition_count() != setup.partitions
        || metadata.policy() != config.args.system.policy()
    {
        return Err(error("group topic topology mismatch"));
    }
    let readers = open_members(&config, &links, &setup, index).await?;
    launch::reply(&json!({"event":"connected", "index":index, "pid":std::process::id()}))?;
    let start = super::command(&mut input, "start").await?;
    let window = super::super::window(&start, &config.args)?;
    let group = super::config::group(&config.args)?;
    let incarnations = incarnations(&metadata, setup.partitions)?;
    let Verification { copies, tasks } = verifiers(&config, window, readers, &incarnations)?;
    window.wait().await;
    let meter = metrics::Meter::start(!config.args.inproc);
    let (finish, finished) = watch::channel(None);
    let shared = Shared {
        config: &config,
        metadata: &metadata,
        group,
        window,
    };
    let work = try_join_all(
        tasks
            .into_iter()
            .map(|task| run_task(task, shared, finished.clone())),
    );
    let completion = super::final_counts(&mut input, finish, &config, window);
    let (members, ()) = tokio::try_join!(work, completion)?;
    let usage = meter.finish();
    links.shutdown().await?;
    let mut lanes = Vec::new();
    let mut groups = Vec::new();
    for (copy, state) in copies {
        let state = Rc::into_inner(state)
            .ok_or_else(|| error("group verifier outlived its members"))?
            .into_inner();
        groups.push(state.detail(copy));
        lanes.extend(state.oracle.rows());
    }
    report(
        &config,
        &json!({"lanes":lanes,"group_members":members,"consumer_groups":groups,"usage":usage,"index":index,"pid":std::process::id()}),
    )?;
    if input.recv().await.is_some() {
        return Err(error("unexpected group reader command"));
    }
    Ok(())
}

/// Open every member of the groups assigned to reader worker `index`.
async fn open_members(
    config: &Config,
    links: &BrokerLinks,
    setup: &Setup,
    index: usize,
) -> Result<Vec<(usize, Vec<GroupReader>)>> {
    let assigned = workload::worker_share(
        config.args.native.groups as usize,
        config.args.reader_workers(),
        index,
    )?;
    let mut readers = Vec::new();
    for copy in assigned {
        let mut members = Vec::new();
        for member in 0..config.args.native.group_members as usize {
            let mut settings = GroupReaderConfig::new(member_identity(copy, member));
            settings.window = config.args.native.ack_window as usize;
            settings.heartbeat = HEARTBEAT;
            members.push(
                GroupReader::open(
                    links.clone(),
                    &setup.topic,
                    &format!("benchmark-group-{copy}"),
                    settings,
                )
                .await?,
            );
        }
        readers.push((copy, members));
    }
    Ok(readers)
}

/// Group copy, then member number, both counted from one.
fn member_identity(copy: usize, member: usize) -> ConsumerMemberId {
    let mut identity = [0; 16];
    identity[..8].copy_from_slice(&(copy as u64 + 1).to_be_bytes());
    identity[8..].copy_from_slice(&(member as u64 + 1).to_be_bytes());
    ConsumerMemberId::from_bytes(identity)
}

/// Current incarnations of every topic partition, by number.
fn incarnations(metadata: &TopicMetadata, partitions: usize) -> Result<Vec<PartitionIncarnation>> {
    (0..partitions as u32)
        .map(|number| {
            metadata
                .partition(number)
                .map(|partition| partition.incarnation)
                .ok_or_else(|| error("group topic partition missing from metadata"))
        })
        .collect()
}

/// Each group copy's verifier, and every member task across copies.
struct Verification {
    copies: Vec<(usize, Verifier)>,
    tasks: Vec<Task>,
}

/// One verifier per group copy, and a task per member that shares it.
fn verifiers(
    config: &Config,
    window: Window,
    readers: Vec<(usize, Vec<GroupReader>)>,
    incarnations: &[PartitionIncarnation],
) -> Result<Verification> {
    let mut copies = Vec::new();
    let mut tasks = Vec::new();
    for (copy, members) in readers {
        let state = Rc::new(RefCell::new(state::State::new(
            config,
            copy,
            window,
            incarnations.to_vec(),
        )?));
        for (member, reader) in members.into_iter().enumerate() {
            tasks.push(Task {
                reader,
                state: state.clone(),
                copy,
                member,
            });
        }
        copies.push((copy, state));
    }
    Ok(Verification { copies, tasks })
}

async fn run_task(
    task: Task,
    shared: Shared<'_>,
    mut finished: watch::Receiver<Option<Vec<u64>>>,
) -> Result<Value> {
    let Task {
        mut reader,
        state,
        copy,
        member,
    } = task;
    let mut member = Member::new(state, copy, member, shared.config)?;
    let deadline = shared.window.deadline(&shared.config.args);
    tokio::time::timeout_at(deadline, async {
        member.run(&mut reader, shared, &mut finished).await?;
        let (copy, index) = (member.copy, member.index);
        reader.close().await.map_err(|cause| {
            error(format!(
                "group copy={copy} member={index} close failed: {cause}"
            ))
        })
    })
    .await
    .map_err(|cause| member.timed_out(&cause))??;
    Ok(member.report(shared.window))
}

/// One group member's acknowledgment loop and its counters.
struct Member {
    state: Verifier,
    copy: usize,
    /// Member number within its group copy.
    index: usize,
    pending: VecDeque<Pending>,
    /// Unconfirmed acknowledgments allowed before waiting for the oldest.
    pending_limit: usize,
    confirmations: Confirmations,
    delivered: u64,
    redelivered: u64,
}

impl Member {
    fn new(state: Verifier, copy: usize, index: usize, config: &Config) -> Result<Self> {
        let pending_limit = config
            .partitions()
            .checked_mul(config.args.native.ack_window as usize)
            .ok_or_else(|| error("group confirmation window overflow"))?;
        Ok(Self {
            state,
            copy,
            index,
            pending: VecDeque::new(),
            pending_limit,
            confirmations: Confirmations {
                latency: Histogram::new(),
                count: 0,
                measured: 0,
                fenced: 0,
            },
            delivered: 0,
            redelivered: 0,
        })
    }

    /// Deliver, verify and acknowledge until the group has every final
    /// record confirmed.
    async fn run(
        &mut self,
        reader: &mut GroupReader,
        shared: Shared<'_>,
        finished: &mut watch::Receiver<Option<Vec<u64>>>,
    ) -> Result<()> {
        let window = shared.window;
        let mut changed = self.state.borrow().subscribe();
        loop {
            self.observe_ready(window)?;
            let target = finished.borrow().clone();
            if self.state.borrow().complete(target.as_deref())? && self.pending.is_empty() {
                break;
            }
            if self.pending.len() >= self.pending_limit
                && let Some(oldest) = self.pending.pop_front()
            {
                let receipt = oldest.progress.confirmed().await;
                self.confirmations
                    .observe(&oldest, receipt, &self.state, window)?;
                continue;
            }
            let record = tokio::select! {
                biased;
                record = reader.next() => record.map_err(|cause| self.next_failed(&cause))?,
                Some(receipt) = oldest_confirmed(&self.pending), if !self.pending.is_empty() => {
                    self.observe_oldest(receipt, window)?;
                    continue;
                }
                result = finished.changed(), if target.is_none() => {
                    result.map_err(|_| error("final group counts unavailable"))?;
                    continue;
                }
                result = changed.changed() => {
                    result.map_err(|_| error("group verifier closed"))?;
                    continue;
                }
            };
            self.accept(reader, record, shared)?;
            if self.delivered.is_multiple_of(256) {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }

    /// Verify one delivery against the group oracle, then acknowledge it.
    fn accept(
        &mut self,
        reader: &mut GroupReader,
        record: GroupRecord,
        shared: Shared<'_>,
    ) -> Result<()> {
        let delivered = Record::delivered(shared.metadata, &record.record)?;
        let now = metrics::monotonic_ns();
        let redelivered =
            self.state
                .borrow_mut()
                .observe(shared.config, shared.group, delivered, now)?;
        self.redelivered += u64::from(redelivered);
        self.delivered += 1;
        match reader.acknowledge(record.token) {
            Ok(progress) => self.pending.push_back(Pending {
                progress,
                delivered: now,
            }),
            Err(ProgressError::Fenced) => self.confirmations.fenced += 1,
            Err(cause) => return Err(cause.into()),
        }
        Ok(())
    }

    /// Complete every acknowledgment whose confirmation is already ready.
    fn observe_ready(&mut self, window: Window) -> Result<()> {
        while let Some(receipt) = self
            .pending
            .front()
            .and_then(|pending| pending.progress.try_confirmed())
        {
            self.observe_oldest(receipt, window)?;
        }
        Ok(())
    }

    fn observe_oldest(&mut self, receipt: Confirmation, window: Window) -> Result<()> {
        if let Some(oldest) = self.pending.pop_front() {
            self.confirmations
                .observe(&oldest, receipt, &self.state, window)?;
        }
        Ok(())
    }

    fn next_failed(&self, cause: &impl Display) -> Box<dyn std::error::Error + Send + Sync> {
        error(format!(
            "group copy={} member={} next failed after delivered={} confirmed={} state={}: {cause}",
            self.copy,
            self.index,
            self.delivered,
            self.confirmations.count,
            self.state.borrow().detail(self.copy),
        ))
    }

    fn timed_out(&self, cause: &impl Display) -> Box<dyn std::error::Error + Send + Sync> {
        error(format!(
            "group copy={} member={} delivered={} confirmed={} fenced={} redelivered={} pending={} state={} timed out: {cause}",
            self.copy,
            self.index,
            self.delivered,
            self.confirmations.count,
            self.confirmations.fenced,
            self.redelivered,
            self.pending.len(),
            self.state.borrow().detail(self.copy),
        ))
    }

    fn report(&self, window: Window) -> Value {
        let seconds = (window.end - window.measured) as f64 / 1e9;
        json!({"copy":self.copy,"member":self.index,"delivered":self.delivered,
            "handoff_redelivered":self.redelivered,"confirmed_records":self.confirmations.count,
            "confirmed_records_per_second":self.confirmations.measured as f64 / seconds,
            "fenced_acknowledgments":self.confirmations.fenced,
            "processing_confirmation":self.confirmations.latency.report()})
    }
}

/// The oldest acknowledgment's confirmation, once one is pending.
async fn oldest_confirmed(pending: &VecDeque<Pending>) -> Option<Confirmation> {
    Some(pending.front()?.progress.confirmed().await)
}
