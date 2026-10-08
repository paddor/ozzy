//! Bounded benchmark orchestration over OMQ. Never stdin, stdout, or marker files.
//!
//! Pumps are application-runtime tasks. All sockets share the process's owned
//! OMQ context; transport work never borrows the application runtime.

use std::cell::RefCell;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Context, ContextConfig, Message, Options, Socket, SocketType};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::{BenchResult, bench_error};

mod pump;
#[cfg(test)]
mod tests;
mod wire;
use pump::pump;
use wire::{Frame, Kind, Route};

const QUEUE: usize = 4;
const CHUNK: usize = 64 * 1024;
/// Maximum complete JSON control report retained by one connection.
pub const MAX_REPORT_BYTES: usize = 128 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(30);
const CONTROLLER: [u8; 16] = [0xca; 16];

#[derive(Clone, Debug, Default, clap::Args)]
#[group(id = "BenchmarkControl")]
/// OMQ benchmark-controller endpoint and exact run/worker identities.
pub struct Args {
    /// Explicit reachable TCP bind for remote workers; local control uses abstract IPC.
    #[arg(long)]
    pub control_bind: Option<String>,
    #[arg(long, hide = true)]
    /// Controller endpoint supplied to a spawned benchmark worker.
    pub control_endpoint: Option<String>,
    #[arg(long, hide = true)]
    /// Run identity required to accept controller frames.
    pub control_run: Option<Uuid>,
    #[arg(long, hide = true)]
    /// Worker identity required to route controller frames.
    pub control_worker: Option<Uuid>,
}

impl Args {
    /// Append configured hidden control arguments to a worker command.
    pub fn append(&self, arguments: &mut Vec<String>) {
        if let Some(endpoint) = &self.control_endpoint {
            arguments.extend(["--control-endpoint".into(), endpoint.clone()]);
        }
        if let Some(run) = self.control_run {
            arguments.extend(["--control-run".into(), run.to_string()]);
        }
        if let Some(worker) = self.control_worker {
            arguments.extend(["--control-worker".into(), worker.to_string()]);
        }
    }
}

tokio::task_local! {
    static CONTEXT: Context;
    static WORKER: Worker;
}

struct Worker {
    input: RefCell<Option<mpsc::Receiver<BenchResult<Value>>>>,
    output: mpsc::Sender<Outgoing>,
}

/// Reuse the process context inside a benchmark scope. Tests outside a scope
/// still get an owned background I/O thread, never `Context::current()`.
pub fn context() -> Context {
    CONTEXT
        .try_with(Clone::clone)
        .unwrap_or_else(|_| Context::with_name("ozzy/omq"))
}

/// Establish execution placement before entering any benchmark role.
pub async fn run<T>(
    args: &Args,
    io_threads: usize,
    work: impl Future<Output = BenchResult<T>>,
) -> BenchResult<T> {
    if !(1..=32).contains(&io_threads) {
        return Err(bench_error("OMQ I/O threads must be between 1 and 32"));
    }
    let context = Context::with_config_and_name(ContextConfig { io_threads }, "ozzy/omq");
    CONTEXT
        .scope(context.clone(), async {
            if args.control_endpoint.is_none() {
                if args.control_run.is_some() || args.control_worker.is_some() {
                    return Err(bench_error("incomplete benchmark control identity"));
                }
                return work.await;
            }
            let mut connection = Connection::connect(&context, args).await?;
            let worker = Worker {
                input: RefCell::new(Some(connection.take_input()?)),
                output: connection.output.clone(),
            };
            let result = tokio::select! {
                result = WORKER.scope(worker, work) => result,
                result = &mut connection.task => {
                    result??;
                    Err(bench_error("worker control stopped before completion"))
                }
            };
            if result.is_ok() {
                connection.finish().await?;
            } else if let Err(error) = &result {
                // Deliver the cause before closing transport. Stderr is only a
                // diagnostic artifact and can be written after disconnect.
                let _ = connection
                    .complete(Kind::Failed, Value::String(error.to_string()))
                    .await;
            }
            result
        })
        .await
}

/// Take the single control receiver within a configured worker scope; panics outside that scope.
pub fn input() -> mpsc::Receiver<BenchResult<Value>> {
    WORKER.with(|worker| {
        worker
            .input
            .borrow_mut()
            .take()
            .expect("one control receiver per worker")
    })
}

/// Queue a worker reply, failing when control is unconfigured, full, or closed.
pub fn reply(value: &Value) -> BenchResult<()> {
    WORKER
        .try_with(|worker| enqueue(&worker.output, Kind::Data, value.clone()))
        .map_err(|_| bench_error("worker control is not configured"))?
}

/// Receive the expected controller command under the control deadline.
pub async fn command(
    input: &mut mpsc::Receiver<BenchResult<Value>>,
    expected: &str,
) -> BenchResult<Value> {
    let value = tokio::time::timeout(DEADLINE, input.recv())
        .await?
        .ok_or_else(|| bench_error("benchmark controller closed"))??;
    if value["command"] != expected {
        return Err(bench_error(format!(
            "expected benchmark command {expected}"
        )));
    }
    Ok(value)
}

#[derive(Debug)]
struct Outgoing {
    kind: Kind,
    value: Value,
    completion: Option<oneshot::Sender<BenchResult<()>>>,
}

fn enqueue(output: &mpsc::Sender<Outgoing>, kind: Kind, value: Value) -> BenchResult<()> {
    output
        .try_send(Outgoing {
            kind,
            value,
            completion: None,
        })
        .map_err(|_| bench_error("benchmark control queue full or closed"))
}

#[derive(Debug)]
/// Owned OMQ control pump with bounded outgoing messages and incoming reports.
pub struct Connection {
    _context: Context,
    output: mpsc::Sender<Outgoing>,
    input: Option<mpsc::Receiver<BenchResult<Value>>>,
    task: tokio::task::JoinHandle<BenchResult<()>>,
    live: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
}

/// Read-only supervision shared with process-liveness checks.
#[derive(Clone, Debug)]
pub struct Monitor {
    live: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
}

impl Monitor {
    /// Whether the control pump still runs.
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    /// First recorded control-pump failure, if any.
    pub fn failure(&self) -> Option<String> {
        self.failure.lock().expect("control failure").clone()
    }
}

impl Connection {
    /// Bind before launching the worker. Abstract IPC owns no filesystem path.
    pub async fn listen(bind: Option<&str>) -> BenchResult<(Self, Args)> {
        let run = Uuid::now_v7();
        let worker = Uuid::now_v7();
        let endpoint = bind.map_or_else(|| format!("ipc://@ozzy-bench-{run}"), str::to_owned);
        if bind.is_some() && !endpoint.starts_with("tcp://") {
            return Err(bench_error(
                "remote control bind must be an explicit TCP endpoint",
            ));
        }
        let context = context();
        let socket = socket(&context, &CONTROLLER);
        let bound = socket.bind(endpoint.parse()?).await?;
        let args = Args {
            control_endpoint: Some(bound.to_string()),
            control_run: Some(run),
            control_worker: Some(worker),
            control_bind: None,
        };
        Ok((
            Self::start(context, socket, run, worker, *worker.as_bytes()),
            args,
        ))
    }

    async fn connect(context: &Context, args: &Args) -> BenchResult<Self> {
        let run = args
            .control_run
            .ok_or_else(|| bench_error("missing control run"))?;
        let worker = args
            .control_worker
            .ok_or_else(|| bench_error("missing control worker"))?;
        let endpoint = args
            .control_endpoint
            .as_ref()
            .ok_or_else(|| bench_error("missing control endpoint"))?;
        let socket = socket(context, worker.as_bytes());
        socket.connect(endpoint.parse()?).await?;
        Ok(Self::start(
            context.clone(),
            socket,
            run,
            worker,
            CONTROLLER,
        ))
    }

    fn start(context: Context, socket: Socket, run: Uuid, worker: Uuid, peer: [u8; 16]) -> Self {
        let (output, outgoing) = mpsc::channel(QUEUE);
        let (incoming, input) = mpsc::channel(QUEUE);
        let live = Arc::new(AtomicBool::new(true));
        let task_live = live.clone();
        let errors = incoming.downgrade();
        let failure = Arc::new(Mutex::new(None));
        let task_failure = failure.clone();
        let network_context = context.clone();
        let task = tokio::spawn(async move {
            let _context = network_context;
            let result = pump(
                &socket,
                Route { run, worker, peer },
                outgoing,
                incoming,
                &task_live,
                &task_failure,
            )
            .await;
            if let Err(error) = &result {
                task_failure
                    .lock()
                    .expect("control failure")
                    .get_or_insert_with(|| error.to_string());
            }
            if let Err(error) = &result
                && let Some(incoming) = errors.upgrade()
            {
                let _ = incoming.try_send(Err(bench_error(error.to_string())));
            }
            task_live.store(false, Ordering::Release);
            result
        });
        Self {
            _context: context,
            output,
            input: Some(input),
            task,
            live,
            failure,
        }
    }

    /// Clone read-only pump liveness and failure supervision.
    pub fn monitor(&self) -> Monitor {
        Monitor {
            live: self.live.clone(),
            failure: self.failure.clone(),
        }
    }
    /// Queue a control data frame without waiting for destination capacity.
    pub fn send(&self, value: &Value) -> BenchResult<()> {
        enqueue(&self.output, Kind::Data, value.clone())
    }
    /// Queue a shutdown frame, failing if the bounded control queue cannot accept it.
    pub fn shutdown(&self) -> BenchResult<()> {
        enqueue(&self.output, Kind::Shutdown, Value::Null)
    }
    /// Receive the next control data frame or terminal pump error.
    pub async fn receive(&mut self) -> BenchResult<Value> {
        self.input
            .as_mut()
            .ok_or_else(|| bench_error("control receiver already taken"))?
            .recv()
            .await
            .ok_or_else(|| {
                bench_error(
                    self.failure
                        .lock()
                        .expect("control failure")
                        .clone()
                        .unwrap_or_else(|| "worker control closed".into()),
                )
            })?
    }
    /// Poll one control data frame without waiting; propagate terminal pump errors.
    pub fn try_receive(&mut self) -> BenchResult<Option<Value>> {
        match self
            .input
            .as_mut()
            .ok_or_else(|| bench_error("control receiver already taken"))?
            .try_recv()
        {
            Ok(value) => value.map(Some),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Err(bench_error("benchmark control disconnected"))
            }
        }
    }
    fn take_input(&mut self) -> BenchResult<mpsc::Receiver<BenchResult<Value>>> {
        self.input
            .take()
            .ok_or_else(|| bench_error("control receiver already taken"))
    }
    async fn finish(&mut self) -> BenchResult<()> {
        self.complete(Kind::Finished, Value::Null).await
    }

    async fn complete(&mut self, kind: Kind, value: Value) -> BenchResult<()> {
        tokio::time::timeout(DEADLINE, self.complete_acknowledged(kind, value)).await?
    }

    async fn complete_acknowledged(&mut self, kind: Kind, value: Value) -> BenchResult<()> {
        let (completion, complete) = oneshot::channel();
        self.output
            .send(Outgoing {
                kind,
                value,
                completion: Some(completion),
            })
            .await
            .map_err(|_| bench_error("control closed before worker completion"))?;
        complete.await??;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
        self.live.store(false, Ordering::Release);
    }
}

fn socket(context: &Context, identity: &[u8; 16]) -> Socket {
    context.socket(
        SocketType::Peer,
        Options::default()
            .workload_profile(WorkloadProfile::Throughput)
            .identity(Bytes::copy_from_slice(identity))
            .router_mandatory(true)
            .send_hwm(32)
            .recv_hwm(32)
            // OMQ bounds account for multipart descriptors as well as wire bytes.
            .max_message_size(CHUNK + 512)
            .linger(Duration::ZERO),
    )
}
