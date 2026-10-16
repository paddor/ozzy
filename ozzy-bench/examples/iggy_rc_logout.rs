//! Bounded Iggy RC diagnostic, never a throughput measurement or result row.
//! Build with --features comparisons, then run the executable without Cargo.
#![forbid(unsafe_code)]

#[cfg(feature = "comparisons")]
mod diagnostic {
    use clap::Parser;
    use futures::future::try_join_all;
    use iggy::prelude::{
        Client, Consumer, Durability, Identifier, IggyByteSize, IggyMessage, MessageClient,
        Partitioning, PollingStrategy, StreamClient, TcpClient, TopicClient, TopicCreateOptions,
        UserClient,
    };
    use ozzy_bench::automation::{self, Result, isolation, server};
    use ozzy_bench::control::{self, Connection};
    use serde_json::json;
    use std::{
        process::{Child, Command, Stdio},
        time::Duration,
    };

    #[derive(Parser)]
    #[expect(
        clippy::struct_excessive_bools,
        reason = "Independent diagnostic CLI dimensions"
    )]
    struct Args {
        #[arg(long)]
        singleton: bool,
        #[arg(long)]
        persisted: bool,
        /// Zero isolates connect/logout without creating a topic.
        #[arg(long, default_value_t = 1)]
        writes: usize,
        #[arg(long, default_value_t = 1)]
        records: usize,
        #[arg(long, default_value_t = 128)]
        record_bytes: usize,
        /// Add a separate verified reader for each writer.
        #[arg(long)]
        poll: bool,
        #[arg(long, default_value_t = 1)]
        writers: usize,
        #[arg(long, default_value_t = 0)]
        pause_ms: u64,
        #[arg(long)]
        processes: bool,
        #[arg(long)]
        logout_on_eof: bool,
        #[arg(long)]
        debug: bool,
        #[arg(long)]
        sync_before_logout: bool,
        #[arg(long, hide = true)]
        worker: Option<usize>,
        #[arg(long, hide = true)]
        endpoint: Option<String>,
        #[command(flatten)]
        control: control::Args,
    }

    #[derive(Default)]
    struct Workers {
        children: Vec<Child>,
        connections: Vec<Connection>,
    }

    /// Clients and worker processes of one diagnostic. Declared before the
    /// brokers start, so the brokers stop first and the disconnects add no
    /// noise to the diagnostics.
    struct Session<'a> {
        root: &'a std::path::Path,
        clients: Vec<TcpClient>,
        workers: Workers,
    }

    impl Drop for Workers {
        fn drop(&mut self) {
            for child in &mut self.children {
                let _ = child.kill();
            }
            for child in &mut self.children {
                let _ = child.wait();
            }
        }
    }

    async fn worker(args: &Args, lane: usize) -> Result<()> {
        let endpoint = args
            .endpoint
            .as_deref()
            .ok_or("worker requires --endpoint")?;
        let client = connect(endpoint).await?;
        let mut input = control::input()?;
        control::reply(&json!({"event":"ready"}))?;
        control::command(&mut input, "start").await?;
        if lane < args.writers {
            write(&client, args, lane).await?;
        } else {
            read(&client, args, lane - args.writers).await?;
        }
        control::reply(&json!({"event":"done"}))?;
        if args.logout_on_eof {
            if input.recv().await.is_some() {
                return Err("expected controller EOF".into());
            }
        } else {
            control::command(&mut input, "logout").await?;
        }
        let started = std::time::Instant::now();
        client.logout_user().await?;
        println!(
            "logout_elapsed_ms={}",
            started.elapsed().as_secs_f64() * 1000.0
        );
        control::reply(&json!({"event":"logged_out"}))?;
        let _ = input.recv().await;
        Ok(())
    }

    async fn process_data(args: &Args, endpoint: &str, session: &mut Session<'_>) -> Result<()> {
        let workers = &mut session.workers;
        for index in 0..args.writers * if args.poll { 2 } else { 1 } {
            let (connection, child) = spawn_worker(args, endpoint, session.root, index).await?;
            workers.children.push(child);
            workers.connections.push(connection);
        }
        for connection in &workers.connections {
            connection.send(&json!({"command":"start"}))?;
        }
        for connection in &mut workers.connections {
            if connection.receive().await?["event"] != "done" {
                return Err("worker not done".into());
            }
        }
        println!("all writer and verified reader processes finished");
        settle_before_logout(args).await?;
        println!("first writer process logout requested");
        logout_first(args, workers).await
        // Keep control connections alive until the caller snapshots diagnostics.
    }

    /// Start worker process `index` and wait until it reports ready.
    async fn spawn_worker(
        args: &Args,
        endpoint: &str,
        root: &std::path::Path,
        index: usize,
    ) -> Result<(Connection, Child)> {
        let (mut connection, config) = Connection::listen(None).await?;
        let mut arguments = vec![
            "--worker".into(),
            index.to_string(),
            "--endpoint".into(),
            endpoint.into(),
            "--writers".into(),
            args.writers.to_string(),
            "--writes".into(),
            args.writes.to_string(),
            "--records".into(),
            args.records.to_string(),
            "--record-bytes".into(),
            args.record_bytes.to_string(),
        ];
        config.append(&mut arguments);
        if args.logout_on_eof {
            arguments.push("--logout-on-eof".into());
        }
        let log = std::fs::File::create(root.join(format!("worker-{index}.log")))?;
        let child = Command::new("taskset")
            .args(["-c", "4-11"])
            .arg(std::env::current_exe()?)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        if connection.receive().await?["event"] != "ready" {
            return Err("worker not ready".into());
        }
        Ok((connection, child))
    }

    /// Log out the first writer process, by controller EOF or by command.
    async fn logout_first(args: &Args, workers: &mut Workers) -> Result<()> {
        if args.logout_on_eof {
            workers.connections[0].shutdown()?;
            loop {
                if let Some(status) = workers.children[0].try_wait()? {
                    if !status.success() {
                        return Err(format!("logout worker exited {status}").into());
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            workers.connections[0].send(&json!({"command":"logout"}))?;
            if workers.connections[0].receive().await?["event"] != "logged_out" {
                return Err("worker did not logout".into());
            }
        }
        Ok(())
    }

    async fn connect(endpoint: &str) -> Result<TcpClient> {
        let client = TcpClient::from_connection_string(&format!("iggy://iggy:iggy@{endpoint}"))?;
        client.connect().await?;
        Ok(client)
    }

    async fn write(client: &TcpClient, args: &Args, lane: usize) -> Result<()> {
        let stream = Identifier::try_from("logout-repro")?;
        let topic = Identifier::try_from("records")?;
        for write in 0..args.writes {
            let mut messages = (0..args.records)
                .map(|record| {
                    IggyMessage::builder()
                        .id((write * args.records + record + 1) as u128)
                        .payload(vec![42; args.record_bytes].into())
                        .build()
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let reply = client
                .send_messages(
                    &stream,
                    &topic,
                    &Partitioning::partition_id(lane as u32),
                    &mut messages,
                )
                .await?;
            if reply.confirmations.len() != 1
                || reply.confirmations[0].base_offset != (write * args.records) as u64
            {
                return Err("unexpected confirmation".into());
            }
        }
        println!(
            "writer {lane} confirmed {} records",
            args.writes * args.records
        );
        Ok(())
    }

    async fn read(client: &TcpClient, args: &Args, lane: usize) -> Result<()> {
        let stream = Identifier::try_from("logout-repro")?;
        let topic = Identifier::try_from("records")?;
        let mut offset = 0;
        while offset < args.writes * args.records {
            let batch = client
                .poll_messages(
                    &stream,
                    &topic,
                    Some(lane as u32),
                    &Consumer::default(),
                    &PollingStrategy::offset(offset as u64),
                    1024,
                    false,
                )
                .await?;
            for message in batch.messages {
                if message.header.offset != offset as u64
                    || message.header.id != offset as u128 + 1
                    || message.payload.len() != args.record_bytes
                    || message.payload.iter().any(|&b| b != 42)
                {
                    return Err("record verification failed".into());
                }
                offset += 1;
            }
        }
        println!("reader {lane} verified {offset} records");
        Ok(())
    }

    async fn operation(
        control: &TcpClient,
        args: &Args,
        endpoint: &str,
        session: &mut Session<'_>,
    ) -> Result<()> {
        if args.writes == 0 {
            println!("control logout requested");
            control.logout_user().await?;
        } else {
            control.create_stream("logout-repro").await?;
            control
                .create_topic(
                    &Identifier::try_from("logout-repro")?,
                    "records",
                    &TopicCreateOptions {
                        partitions_count: Some(args.writers as u32),
                        durability: if args.persisted {
                            Durability::Persisted
                        } else {
                            Durability::Replicated
                        },
                        preallocate_segments: Some(false),
                        segment_size: Some(IggyByteSize::from(64_u64 * 1024 * 1024)),
                        ..TopicCreateOptions::default()
                    },
                )
                .await?;
            control.logout_user().await?;
            control.shutdown().await?;
            if args.processes {
                process_data(args, endpoint, session).await?;
            } else {
                client_data(args, endpoint, session).await?;
            }
        }
        println!("logout completed");
        Ok(())
    }

    /// Write and verify through in-process clients, then log out the first
    /// writer.
    async fn client_data(args: &Args, endpoint: &str, session: &mut Session<'_>) -> Result<()> {
        for _ in 0..args.writers * if args.poll { 2 } else { 1 } {
            session.clients.push(connect(endpoint).await?);
        }
        let clients = &session.clients;
        let writes = try_join_all(
            clients[..args.writers]
                .iter()
                .enumerate()
                .map(|(lane, client)| write(client, args, lane)),
        );
        let reads = try_join_all(
            clients[args.writers..]
                .iter()
                .enumerate()
                .map(|(lane, client)| read(client, args, lane)),
        );
        tokio::try_join!(writes, reads)?;
        settle_before_logout(args).await?;
        println!("first writer logout requested; all readers finished");
        clients[0].logout_user().await?;
        Ok(())
    }

    /// Sync the artifact root when asked, then pause before the logout.
    async fn settle_before_logout(args: &Args) -> Result<()> {
        if args.sync_before_logout {
            automation::capture(
                Command::new("sync")
                    .arg("-f")
                    .arg(automation::artifact_root()),
            )?;
        }
        tokio::time::sleep(Duration::from_millis(args.pause_ms)).await;
        Ok(())
    }

    pub(super) async fn run() -> Result<()> {
        let args = Args::parse();
        let control = args.control.clone();
        control::run(&control, 1, execute(args)).await
    }

    /// Refuse workloads outside the bounded diagnostic shape.
    fn check_bounds(args: &Args) -> Result<()> {
        let within = args.writes <= 128
            && (1..=1024).contains(&args.records)
            && (1..=8192).contains(&args.record_bytes)
            && (1..=4).contains(&args.writers)
            && args.pause_ms <= 8000;
        if within {
            Ok(())
        } else {
            Err("diagnostic workload exceeds its bounds".into())
        }
    }

    /// The saved outcome row; never a performance ledger row.
    fn outcome_row(
        args: &Args,
        result: &str,
        diagnostics: Option<&str>,
        server: &serde_json::Value,
    ) -> serde_json::Value {
        json!({"kind":"diagnostic-not-benchmark", "result":result,
            "singleton":args.singleton,"writes":args.writes,"records":args.records,"writers":args.writers,
            "record_bytes":args.record_bytes,"poll":args.poll,"persisted":args.persisted,"pause_ms":args.pause_ms,"processes":args.processes,"logout_on_eof":args.logout_on_eof,"sync_before_logout":args.sync_before_logout,
            "server_diagnostics":diagnostics,"server":server})
    }

    /// Start the singleton or three-broker Iggy fixture under `root`.
    fn start_brokers(args: &Args, root: &std::path::Path) -> Result<server::Iggy> {
        let brokers = if args.singleton { 1 } else { 3 };
        server::Iggy::start(
            root,
            if args.singleton {
                "buffered"
            } else {
                "disk-quorum"
            },
            &vec![vec![0, 1, 2, 3]; brokers],
            if args.debug {
                "info,iggy.metadata.diag=debug,consensus::impls=debug"
            } else {
                "info"
            },
        )
    }

    async fn execute(args: Args) -> Result<()> {
        if let Some(lane) = args.worker {
            return worker(&args, lane).await;
        }
        check_bounds(&args)?;
        isolation::require_idle()?;
        isolation::pin(None, &[4, 5, 6, 7, 8, 9, 10, 11])?;
        let _lock = automation::lock_runner(&automation::cache()?)?;
        let root = automation::cache()?
            .join("diagnostics")
            .join(format!("iggy-logout-{}", automation::run_id()?));
        std::fs::create_dir_all(&root)?;
        server::prepare(true, &root)?;
        let mut session = Session {
            root: &root,
            clients: Vec::new(),
            workers: Workers::default(),
        };
        let broker = start_brokers(&args, &root)?;
        println!("DIAGNOSTIC ONLY: {}", root.display());
        let control =
            tokio::time::timeout(Duration::from_secs(5), connect(&broker.endpoint)).await??;
        println!("connected");
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            operation(&control, &args, &broker.endpoint, &mut session),
        )
        .await;
        let result = match outcome {
            Ok(Ok(())) => "completed".to_owned(),
            Ok(Err(error)) => format!("failed: {error}"),
            Err(_) => "deadline expired".to_owned(),
        };
        let diagnostics = broker.check().err().map(|error| error.to_string());
        let row = outcome_row(&args, &result, diagnostics.as_deref(), &broker.identity()?);
        automation::json_file(&root.join("outcome.json"), &row)?;
        println!("{result}; diagnostics: {diagnostics:?}");
        // End the disposable fixture after the bounded observation. Connections
        // stay alive until brokers stop, avoiding unrelated disconnect noise.
        // Preserve logs; never append this to either performance ledger.
        drop(broker);
        Ok(())
    }
}

#[cfg(feature = "comparisons")]
fn main() -> ozzy_bench::automation::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(diagnostic::run())
}

#[cfg(not(feature = "comparisons"))]
fn main() {
    eprintln!("build this diagnostic with --features comparisons");
    std::process::exit(2);
}
