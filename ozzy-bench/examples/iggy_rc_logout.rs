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
        fs::OpenOptions,
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

    async fn worker(args: &Args) -> Result<()> {
        let lane = args.worker.unwrap();
        let client = connect(args.endpoint.as_deref().unwrap()).await?;
        let mut input = control::input();
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

    async fn process_data(
        args: &Args,
        endpoint: &str,
        workers: &mut Workers,
        root: &std::path::Path,
    ) -> Result<()> {
        for index in 0..args.writers * if args.poll { 2 } else { 1 } {
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
            workers.children.push(
                Command::new("taskset")
                    .args(["-c", "4-11"])
                    .arg(std::env::current_exe()?)
                    .args(arguments)
                    .stdin(Stdio::null())
                    .stdout(log.try_clone()?)
                    .stderr(log)
                    .spawn()?,
            );
            if connection.receive().await?["event"] != "ready" {
                return Err("worker not ready".into());
            }
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
        if args.sync_before_logout {
            automation::capture(Command::new("sync").args(["-f", automation::SSD]))?;
        }
        tokio::time::sleep(Duration::from_millis(args.pause_ms)).await;
        println!("first writer process logout requested");
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
        // Keep control connections alive until the caller snapshots diagnostics.
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
        clients: &mut Vec<TcpClient>,
        endpoint: &str,
        args: &Args,
        workers: &mut Workers,
        root: &std::path::Path,
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
                process_data(args, endpoint, workers, root).await?;
                println!("logout completed");
                return Ok(());
            }
            for _ in 0..args.writers * if args.poll { 2 } else { 1 } {
                clients.push(connect(endpoint).await?);
            }
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
            if args.sync_before_logout {
                automation::capture(Command::new("sync").args(["-f", automation::SSD]))?;
            }
            tokio::time::sleep(Duration::from_millis(args.pause_ms)).await;
            println!("first writer logout requested; all readers finished");
            clients[0].logout_user().await?;
        }
        println!("logout completed");
        Ok(())
    }

    pub(super) async fn run() -> Result<()> {
        let args = Args::parse();
        let control = args.control.clone();
        control::run(&control, 1, execute(args)).await
    }

    async fn execute(args: Args) -> Result<()> {
        if args.worker.is_some() {
            return worker(&args).await;
        }
        if args.writes > 128
            || !(1..=1024).contains(&args.records)
            || !(1..=8192).contains(&args.record_bytes)
            || !(1..=4).contains(&args.writers)
            || args.pause_ms > 8000
        {
            return Err("diagnostic workload exceeds its bounds".into());
        }
        isolation::require_idle()?;
        isolation::pin(None, &[4, 5, 6, 7, 8, 9, 10, 11])?;
        let lock = OpenOptions::new()
            .create(true)
            .append(true)
            .open(automation::cache().join("runner.lock"))?;
        lock.try_lock()?;
        let root = automation::cache()
            .join("diagnostics")
            .join(format!("iggy-logout-{}", automation::run_id()?));
        std::fs::create_dir_all(&root)?;
        server::prepare(true, &root)?;
        let mut workers = Workers::default();
        let brokers = if args.singleton { 1 } else { 3 };
        let broker = server::Iggy::start(
            &root,
            if args.singleton {
                "buffered"
            } else {
                "replicated-persisting"
            },
            &vec![vec![0, 1, 2, 3]; brokers],
            if args.debug {
                "info,iggy.metadata.diag=debug,consensus::impls=debug"
            } else {
                "info"
            },
        )?;
        println!("DIAGNOSTIC ONLY: {}", root.display());
        let control =
            tokio::time::timeout(Duration::from_secs(5), connect(&broker.endpoint)).await??;
        let mut clients = vec![];
        println!("connected");
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            operation(
                &control,
                &mut clients,
                &broker.endpoint,
                &args,
                &mut workers,
                &root,
            ),
        )
        .await;
        let result = match outcome {
            Ok(Ok(())) => "completed".to_owned(),
            Ok(Err(error)) => format!("failed: {error}"),
            Err(_) => "deadline expired".to_owned(),
        };
        let diagnostics = broker.check().err().map(|error| error.to_string());
        let row = json!({"kind":"diagnostic-not-benchmark", "result":result,
            "singleton":args.singleton,"writes":args.writes,"records":args.records,"writers":args.writers,
            "record_bytes":args.record_bytes,"poll":args.poll,"persisted":args.persisted,"pause_ms":args.pause_ms,"processes":args.processes,"logout_on_eof":args.logout_on_eof,"sync_before_logout":args.sync_before_logout,
            "server_diagnostics":diagnostics,"server":broker.identity()?});
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
#[tokio::main(flavor = "current_thread")]
async fn main() -> ozzy_bench::automation::Result<()> {
    diagnostic::run().await
}

#[cfg(not(feature = "comparisons"))]
fn main() {
    panic!("build this diagnostic with --features comparisons");
}
