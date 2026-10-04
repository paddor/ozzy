use super::*;

pub(super) struct Fleet {
    pub(super) checked: CheckedConfig,
    pub(super) brokers: Vec<Option<Broker>>,
    pub(super) kills: u64,
    hosts: Vec<String>,
    directories: Vec<PathBuf>,
    cpus: Vec<usize>,
    logs: PathBuf,
    next_kill: Duration,
    stopped: Option<(usize, Duration, bool)>,
    memory: bool,
}

const NAMES: [&str; 3] = ["si-dev", "wu-dev", "er-dev"];

impl Fleet {
    pub(super) async fn verify_wave(
        &mut self,
        client: &mut Client,
        mut reader: ozzy_runtime::replicated::TopicReader,
        wave: usize,
    ) -> (ozzy_runtime::replicated::TopicReader, usize) {
        let positions = client.positions();
        let pending = observe(
            &mut self.brokers,
            "admit soak records",
            client.queue_varied(wave),
        )
        .await;
        let records = pending.len();
        observe(
            &mut self.brokers,
            "confirm soak records",
            client.confirm(pending),
        )
        .await;
        if wave % 6 == 2 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        observe(
            &mut self.brokers,
            "verify live soak reader",
            client.read(&mut reader, positions),
        )
        .await;
        if wave.is_multiple_of(17) {
            observe(
                &mut self.brokers,
                "verify checkpoint replay",
                client.replay(),
            )
            .await;
            observe(&mut self.brokers, "close live soak reader", reader.close())
                .await
                .unwrap();
            reader = observe(
                &mut self.brokers,
                "resume live soak reader",
                client.reader(true),
            )
            .await;
        }
        (reader, records)
    }
    pub(super) async fn start() -> Self {
        let directories: Vec<_> = std::env::var("OZZY_SOAK_DIRS")
            .unwrap()
            .split(',')
            .map(PathBuf::from)
            .collect();
        let config = PathBuf::from(std::env::var("OZZY_SOAK_CONFIG").unwrap());
        let identity = std::env::var_os("OZZY_SOAK_IDENTITY")
            .map_or_else(|| directories[0].join("shared.identity"), PathBuf::from);
        let checked = checked(&config, &identity, NAMES[0]);
        let count = if checked.deployment.deployment().topics["orders"].confirmation
            == ozzy_config::Confirmation::LocalDurable
        {
            1
        } else {
            3
        };
        assert_eq!(directories.len(), count);
        let logs = PathBuf::from(std::env::var("OZZY_SOAK_LOGS").unwrap());
        fs::create_dir_all(&logs).unwrap();
        let hosts: Vec<_> = std::env::var("OZZY_SOAK_HOSTS")
            .unwrap_or_else(|_| NAMES[..count].join(","))
            .split(',')
            .map(str::to_owned)
            .collect();
        assert_eq!(hosts.len(), count);
        let cpus: Vec<_> = (0..count)
            .map(|index| {
                hosts[..index]
                    .iter()
                    .filter(|host| *host == &hosts[index])
                    .count()
            })
            .collect();
        let memory = std::env::var("OZZY_SOAK_MEMORY").is_ok_and(|value| value == "1");
        let mut fleet = Self {
            checked,
            brokers: vec![],
            kills: 0,
            hosts,
            directories,
            cpus,
            logs,
            next_kill: Duration::from_secs(60),
            stopped: None,
            memory,
        };
        for index in 0..count {
            fleet
                .brokers
                .push(Some(fleet.start_one(index, false).await));
        }
        fleet
    }

    async fn start_one(&self, index: usize, recovering: bool) -> Broker {
        Broker::start(
            &self.hosts[index],
            NAMES[index],
            &self.directories[index],
            &self.logs,
            recovering,
            self.kills,
            self.cpus[index],
        )
        .await
    }

    pub(super) async fn churn(&mut self, elapsed: Duration) {
        let policy = self.checked.deployment.deployment().topics["orders"].confirmation;
        if let Some((index, deadline, unclean)) = self.stopped {
            if elapsed >= deadline {
                let recovering = self.memory
                    || (unclean && policy == ozzy_config::Confirmation::ReplicatedPersisting);
                self.brokers[index] = Some(self.start_one(index, recovering).await);
                self.stopped = None;
            }
        } else if elapsed >= self.next_kill {
            let index = self.kills as usize % self.hosts.len();
            let unclean = self.kills % 4 != 3;
            let signal = if unclean { "KILL" } else { "TERM" };
            self.brokers[index].take().unwrap().stop(signal).await;
            self.kills += 1;
            println!(
                "stopped {} on {} with {signal} at {} seconds",
                NAMES[index],
                self.hosts[index],
                elapsed.as_secs()
            );
            if policy == ozzy_config::Confirmation::LocalDurable {
                // No alternate copy exists. Resume the one broker before
                // awaiting SDK progress; its durable store is authoritative.
                self.brokers[index] = Some(self.start_one(index, false).await);
            } else {
                self.stopped = Some((index, elapsed + Duration::from_secs(10), unclean));
            }
            self.next_kill = elapsed + Duration::from_secs(90);
        }
    }
}
