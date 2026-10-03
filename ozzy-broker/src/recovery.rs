//! Explicit CLI intent. Topic text identifies configuration, never a file path.

use ozzy_broker::{RecoveryIntent, RecoverySelection};

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = true)]
pub(super) struct RecoveryArgs {
    /// Create an absent nonvoting replacement with its existing identity.
    #[arg(long, value_name = "TOPIC/PARTITION")]
    replace: Vec<Partition>,
    /// Preserve an established store while durably removing voting eligibility.
    #[arg(long, value_name = "TOPIC/PARTITION")]
    quarantine: Vec<Partition>,
    /// Resume an exact unfinished recovery marker, allowing sealed-file repair.
    #[arg(long, value_name = "TOPIC/PARTITION")]
    resume: Vec<Partition>,
    /// Resume an exact marker using full history transfer instead of repair.
    #[arg(long, value_name = "TOPIC/PARTITION")]
    resume_full: Vec<Partition>,
}

#[derive(Debug, Clone)]
struct Partition {
    topic: String,
    number: u32,
}

impl std::str::FromStr for Partition {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (topic, number) = value.rsplit_once('/').ok_or("expected TOPIC/PARTITION")?;
        if topic.is_empty() {
            return Err("expected a topic name".into());
        }
        Ok(Self {
            topic: topic.into(),
            number: number
                .parse()
                .map_err(|_| "expected a partition number from 0 to 4294967295")?,
        })
    }
}

impl RecoveryArgs {
    pub(super) fn into_selections(self) -> Vec<RecoverySelection> {
        [
            (self.replace, RecoveryIntent::Replace),
            (self.quarantine, RecoveryIntent::Quarantine),
            (self.resume, RecoveryIntent::Resume),
            (self.resume_full, RecoveryIntent::ResumeFull),
        ]
        .into_iter()
        .flat_map(|(partitions, intent)| {
            partitions
                .into_iter()
                .map(move |partition| RecoverySelection {
                    topic: partition.topic,
                    partition: partition.number,
                    intent,
                })
        })
        .collect()
    }
}
