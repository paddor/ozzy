//! Placement and lifecycle shared by topic adapters, without socket ownership.

use super::{
    Partition, PendingRecord, RecordInput, TopicSelection, TopicWriterError, WriterError,
    keyed_index,
};

#[derive(Clone, Copy)]
struct Sticky {
    index: usize,
    groups_formed: u64,
}

impl Sticky {
    fn select(&mut self, partitions: usize, formed: impl Fn(usize) -> u64) -> usize {
        if formed(self.index) != self.groups_formed {
            self.index = (self.index + 1) % partitions;
            self.groups_formed = formed(self.index);
        }
        self.index
    }
}

pub(super) struct Partitions {
    pub(super) entries: Vec<Partition>,
    seed: u64,
    sticky: Sticky,
}

impl Partitions {
    pub(super) fn new(entries: Vec<Partition>, seed: u64) -> Self {
        let groups_formed = entries[0].writer.groups_formed();
        Self {
            entries,
            seed,
            sticky: Sticky {
                index: 0,
                groups_formed,
            },
        }
    }

    pub(super) fn try_clone(&self) -> Result<Self, TopicWriterError> {
        let entries = self
            .entries
            .iter()
            .map(|partition| {
                Ok(Partition {
                    target: partition.target.clone(),
                    writer: partition.writer.try_clone()?,
                })
            })
            .collect::<Result<Vec<_>, TopicWriterError>>()?;
        Ok(Self {
            entries,
            seed: self.seed,
            sticky: self.sticky,
        })
    }

    fn select(&mut self, selection: TopicSelection<'_>) -> usize {
        match selection {
            TopicSelection::Key(key) => keyed_index(key, self.seed, self.entries.len()),
            TopicSelection::Keyless => self.sticky.select(self.entries.len(), |index| {
                self.entries[index].writer.groups_formed()
            }),
        }
    }

    pub(super) async fn send(
        &mut self,
        record: RecordInput,
        selection: TopicSelection<'_>,
    ) -> Result<(usize, PendingRecord), TopicWriterError> {
        let index = self.select(selection);
        let partition = &mut self.entries[index];
        let pending =
            partition
                .writer
                .send(record)
                .await
                .map_err(|source| TopicWriterError::Send {
                    partition: partition.target.clone(),
                    source,
                })?;
        Ok((index, pending))
    }

    pub(super) fn flush(
        &self,
    ) -> impl std::future::Future<Output = Result<(), WriterError>> + Send + 'static + use<> {
        let waits = self
            .entries
            .iter()
            .map(|partition| partition.writer.flush())
            .collect::<Vec<_>>();
        async move {
            for result in futures::future::join_all(waits).await {
                result?;
            }
            Ok(())
        }
    }

    pub(super) async fn close(self) -> Result<(), WriterError> {
        futures::future::join_all(
            self.entries
                .into_iter()
                .map(|partition| partition.writer.close()),
        )
        .await
        .into_iter()
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Sticky;

    #[test]
    fn cloned_handles_rotate_keyless_records_after_their_own_batch() {
        let mut first = Sticky {
            index: 0,
            groups_formed: 0,
        };
        let mut second = first;
        let mut formed = [0, 0, 0];
        assert_eq!(first.select(formed.len(), |index| formed[index]), 0);
        formed[0] += 1;
        assert_eq!(first.select(formed.len(), |index| formed[index]), 1);
        assert_eq!(second.select(formed.len(), |index| formed[index]), 1);
        formed[1] += 1;
        assert_eq!(first.select(formed.len(), |index| formed[index]), 2);
        assert_eq!(second.select(formed.len(), |index| formed[index]), 2);
    }
}
