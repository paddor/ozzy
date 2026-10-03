//! Bounded real-journal installation fixture. Every function runs on Disk's OS thread.

use super::*;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryCandidate, CanonicalRecoveryLimits, JournalIdentityIndex,
    SuffixReplacement, SuffixStreamLimits,
};
use ozzy_replication::{InstallTicket, Scope};

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "one worker moves owned recovery state without extra boxing"
)]
pub(super) enum State {
    Empty,
    Selected(CanonicalRecoveryCandidate),
    Active(CanonicalImages<JournalIdentityIndex>),
}

#[derive(Debug)]
pub(super) enum Command {
    Install {
        ticket: InstallTicket,
        operations: Option<Vec<OwnedOperation>>,
        done: oneshot::Sender<Completion>,
    },
    Fetch {
        request: FetchOps,
        done: oneshot::Sender<Completion>,
    },
    Activate {
        scope: Scope,
        generation: JournalGeneration,
        through: Prefix,
        done: oneshot::Sender<Completion>,
    },
}

#[derive(Debug)]
pub(super) enum Completion {
    Installed {
        ticket: InstallTicket,
        prepared: Vec<PreparedOperation>,
        applied: Prefix,
    },
    History {
        request: FetchOps,
        operations: Vec<OwnedOperation>,
    },
    Activated {
        scope: Scope,
        generation: JournalGeneration,
        through: Prefix,
    },
}

impl Disk {
    pub(super) fn installation(
        &mut self,
        command: impl FnOnce(oneshot::Sender<Completion>) -> Command,
    ) -> oneshot::Receiver<Completion> {
        let (done, completed) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Installation(command(done)))
            .unwrap();
        completed
    }

    pub(super) async fn verify_installed(
        &mut self,
        prefix: Prefix,
        commit_floor: Prefix,
        expected_records: usize,
    ) {
        let (done, completed) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Reopen(done))
            .unwrap();
        let (accepted, committed, records) = completed.await.unwrap();
        assert_eq!(accepted, position(prefix));
        assert_eq!(committed, position(commit_floor));
        assert_eq!(records, expected_records);
    }
}

fn position(prefix: Prefix) -> LogPosition {
    LogPosition {
        op_number: prefix.op.0,
        digest: prefix.digest,
    }
}

fn read_history(journal: &OpenGroupJournal, source: LogSource) -> Vec<OwnedOperation> {
    assert_eq!(source.voter, journal.directory().identity().replica_node_id);
    assert_eq!(
        source.generation,
        journal.writer().durable_position().generation()
    );
    assert_eq!(
        position(source.accepted),
        journal.accepted_position().unwrap()
    );
    assert!(source.accepted.op.0 <= 8); // Whole-history retention is fixture-bounded.
    let mut operations = Vec::with_capacity(8);
    let mut bytes = 0;
    journal
        .replay_accepted(|item| {
            let operation = item.operation;
            bytes += operation.body.len();
            assert!(bytes <= 8192);
            operations.push(OwnedOperation {
                original_view: operation.original_view,
                op_number: operation.op_number,
                previous: operation.previous_digest,
                kind: operation.kind,
                body: operation.body.to_vec(),
            });
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    operations
}

fn install(
    mut journal: OpenGroupJournal,
    ticket: InstallTicket,
    operations: Option<Vec<OwnedOperation>>,
    done: oneshot::Sender<Completion>,
) -> (OpenGroupJournal, State) {
    let operations = operations.unwrap_or_else(|| read_history(&journal, ticket.source()));
    let canonical: Vec<_> = operations.iter().map(OwnedOperation::canonical).collect();
    assert!(canonical.len() <= 8);
    assert!(canonical.iter().map(|op| op.body.len()).sum::<usize>() <= 8192);
    assert_eq!(
        journal.writer().durable_position().generation(),
        ticket.previous_generation()
    );
    let current = journal.directory().current();
    let mut staging = journal
        .begin_suffix_replacement(
            SuffixReplacement {
                expected_current: current,
                protected_committed: position(ticket.protected_committed()),
                promised_view: ticket.scope().view,
                last_normal_view: ticket.scope().view,
                committed: position(ticket.committed()),
                writer_generation: ticket.generation(),
                segment_capacity: 32 * 1024,
                body_encoding: BodyEncoding::Raw,
            },
            position(ticket.accepted()),
            SuffixStreamLimits {
                max_group_operations: 2,
                max_group_body_bytes: 8192,
                max_segments: 4,
                max_staged_bytes: 128 * 1024,
                max_source_segment_bytes: 32 * 1024,
                max_orphan_probes: 8,
            },
        )
        .unwrap();
    for chunk in canonical.chunks(2) {
        staging.append_chunk(chunk).unwrap();
    }
    journal = staging.finish().unwrap();
    let candidate = journal
        .recover_canonical_candidate(CanonicalRecoveryLimits {
            accepted_transitions: 1,
            retained_identities: 1,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(
        candidate.committed_images().committed().revision(),
        ticket.committed().op.0
    );
    assert_eq!(candidate.accepted_position(), position(ticket.accepted()));
    let prepared = canonical
        .iter()
        .filter(|operation| operation.op_number > ticket.committed().op.0)
        .map(|operation| {
            PreparedOperation::from_verified(operation, canonical_body_digest(operation.body))
        })
        .collect();
    done.send(Completion::Installed {
        ticket,
        prepared,
        applied: ticket.committed(),
    })
    .unwrap();
    (journal, State::Selected(candidate))
}

pub(super) fn handle(
    mut journal: OpenGroupJournal,
    state: State,
    command: Command,
) -> (OpenGroupJournal, State) {
    match command {
        Command::Install {
            ticket,
            operations,
            done,
        } => {
            assert!(matches!(state, State::Empty));
            install(journal, ticket, operations, done)
        }
        Command::Fetch { request, done } => {
            let operations = read_history(&journal, request.source);
            if request.predecessor != Prefix::GENESIS {
                let previous = operations
                    .iter()
                    .find(|op| op.op_number == request.predecessor.op.0)
                    .unwrap();
                let prefix = PreparedOperation::from_verified(
                    &previous.canonical(),
                    canonical_body_digest(&previous.body),
                )
                .prefix();
                assert_eq!(prefix, request.predecessor);
            }
            let mut bytes = 0;
            let operations = operations
                .into_iter()
                .filter(|op| op.op_number > request.predecessor.op.0)
                .take(request.max_operations as usize)
                .take_while(|op| {
                    bytes += op.body.len();
                    bytes <= request.max_body_bytes as usize
                })
                .collect();
            done.send(Completion::History {
                request,
                operations,
            })
            .unwrap();
            (journal, state)
        }
        Command::Activate {
            scope,
            generation,
            through,
            done,
        } => {
            assert_eq!(
                scope.configuration_digest,
                configuration().scope().configuration_digest
            );
            assert_eq!(scope.group_id, journal.directory().identity().group_id);
            assert_eq!(
                scope.configuration_epoch,
                journal.directory().manifest().configuration_epoch
            );
            assert_eq!(scope.view, journal.directory().manifest().last_normal_view);
            assert_eq!(generation, journal.writer().durable_position().generation());
            let State::Selected(candidate) = state else {
                panic!("selected application image")
            };
            assert_eq!(candidate.accepted_position(), position(through));
            let mut next = journal.directory().manifest().clone();
            next.parent_generation = next.generation;
            next.generation += 1;
            next.accepted = position(through);
            next.committed = next.accepted;
            journal = journal.install_metadata(next).unwrap();
            let images = candidate.activate(&journal).unwrap();
            assert_eq!(
                images
                    .committed()
                    .partition(partition())
                    .unwrap()
                    .next_offset,
                Offset::new(1)
            );
            assert_eq!(images.pending_len(), 0);
            done.send(Completion::Activated {
                scope,
                generation,
                through,
            })
            .unwrap();
            (journal, State::Active(images))
        }
    }
}
