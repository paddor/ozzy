//! Post-installation application validation, write, sync, and apply on Disk's thread.
//! Validation returns an owned plan without journal effects. The protocol actor
//! rechecks its live role before admission and only then submits the exact write.

use super::*;
use ozzy_core::state::PreparedCanonicalGroup;
use ozzy_replication::Scope;
use ozzy_replication::driver::ValidationTicket;

#[derive(Debug)]
pub(super) struct Validated {
    pub validation: ValidationTicket,
    pub operation: OwnedOperation,
    pub prepared: PreparedOperation,
    plan: PreparedCanonicalGroup,
}

#[derive(Debug)]
pub(super) enum Command {
    Validate {
        validation: ValidationTicket,
        operation: OwnedOperation,
        done: oneshot::Sender<Completion>,
    },
    Write {
        validated: Validated,
        ticket: WriteTicket,
        release: Option<oneshot::Receiver<()>>,
        done: oneshot::Sender<Completion>,
    },
    Apply {
        scope: Scope,
        generation: JournalGeneration,
        through: Prefix,
        done: oneshot::Sender<Completion>,
    },
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded worker completion moves an owned plan without another allocation"
)]
pub(super) enum Completion {
    Validated(Validated),
    Written {
        ticket: WriteTicket,
        durable: oneshot::Receiver<Completion>,
    },
    Durable(LogPosition),
    Applied {
        scope: Scope,
        generation: JournalGeneration,
        through: Prefix,
    },
}

impl Disk {
    pub(super) fn append_work(
        &mut self,
        command: impl FnOnce(oneshot::Sender<Completion>) -> Command,
    ) -> oneshot::Receiver<Completion> {
        let (done, completed) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Append(command(done)))
            .unwrap();
        completed
    }
}

fn validate_scope(journal: &OpenGroupJournal, scope: Scope, generation: JournalGeneration) {
    let manifest = journal.directory().manifest();
    assert_eq!(scope.group_id, manifest.identity.group_id);
    assert_eq!(scope.configuration_epoch, manifest.configuration_epoch);
    assert_eq!(
        scope.configuration_digest,
        configuration().scope().configuration_digest
    );
    assert_eq!(scope.view, manifest.last_normal_view);
    assert_eq!(scope.view, manifest.promised_view);
    assert_eq!(generation, journal.writer().durable_position().generation());
}

pub(super) fn handle(
    journal: &mut OpenGroupJournal,
    state: &mut install_worker::State,
    command: Command,
) {
    let install_worker::State::Active(images) = state else {
        panic!("fresh operations require activated application state");
    };
    match command {
        Command::Validate {
            validation,
            operation,
            done,
        } => {
            let validated = validate_append(journal, images, validation, operation);
            done.send(Completion::Validated(validated)).unwrap();
        }
        Command::Write {
            validated,
            ticket,
            release,
            done,
        } => {
            let validation = validated.validation;
            validate_scope(journal, validation.scope(), validation.generation());
            assert_eq!(ticket.generation(), validation.generation());
            assert_eq!(ticket.first().0, validated.operation.op_number);
            assert_eq!(ticket.through(), validated.prepared.prefix().op);
            assert_eq!(
                journal.accepted_position().unwrap(),
                LogPosition {
                    op_number: validation.accepted().op.0,
                    digest: validation.accepted().digest,
                }
            );
            images.install_prepared_group(validated.plan).unwrap();
            let position = journal.append(&[validated.operation.canonical()]).unwrap();
            let (durable, completion) = oneshot::channel();
            done.send(Completion::Written {
                ticket,
                durable: completion,
            })
            .unwrap();
            if let Some(release) = release {
                release.blocking_recv().unwrap();
            }
            journal.sync_through(position).unwrap();
            durable
                .send(Completion::Durable(journal.accepted_position().unwrap()))
                .unwrap();
        }
        Command::Apply {
            scope,
            generation,
            through,
            done,
        } => {
            validate_scope(journal, scope, generation);
            assert_eq!(
                journal.accepted_position().unwrap(),
                LogPosition {
                    op_number: through.op.0,
                    digest: through.digest,
                }
            );
            images.commit_through(through.op.0).unwrap();
            assert_eq!(images.pending_len(), 0);
            assert_eq!(
                images
                    .committed()
                    .partition(partition())
                    .unwrap()
                    .next_offset,
                Offset::new(through.op.0 - 2)
            );
            // No per-response metadata barrier: quorum-prepared history protects
            // the newly applied records while the activation commit marker lags.
            done.send(Completion::Applied {
                scope,
                generation,
                through,
            })
            .unwrap();
        }
    }
}

fn validate_append(
    journal: &OpenGroupJournal,
    images: &CanonicalImages<ozzy_journal_segment::JournalIdentityIndex>,
    validation: ValidationTicket,
    operation: OwnedOperation,
) -> Validated {
    validate_scope(journal, validation.scope(), validation.generation());
    let predecessor = validation.accepted();
    assert_eq!(images.speculative().revision(), predecessor.op.0);
    assert_eq!(images.committed().revision(), validation.applied().op.0);
    let accepted = journal.accepted_position().unwrap();
    assert_eq!(accepted.op_number, predecessor.op.0);
    assert_eq!(accepted.digest, predecessor.digest);
    assert_eq!(operation.op_number, predecessor.op.0 + 1);
    assert_eq!(operation.previous, predecessor.digest);
    assert_eq!(operation.original_view, validation.scope().view);
    assert!(operation.body.len() <= 8192);
    let body =
        decode_operation_body(operation.kind, &operation.body, OperationLimits::default()).unwrap();
    let plan = images
        .prepare_group(&[(operation.op_number, body)])
        .unwrap();
    let prepared = PreparedOperation::from_verified(
        &operation.canonical(),
        canonical_body_digest(&operation.body),
    );
    Validated {
        validation,
        operation,
        prepared,
        plan,
    }
}
