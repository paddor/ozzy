use super::*;

#[tokio::test]
#[cfg(target_os = "linux")]
#[expect(
    clippy::too_many_lines,
    reason = "linear real-backend repair and restart fixture"
)]
async fn pool_and_aio_repair_are_readable_by_strict_legacy_recovery() {
    for aio in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("group");
        let config = ozzy_io_pool::Config {
            threads: 1,
            max_inflight: 1,
            handles: 32,
            limits: io_limits(),
        };
        let (pool, aio_backend, mut clients) = if aio {
            let (backend, clients) = ozzy_io_aio::Aio::new(ozzy_io_aio::Config {
                pool: config,
                depth: 1,
            })
            .unwrap();
            (None, Some(backend), clients)
        } else {
            let (backend, clients) = ozzy_io_pool::Pool::new(config).unwrap();
            (Some(backend), None, clients)
        };
        let io = Local::new(clients.remove(0));
        let mut limits = limits();
        limits.io.direct = aio;
        let mut journal = Journal::format(
            root.clone(),
            io.clone(),
            spec(CommitMode::External),
            JournalGeneration(1),
            limits,
        )
        .await
        .unwrap();
        let first = operation(&journal);
        let position = journal.append(&[first], BodyEncoding::Raw).await.unwrap();
        journal.sync_through(position).await.unwrap();
        journal.roll_active(32768, 4).await.unwrap();
        let second = CanonicalOperation {
            body: &[2; 16],
            ..operation(&journal)
        };
        let position = journal.append(&[second], BodyEncoding::Raw).await.unwrap();
        journal.sync_through(position).await.unwrap();
        journal.publish_durable_progress().await.unwrap();
        let accepted = journal.accepted_position().unwrap();
        let file = journal
            .access
            .open(
                root.join("segments/1.log"),
                ozzy_io::OpenMode::ReadWrite,
                false,
                false,
            )
            .await
            .unwrap();
        journal.access.write_all(&file, 0, &[99]).await.unwrap();
        journal.access.sync(&file).await.unwrap();
        journal
            .access
            .done(Operation::Close { handle: file })
            .await
            .unwrap();
        drop(journal);
        let directory =
            RecoveryDirectory::open_for_repair(root.clone(), io, identity(), CONFIG, limits)
                .await
                .unwrap();
        let directory = directory.quarantine_for_recovery(CONFIG).await.unwrap();
        let repair = directory
            .begin_sealed_repair(
                CONFIG,
                JournalGeneration(9),
                7,
                accepted,
                repair_limits(BodyEncoding::Raw),
            )
            .await
            .unwrap();
        assert!(
            repair.pending().unwrap().is_none(),
            "header-only damage needs no donor"
        );
        let journal = Box::pin(repair.finish(CONFIG, super::super::recovery::recovery_limits()))
            .await
            .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), accepted);
        assert_eq!(journal.manifest.segments[0].file_generation, 1);
        drop(journal);
        if let Some(pool) = pool {
            pool.shutdown().await;
        }
        if let Some(aio) = aio_backend {
            aio.shutdown().await;
        }
        let directory = GroupDirectory::open_with_configuration(
            root,
            identity(),
            MetadataLimits::default(),
            CONFIG,
        )
        .unwrap();
        let journal = directory
            .recover(JournalGeneration(10), limits.decode, limits.operations)
            .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), accepted);
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        assert_eq!(journal.directory().manifest().last_normal_view, 0);
        assert_eq!(journal.directory().manifest().promised_view, 7);
    }
}
