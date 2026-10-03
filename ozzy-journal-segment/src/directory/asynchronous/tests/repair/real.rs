#![cfg(target_os = "linux")]

use super::*;

#[tokio::test]
async fn pool_and_aio_repair_reopen_exact_history_and_preserve_original_files() {
    for aio in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("group");
        let config = ozzy_io_pool::Config {
            threads: 1,
            max_inflight: 1,
            handles: 32,
            limits: io_limits(),
        };
        let (backend, io) = device(aio, config);
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
        let mut damaged = std::fs::read(root.join("segments/1.log")).unwrap();
        damaged[0] = 99;
        let healthy = std::fs::read(root.join("segments/2.log")).unwrap();
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
        backend.shutdown().await;
        assert_eq!(std::fs::read(root.join("segments/1.log")).unwrap(), damaged);
        assert_eq!(std::fs::read(root.join("segments/2.log")).unwrap(), healthy);
        let (backend, io) = device(aio, config);
        let journal = Journal::open(
            root,
            io,
            identity(),
            Some(CONFIG),
            JournalGeneration(10),
            limits,
        )
        .await
        .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), accepted);
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        assert_eq!(journal.manifest.last_normal_view, 0);
        assert_eq!(journal.manifest.promised_view, 7);
        journal.close().await.unwrap();
        backend.shutdown().await;
    }
}

#[derive(Debug)]
enum Device {
    Pool(ozzy_io_pool::Pool),
    Aio(ozzy_io_aio::Aio),
}

fn device(aio: bool, config: ozzy_io_pool::Config) -> (Device, Local) {
    if aio {
        let (backend, mut clients) = ozzy_io_aio::Aio::new(ozzy_io_aio::Config {
            pool: config,
            depth: 1,
        })
        .unwrap();
        (Device::Aio(backend), Local::new(clients.remove(0)))
    } else {
        let (backend, mut clients) = ozzy_io_pool::Pool::new(config).unwrap();
        (Device::Pool(backend), Local::new(clients.remove(0)))
    }
}

impl Device {
    async fn shutdown(self) {
        match self {
            Self::Pool(pool) => pool.shutdown().await,
            Self::Aio(aio) => aio.shutdown().await,
        }
    }
}
