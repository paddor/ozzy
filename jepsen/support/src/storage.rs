use crate::Result;
use ozzy_io::{
    Backend, Class, Completed, Handle, Limits, OpenMode, Operation, Outcome, Quota, SyncMode,
    WriteBuffer,
};
use ozzy_io_pool::{Client, Config, Pool};
use serde_json::{Value, json};
use std::{fs, path::Path};

async fn execute(client: &mut Client, operation: Operation) -> Result<Completed> {
    Ok(client.submit(Class::Data, operation)?.await?)
}

async fn open(client: &mut Client, path: &Path, data_sync: bool) -> Result<Handle> {
    let result = execute(
        client,
        Operation::Open {
            path: path.into(),
            mode: OpenMode::CreateNew,
            direct: false,
            data_sync,
        },
    )
    .await?;
    match &*result {
        Outcome::Opened(handle) => Ok(handle.clone()),
        _ => Err("expected opened handle".into()),
    }
}

async fn write(client: &mut Client, handle: &Handle, byte: u8, offset: u64) -> Result<()> {
    let result = execute(
        client,
        Operation::Write {
            handle: handle.clone(),
            offset,
            data: WriteBuffer::from_vec(vec![byte; 4096]),
        },
    )
    .await?;
    if !matches!(*result, Outcome::Written(4096)) {
        return Err("short qualification write".into());
    }
    Ok(())
}

async fn sync(client: &mut Client, handle: &Handle, mode: SyncMode) -> Result<()> {
    execute(
        client,
        Operation::Sync {
            handle: handle.clone(),
            mode,
        },
    )
    .await?;
    Ok(())
}

pub async fn prepare(root: &Path) -> Result<Value> {
    // The caller gives a new disposable directory on the exact LazyFS mount.
    fs::create_dir(root)?;
    let quota = Quota {
        operations: 8,
        bytes: 1024 * 1024,
    };
    let (pool, mut clients) = Pool::new(Config {
        threads: 1,
        max_inflight: 8,
        handles: 16,
        limits: Limits {
            shards: 1,
            data: quota,
            progress: quota,
        },
    })?;
    let client = &mut clients[0];
    let directory = execute(client, Operation::OpenDirectory { path: root.into() }).await?;
    let Outcome::Opened(directory) = &*directory else {
        return Err("expected directory handle".into());
    };
    let directory = directory.clone();
    for (name, byte, mode) in [
        ("dsync", b'D', None),
        ("datasync", b'A', Some(SyncMode::Data)),
        ("fsync", b'F', Some(SyncMode::All)),
        ("dirty", b'X', None),
    ] {
        let path = root.join(name);
        let handle = open(client, &path, name == "dsync").await?;
        execute(
            client,
            Operation::Allocate {
                handle: handle.clone(),
                offset: 0,
                length: 8192,
            },
        )
        .await?;
        write(client, &handle, b'0', 0).await?;
        write(client, &handle, b'0', 4096).await?;
        sync(client, &handle, SyncMode::All).await?;
        write(client, &handle, byte, 0).await?;
        if let Some(mode) = mode {
            sync(client, &handle, mode).await?;
        }
        execute(client, Operation::Close { handle }).await?;
    }
    let temporary = root.join("pending");
    let handle = open(client, &temporary, false).await?;
    write(client, &handle, b'R', 0).await?;
    sync(client, &handle, SyncMode::All).await?;
    execute(
        client,
        Operation::Rename {
            source: temporary,
            destination: root.join("renamed"),
        },
    )
    .await?;
    sync(client, &directory, SyncMode::All).await?;
    execute(client, Operation::Close { handle }).await?;
    execute(client, Operation::Close { handle: directory }).await?;
    pool.shutdown().await;
    pool.join();
    Ok(json!({"prepared": true, "backend": "pool", "root": root}))
}

pub fn verify(root: &Path) -> Result<Value> {
    for (name, byte) in [("dsync", b'D'), ("datasync", b'A'), ("fsync", b'F')] {
        let bytes = fs::read(root.join(name))?;
        if bytes.len() != 8192
            || bytes[..4096] != vec![byte; 4096]
            || bytes[4096..] != vec![b'0'; 4096]
        {
            return Err(format!("{name}: durable overwrite/preallocation did not survive").into());
        }
    }
    if fs::read(root.join("dirty"))? != vec![b'0'; 8192] {
        return Err("unsynced overwrite was not lost; injector is unqualified".into());
    }
    if fs::read(root.join("renamed"))? != vec![b'R'; 4096] || root.join("pending").exists() {
        return Err("synchronized rename did not survive".into());
    }
    Ok(json!({"qualified": true, "backend": "pool",
        "limits": "file cache-loss only; no namespace rollback or hardware claim"}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_loss_image(root: &Path) {
        for (name, byte) in [("dsync", b'D'), ("datasync", b'A'), ("fsync", b'F')] {
            let mut data = vec![byte; 4096];
            data.extend(vec![b'0'; 4096]);
            fs::write(root.join(name), data).unwrap();
        }
        fs::write(root.join("dirty"), vec![b'0'; 8192]).unwrap();
        fs::write(root.join("renamed"), vec![b'R'; 4096]).unwrap();
    }

    #[test]
    fn qualification_requires_both_cache_loss_and_durable_preservation() {
        let root = tempfile::tempdir().unwrap();
        cache_loss_image(root.path());
        assert!(verify(root.path()).is_ok());
        fs::write(root.path().join("dirty"), vec![b'X'; 8192]).unwrap();
        assert!(
            verify(root.path())
                .unwrap_err()
                .to_string()
                .contains("not lost")
        );
        cache_loss_image(root.path());
        fs::write(root.path().join("dsync"), vec![0; 8192]).unwrap();
        assert!(
            verify(root.path())
                .unwrap_err()
                .to_string()
                .contains("dsync")
        );
    }

    #[tokio::test]
    async fn actual_pool_without_an_injector_cannot_qualify() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("qualification");
        prepare(&root).await.unwrap();
        assert!(verify(&root).unwrap_err().to_string().contains("not lost"));
    }
}
