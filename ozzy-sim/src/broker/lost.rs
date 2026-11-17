//! What a stopped broker loses between two runs.

use super::{device, pump, sync_directory};
use ozzy_broker::CheckedConfig;
use ozzy_io::simulation::{Image, ImageLimits};
use ozzy_io::{Class, FileKind, Local, Operation, Outcome};
use std::path::{Path, PathBuf};

/// The image without the tree at `path`, as after the loss of one store.
pub async fn lose(
    checked: &CheckedConfig,
    image: Image,
    limits: ImageLimits,
    path: &Path,
) -> Image {
    let (controller, mut clients) = device(checked, 1, image, limits);
    let (storage, _, task) = pump(controller, true);
    let io = Local::new(clients.pop().unwrap());
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let children = children(&io, &directory).await;
        let mut below = Vec::new();
        for (child, kind) in children {
            if kind == FileKind::Directory {
                below.push(child);
            } else {
                run(&io, Operation::RemoveFile { path: child }).await;
            }
        }
        if below.is_empty() {
            let path = directory.clone();
            run(&io, Operation::RemoveDirectory { path }).await;
        } else {
            pending.push(directory);
            pending.extend(below);
        }
    }
    sync_directory(&io, path.parent().unwrap()).await;
    drop(io);
    storage.stop();
    task.await.unwrap()
}

async fn run(io: &Local, operation: Operation) {
    io.execute(Class::Progress, operation).await.unwrap();
}

async fn children(io: &Local, directory: &Path) -> Vec<(PathBuf, FileKind)> {
    let listing = Operation::ReadDirectory {
        path: directory.to_path_buf(),
        max_entries: 4096,
        max_name_bytes: 4096 * 64,
    };
    let listed = io.execute(Class::Progress, listing).await.unwrap();
    let Outcome::Directory(entries) = &*listed else {
        panic!("directory listing returned no entries");
    };
    entries
        .iter()
        .map(|entry| (directory.join(&entry.name), entry.kind))
        .collect()
}
