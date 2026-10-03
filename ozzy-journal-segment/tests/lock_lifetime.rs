//! Unrelated fork/exec must not extend a finished store owner's lock lifetime.
#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::Duration;

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    DecodeLimits, Digest, DirectoryError, GroupDirectory, GroupIdentity, MetadataLimits,
    MountPolicy, OperationLimits, PlacementDirectory, PlacementLimits, SegmentHeader,
    VolumeDirectory, VolumeIdentity,
};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

struct HeldFork {
    gate: UnixStream,
    task: Option<JoinHandle<()>>,
}

impl HeldFork {
    fn start() -> Self {
        let (gate, child_gate) = UnixStream::pair().unwrap();
        gate.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.arg("--list").stdout(Stdio::null());
        // SAFETY: The post-fork closure only invokes async-signal-safe read/write
        // syscalls through rustix, on a socket allocated before fork. It neither
        // allocates nor locks. Parent release or EOF always releases the child.
        unsafe {
            command.pre_exec(move || {
                rustix::io::write(&child_gate, b"R")?;
                let mut release = [0];
                loop {
                    match rustix::io::read(&child_gate, &mut release) {
                        Err(rustix::io::Errno::INTR) => {}
                        result => return result.map(|_| ()).map_err(Into::into),
                    }
                }
            });
        }
        let task = std::thread::spawn(move || {
            let mut child = command.spawn().unwrap();
            assert!(child.wait().unwrap().success());
        });
        let mut held = Self {
            gate,
            task: Some(task),
        };
        let mut ready = [0];
        held.gate.read_exact(&mut ready).unwrap();
        assert_eq!(ready, *b"R");
        held
    }
}

impl Drop for HeldFork {
    fn drop(&mut self) {
        let _ = self.gate.write_all(b"G");
        if let Some(task) = self.task.take() {
            let result = task.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

#[test]
fn group_reopens_after_owner_drop_while_unrelated_child_is_before_exec() {
    let temporary = tempfile::TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let identity = identity();
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8192).unwrap();
    let directory = GroupDirectory::format_new(&root, identity, 1, &header).unwrap();
    let _child = HeldFork::start();
    drop(directory);
    let reopened = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
    assert_eq!(reopened.identity(), identity);
}

#[test]
fn detached_pin_keeps_ownership_until_its_release_even_with_a_forked_child() {
    let temporary = tempfile::TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let identity = identity();
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8192).unwrap();
    let journal = GroupDirectory::format_new(&root, identity, 1, &header)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let pin = journal.pin_segments(&[1]).unwrap();
    let _child = HeldFork::start();
    drop(journal);
    // A failed competing open must not unlock the reader's lease, either.
    for _ in 0..2 {
        assert!(matches!(
            GroupDirectory::open(&root, identity, MetadataLimits::default()),
            Err(DirectoryError::Locked)
        ));
    }
    drop(pin);
    let reopened = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
    assert_eq!(reopened.identity(), identity);
}

#[test]
fn volume_reopens_after_owner_drop_while_unrelated_child_is_before_exec() {
    let temporary = tempfile::TempDir::new().unwrap();
    let identity = VolumeIdentity {
        volume_id: identity().volume_id,
    };
    let volume =
        VolumeDirectory::format_new(temporary.path(), identity, MountPolicy::PortableIdentity)
            .unwrap();
    let _child = HeldFork::start();
    drop(volume);
    let reopened =
        VolumeDirectory::open(temporary.path(), identity, MountPolicy::PortableIdentity).unwrap();
    assert_eq!(reopened.identity(), identity);
}

#[test]
fn placement_reopens_after_owner_drop_while_unrelated_child_is_before_exec() {
    let temporary = tempfile::TempDir::new().unwrap();
    let root = temporary.path().join("placement");
    let node = identity().replica_node_id;
    let directory =
        PlacementDirectory::format_new(&root, node, Vec::new(), PlacementLimits::default())
            .unwrap();
    let _child = HeldFork::start();
    drop(directory);
    let reopened = PlacementDirectory::open(&root, node, PlacementLimits::default()).unwrap();
    assert_eq!(reopened.current().generation, 1);
}
