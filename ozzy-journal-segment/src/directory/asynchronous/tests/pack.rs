use super::*;
use crate::{PackLimits, SEGMENT_HEADER_BYTES, WRITE_GROUP_ALIGNMENT};

const PACK: PackLimits = PackLimits {
    max_group_bytes: 8192,
    read_ahead_bytes: crate::PACK_CHUNK_BYTES,
    max_probes: 4,
};

/// Seal `segments` segments of seven one-operation groups each, then write
/// one operation into the active segment.
fn sparse_journal(segments: usize) -> (Controller, Journal) {
    let (mut controller, mut journal) = empty_journal();
    for _ in 0..segments {
        for _ in 0..7 {
            drive(&mut controller, append_confirmed(&mut journal));
        }
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    }
    drive(&mut controller, append_confirmed(&mut journal));
    (controller, journal)
}

/// Read every group of a selected segment, which checks its seal digest,
/// chain, and that its group numbers end by the next segment's first.
fn scan(controller: &mut Controller, journal: &Journal, reference: SegmentReference) -> usize {
    drive(controller, async {
        let mut groups = journal.segment_groups(reference).await.unwrap();
        let mut operations = 0;
        while let Some(group) = groups.next().await.unwrap() {
            operations += group.operations.len();
        }
        groups.finish().await.unwrap();
        operations
    })
}

#[test]
fn sparse_segments_pack_into_one_that_reopens_strictly() {
    let (mut controller, mut journal) = sparse_journal(4);
    let before = journal.manifest().segments.clone();
    assert_eq!(before.len(), 5);
    let job = journal.prepare_pack(PACK).unwrap().expect("a sparse run");
    let mut outcome = drive(&mut controller, job.run()).unwrap();
    journal.record_pack_layouts(&outcome);
    let packed = outcome.take_packed().expect("the run fits one segment");
    assert_eq!(packed.replaced(), &before[..4]);
    let merged = packed.merged();
    assert_eq!(
        (merged.segment_id, merged.file_generation),
        (before[3].segment_id, 1)
    );
    assert_eq!(merged.first_chain, before[0].first_chain);
    // One 8 KiB group holds all 28 operations, numbered from the run's first.
    assert_eq!(merged.first_group_number, before[0].first_group_number);
    assert_eq!(
        merged.sealed.unwrap().valid_bytes,
        (SEGMENT_HEADER_BYTES + 2 * WRITE_GROUP_ALIGNMENT) as u64
    );
    drive(&mut controller, journal.install_pack(packed)).unwrap();
    assert_eq!(journal.manifest().segments, [merged, before[4]]);
    assert_eq!(scan(&mut controller, &journal, merged), 28);
    // Writes continue into the active segment, and nothing more packs.
    drive(&mut controller, append_confirmed(&mut journal));
    assert!(journal.prepare_pack(PACK).unwrap().is_none());

    drop(journal);
    let image = controller.crash(true).unwrap().0;
    let (mut controller, io) = setup(image);
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(journal.manifest().segments[0], merged);
    assert_eq!(scan(&mut controller, &journal, merged), 28);
}

#[test]
fn segments_around_repeated_packs_stay_readable() {
    let (mut controller, mut journal) = sparse_journal(3);
    for round in 0..2 {
        let job = journal.prepare_pack(PACK).unwrap().expect("a sparse run");
        let mut outcome = drive(&mut controller, job.run()).unwrap();
        journal.record_pack_layouts(&outcome);
        let packed = outcome.take_packed().unwrap();
        drive(&mut controller, journal.install_pack(packed)).unwrap();
        if round == 0 {
            // Three more sparse segments; the active one already holds one.
            for index in 0..3 {
                for _ in usize::from(index == 0)..7 {
                    drive(&mut controller, append_confirmed(&mut journal));
                }
                drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
            }
            drive(&mut controller, append_confirmed(&mut journal));
        }
    }
    // The second pack left the first alone; a gap in group numbers now
    // follows each packed segment.
    let sealed = journal.manifest().segments.len() - 1;
    assert_eq!(sealed, 2);
    let count = |controller: &mut Controller, journal: &Journal| {
        journal.manifest().segments[..sealed]
            .iter()
            .map(|reference| scan(controller, journal, *reference))
            .sum::<usize>()
    };
    assert_eq!(count(&mut controller, &journal), 42);
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(count(&mut controller, &journal), 42);
}

#[test]
fn a_pack_whose_run_moved_on_installs_nothing() {
    let (mut controller, mut journal) = sparse_journal(3);
    let job = journal.prepare_pack(PACK).unwrap().expect("a sparse run");
    // A second pack of the same run selects first; the first one is stale.
    let other = journal.prepare_pack(PACK).unwrap().expect("a sparse run");
    let mut first = drive(&mut controller, job.run()).unwrap();
    let mut second = drive(&mut controller, other.run()).unwrap();
    let first = first.take_packed().unwrap();
    let second = second.take_packed().unwrap();
    assert_ne!(
        first.merged().file_generation,
        second.merged().file_generation
    );
    drive(&mut controller, journal.install_pack(second)).unwrap();
    assert!(!journal.pack_selected(&first));
    assert!(matches!(
        drive(&mut controller, journal.install_pack(first)),
        Err(DirectoryError::PackMismatch)
    ));
    assert!(!journal.is_faulted());
    let merged = journal.manifest().segments[0];
    assert_eq!(scan(&mut controller, &journal, merged), 21);
}

#[test]
fn every_pack_and_install_cut_recovers_one_selection() {
    let image = {
        let (controller, journal) = sparse_journal(4);
        drop(journal);
        controller.crash(true).unwrap().0
    };
    let mut finished = false;
    for cut in 0..400 {
        let (mut controller, io) = setup(image.clone());
        let mut journal = drive(&mut controller, open(io, 2)).unwrap();
        let job = journal.prepare_pack(PACK).unwrap().expect("a sparse run");
        let done = {
            let mut future = std::pin::pin!(async {
                let mut outcome = job.run().await.unwrap();
                let packed = outcome.take_packed().unwrap();
                journal.install_pack(packed).await.unwrap();
            });
            let mut done = false;
            for _ in 0..cut {
                if poll(future.as_mut()).is_ready() {
                    done = true;
                    break;
                }
                let (id, _) = controller.jobs()[0];
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            done
        };
        drop(journal);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        let journal = drive(&mut controller, open(io, 3)).unwrap();
        let segments = journal.manifest.segments.len();
        assert!([2, 5].contains(&segments), "cut {cut}: {segments} segments");
        let mut replayed = Vec::new();
        drive(
            &mut controller,
            journal.replay_accepted(|item| {
                replayed.push(item.operation.op_number);
                Ok::<_, io::Error>(())
            }),
        )
        .unwrap();
        assert_eq!(replayed, (1..=29).collect::<Vec<_>>(), "cut {cut}");
        if done {
            assert_eq!(segments, 2);
            finished = true;
            break;
        }
    }
    assert!(finished, "pack did not finish within test cut bound");
}
