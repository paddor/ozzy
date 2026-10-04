use super::*;

#[test]
fn async_suffix_execution_observation_crash_cuts_select_only_complete_histories() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    let protected = journal.accepted_position().unwrap();
    let mut next = journal.next_manifest().unwrap();
    next.accepted = protected;
    next.committed = protected;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let original = controller
        .image()
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    drop(journal);
    let image = controller.crash(true).unwrap().0;
    let ops = operations(protected, &BODIES[1..4], 1);
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..800 {
            let (mut controller, io) = setup(image.clone());
            let journal = drive(&mut controller, open(io, 3)).unwrap();
            let selected = journal.current();
            let replacement = request(&journal, position(ops[1]));
            let done = super::super::recovery::run_cut(
                &mut controller,
                async {
                    let mut installer = journal
                        .begin_suffix_replacement(replacement, position(ops[2]), stream_limits())
                        .await?;
                    for chunk in ops.chunks(2) {
                        installer.append_chunk(chunk).await?;
                    }
                    Box::pin(installer.finish()).await
                },
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 12)).unwrap();
            assert_eq!(
                controller
                    .image()
                    .bytes(Path::new("/group/segments/1.log"), false)
                    .unwrap(),
                original
            );
            if journal.current() == selected {
                assert!(!done);
                assert_eq!(journal.accepted_position().unwrap(), protected);
                assert_eq!(journal.committed_position().unwrap(), protected);
                assert_eq!(journal.manifest.promised_view, 0);
            } else {
                assert_eq!(journal.accepted_position().unwrap(), position(ops[2]));
                assert_eq!(journal.committed_position().unwrap(), position(ops[1]));
                assert_eq!(journal.manifest.last_normal_view, 2);
                assert_eq!(drive(&mut controller, replay(&journal)).len(), 4);
            }
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}

#[test]
fn async_suffix_abort_crash_cuts_never_delete_selected_history() {
    let image = baseline();
    let ops = operations(LogPosition::GENESIS, &BODIES[1..4], 1);
    let original = image
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    let mut finished = false;
    for cut in 0..80 {
        let (mut controller, io) = setup(image.clone());
        let journal = drive(&mut controller, open(io, 3)).unwrap();
        let selected = journal.current();
        let replacement = request(&journal, LogPosition::GENESIS);
        let mut installer = drive(
            &mut controller,
            journal.begin_suffix_replacement(replacement, position(ops[2]), stream_limits()),
        )
        .unwrap();
        for chunk in ops.chunks(2) {
            drive(&mut controller, installer.append_chunk(chunk)).unwrap();
        }
        let done = super::super::recovery::run_cut(&mut controller, installer.abort(), cut, false);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        let journal = drive(&mut controller, open(io, 12)).unwrap();
        assert_eq!(journal.current(), selected);
        assert_eq!(journal.accepted_position().unwrap().op_number, 1);
        assert_eq!(
            controller
                .image()
                .bytes(Path::new("/group/segments/1.log"), false)
                .unwrap(),
            original
        );
        if done {
            finished = true;
            break;
        }
    }
    assert!(finished);
}
