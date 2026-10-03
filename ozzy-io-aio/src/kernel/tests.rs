use super::*;
use std::os::unix::fs::OpenOptionsExt;

/// A file opened with `O_DIRECT`, or `None` where the file system rejects it.
fn direct_file(path: &std::path::Path, write: bool) -> Option<Arc<File>> {
    std::fs::write(path, vec![0; 4 * ALIGNMENT]).unwrap();
    match std::fs::OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Some(Arc::new(file)),
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipped: file system rejects O_DIRECT: {error}");
            None
        }
        Err(error) => panic!("{error}"),
    }
}

fn buffer(byte: u8, len: usize) -> AlignedBuf {
    let mut buffer = AlignedBuf::default();
    let bytes = vec![byte; len];
    buffer.fill(len, std::iter::once(bytes.as_slice())).unwrap();
    buffer
}

#[test]
fn buffers_reject_malformed_lengths_without_reallocation_after_alignment() {
    let mut buffer = AlignedBuf::default();
    for (total, slices) in [(1, vec![&b"too long"[..]]), (2, vec![&b"a"[..]])] {
        assert_eq!(
            buffer.fill(total, slices.into_iter()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(
        buffer
            .fill(usize::MAX, std::iter::empty())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    buffer.fill(4096, std::iter::once(&[1; 4096][..])).unwrap();
    assert_eq!(buffer.as_slice(), &[1; 4096]);
    assert!((buffer.as_slice().as_ptr() as usize).is_multiple_of(ALIGNMENT));
    assert!(AioContext::<()>::new(0).is_err());
    assert!(AioContext::<()>::new(66).is_err());
}

#[test]
fn write_is_read_back_and_signals_the_eventfd() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(4).unwrap();
    context
        .submit_write(file, ALIGNMENT as u64, buffer(7, 2 * ALIGNMENT), 11)
        .unwrap();
    assert_eq!(context.in_flight(), 1);
    let mut done = Vec::new();
    while done.is_empty() {
        context
            .wait(|job, buffer, result| done.push((job, buffer.as_slice().len(), result.unwrap())))
            .unwrap();
    }
    assert_eq!(done, [(11, 2 * ALIGNMENT, 2 * ALIGNMENT)]);
    assert_eq!(context.in_flight(), 0);
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes[..ALIGNMENT].iter().all(|&byte| byte == 0));
    assert!(
        bytes[ALIGNMENT..3 * ALIGNMENT]
            .iter()
            .all(|&byte| byte == 7)
    );

    // A completion makes the eventfd readable until the next reap drains it.
    let file = direct_file(&temporary.path().join("second"), true).unwrap();
    context
        .submit_write(file, 0, buffer(1, ALIGNMENT), 12)
        .unwrap();
    let mut poll = [libc::pollfd {
        fd: context.as_fd().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }];
    // SAFETY: one live pollfd.
    assert_eq!(unsafe { libc::poll(poll.as_mut_ptr(), 1, 5000) }, 1);
    let mut reaped = 0;
    while reaped == 0 {
        reaped = context
            .reap(|job, _, result| {
                assert_eq!(job, 12);
                result.unwrap();
            })
            .unwrap();
    }
    // A completion may signal between a drain and the collection that reaps
    // it, leaving one spurious signal. With nothing in flight, one more reap
    // clears it for good.
    assert_eq!(context.reap(|_, _, _| unreachable!()).unwrap(), 0);
    poll[0].revents = 0;
    // SAFETY: one live pollfd.
    assert_eq!(unsafe { libc::poll(poll.as_mut_ptr(), 1, 0) }, 0);
}

#[test]
fn unusable_writes_come_back_before_the_kernel() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(1).unwrap();
    let buffered = Arc::new(std::fs::OpenOptions::new().write(true).open(&path).unwrap());
    let (job, _, error) = context
        .submit_write(buffered, 0, buffer(1, ALIGNMENT), 1)
        .unwrap_err();
    assert_eq!((job, error.kind()), (1, io::ErrorKind::InvalidInput));
    let (_, _, error) = context
        .submit_write(Arc::clone(&file), 100, buffer(1, ALIGNMENT), 2)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let (_, _, error) = context
        .submit_write(Arc::clone(&file), 0, AlignedBuf::default(), 3)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(context.in_flight(), 0);

    let (_, _, error) = context
        .submit_write(
            Arc::clone(&file),
            (i64::MAX as u64) & !4095,
            buffer(1, ALIGNMENT),
            3,
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    // A full context hands the job back; after a reap it accepts again.
    context
        .submit_write(Arc::clone(&file), 0, buffer(2, ALIGNMENT), 4)
        .unwrap();
    let (job, _, error) = context
        .submit_write(Arc::clone(&file), 0, buffer(3, ALIGNMENT), 5)
        .unwrap_err();
    assert_eq!((job, error.kind()), (5, io::ErrorKind::WouldBlock));
    context
        .wait(|_, _, result| {
            result.unwrap();
        })
        .unwrap();
    context
        .submit_write(file, 0, buffer(3, ALIGNMENT), 5)
        .unwrap();
    context
        .wait(|_, _, result| {
            result.unwrap();
        })
        .unwrap();
}

#[test]
fn kernel_errors_arrive_as_failed_completions() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment");
    let Some(read_only) = direct_file(&path, false) else {
        return;
    };
    let mut context = AioContext::<u32>::new(1).unwrap();
    // Most kernels reject the request at submission; otherwise it completes
    // with the error.
    let error =
        if let Err((_, _, error)) = context.submit_write(read_only, 0, buffer(1, ALIGNMENT), 1) {
            error
        } else {
            let mut error = None;
            while error.is_none() {
                context
                    .wait(|_, _, result| error = Some(result.unwrap_err()))
                    .unwrap();
            }
            error.unwrap()
        };
    assert_eq!(error.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn dropping_the_context_waits_for_writes_in_flight() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(4).unwrap();
    for block in 0..4 {
        context
            .submit_write(
                Arc::clone(&file),
                (block * ALIGNMENT) as u64,
                buffer(block as u8 + 1, ALIGNMENT),
                block as u32,
            )
            .unwrap();
    }
    drop(context);
    let bytes = std::fs::read(&path).unwrap();
    for block in 0..4 {
        assert!(
            bytes[block * ALIGNMENT..][..ALIGNMENT]
                .iter()
                .all(|&byte| byte == block as u8 + 1)
        );
    }
}
