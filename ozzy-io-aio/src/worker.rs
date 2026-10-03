use crate::kernel::{AioContext, AlignedBuf};
use ozzy_io::Class;
use ozzy_io_pool::direct::{OwnedFile, Queue, Worker, Write};
use rustix::event::{EventfdFlags, PollFd, PollFlags, eventfd, poll};
use std::{
    io,
    os::fd::OwnedFd,
    sync::Arc,
    task::{Wake, Waker},
};

pub(crate) struct Driver {
    pub(crate) depth: usize,
}

#[derive(Debug)]
struct Signal(OwnedFd);

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        loop {
            match rustix::io::write(&self.0, &1_u64.to_ne_bytes()) {
                Err(rustix::io::Errno::INTR) => {}
                // EAGAIN means the counter is already readable. The descriptor
                // remains owned through this call and cannot be concurrently closed.
                Ok(_) | Err(rustix::io::Errno::AGAIN) => return,
                Err(error) => panic!("AIO worker wake failed: {error}"),
            }
        }
    }
}

impl Signal {
    fn clear(&self) -> io::Result<()> {
        loop {
            match rustix::io::read(&self.0, &mut [0_u8; 8]) {
                Err(rustix::io::Errno::INTR) => {}
                Ok(_) | Err(rustix::io::Errno::AGAIN) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl Worker for Driver {
    fn run(self: Box<Self>, queue: &mut Queue) -> io::Result<()> {
        let signal = Arc::new(Signal(eventfd(
            0,
            EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK,
        )?));
        let waker = Waker::from(signal.clone());
        let mut context = AioContext::<Write, OwnedFile>::new(self.depth + 1)?;
        let mut data = 0;
        let mut progress = 0;
        loop {
            // Clear first, then register and inspect. A wake before register
            // is covered by the subsequent state inspection. Never clear a
            // wake after registration: it may have consumed that registration.
            signal.clear()?;
            queue.register(&waker);
            context.reap(|write, buffer, result| {
                match write.class() {
                    Class::Data => data -= 1,
                    Class::Progress => progress -= 1,
                }
                // No uncharged staging cache: release scratch before the reply.
                drop(buffer);
                write.finish(result);
            })?;
            for (class, count, limit) in [
                (Class::Progress, &mut progress, 1),
                (Class::Data, &mut data, self.depth),
            ] {
                while *count < limit {
                    let Some(write) = queue.try_recv(class) else {
                        break;
                    };
                    let mut buffer = AlignedBuf::default();
                    if let Err(error) = buffer.fill(
                        write.data().len(),
                        write.data().parts().iter().map(AsRef::as_ref),
                    ) {
                        drop(buffer);
                        write.finish(Err(error));
                        continue;
                    }
                    match context.submit_write(write.file(), write.offset(), buffer, write) {
                        Ok(()) => *count += 1,
                        Err((write, buffer, error)) => {
                            drop(buffer);
                            write.finish(Err(error));
                        }
                    }
                }
            }
            if queue.drained() {
                return Ok(());
            }
            let mut descriptors = [
                PollFd::new(&signal.0, PollFlags::IN),
                PollFd::new(&context, PollFlags::IN),
            ];
            loop {
                match poll(&mut descriptors, None) {
                    Err(rustix::io::Errno::INTR) => {}
                    Ok(_) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            if descriptors.iter().any(|fd| {
                fd.revents()
                    .intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL)
            }) {
                return Err(io::Error::other("AIO worker readiness failed"));
            }
        }
    }
}
