package io.ozzy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class NativeResource {
    private static final Cleaner CLEANER = Cleaner.create();
    private final State state;
    private final Cleaner.Cleanable cleanup;
    private final AtomicBoolean busy = new AtomicBoolean();
    private volatile boolean sealed;
    private volatile NativeFuture<Void> closing;
    final MemorySegment scratch;

    NativeResource(long pointer, boolean mutable) {
        Arena arena = mutable ? Arena.ofShared() : null;
        try {
            scratch = arena == null ? null : arena.allocate(128, 8);
            state = new State(arena);
            cleanup = CLEANER.register(this, state);
            // Nothing that can allocate or throw follows native ownership transfer.
            state.pointer.set(pointer);
        } catch (Throwable error) {
            if (arena != null) arena.close();
            throw error;
        }
    }

    long pointer() {
        return state.pointer.get();
    }

    long enter() {
        return enter(false);
    }

    private long enter(boolean closingOperation) {
        if (pointer() == 0 || (sealed && !closingOperation)) throw new ClosedException();
        if (!busy.compareAndSet(false, true)) throw new BusyException();
        if (pointer() == 0 || (sealed && !closingOperation)) {
            exit();
            throw new ClosedException();
        }
        return pointer();
    }

    void exit() {
        busy.set(false);
        Reference.reachabilityFence(this);
    }

    boolean isClosed() {
        return pointer() == 0;
    }

    CompletableFuture<Void> closeAsync(Consumer consumer) {
        if (isClosed()) return CompletableFuture.completedFuture(null);
        NativeFuture<Void> previous = closing;
        if (previous != null && !previous.isCancelled() && !previous.isCompletedExceptionally())
            return previous;
        long pointer = enter(true);
        sealed = true;
        NativeFuture<Void> future;
        try {
            future = new NativeFuture<>(this, null, true, consumer);
        } catch (Throwable error) {
            exit();
            throw error;
        }
        closing = future;
        future.start(pointer, 7, null, null, null, null, null, null);
        return future;
    }

    // Called only after Rust has restored the Box, while this lease is still held.
    void disposeLeased() {
        cleanup.clean();
    }

    void discard() {
        if (isClosed()) return;
        enter(true);
        try {
            cleanup.clean();
        } finally {
            exit();
        }
    }

    private static final class State implements Runnable {
        final AtomicLong pointer = new AtomicLong();
        final Arena arena;

        State(Arena arena) {
            this.arena = arena;
        }

        @Override
        public void run() {
            long value = pointer.getAndSet(0);
            try {
                if (value != 0) Native.release(value);
            } finally {
                if (arena != null) arena.close();
            }
        }
    }
}
