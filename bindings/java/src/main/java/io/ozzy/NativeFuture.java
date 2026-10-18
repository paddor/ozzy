package io.ozzy;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

final class NativeFuture<T> extends CompletableFuture<T> {
    // Fires timeouts by handing them to the common pool. A canceled native
    // operation waits for its lease, which must not stall a shared timer thread.
    private static final ScheduledThreadPoolExecutor TIMERS = timers();
    private final NativeResource owner;
    private final ClientConfig config;
    private final boolean close;
    private final Consumer consumer;
    private final AtomicLong task = new AtomicLong();
    private final AtomicBoolean returned = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final CountDownLatch released = new CountDownLatch(1);
    private Object value;
    private Throwable error;
    private boolean prepared;
    private volatile boolean aborting;
    private volatile boolean blocking;
    private volatile Thread starter;

    NativeFuture(NativeResource owner, ClientConfig config, boolean close, Consumer consumer) {
        this.owner = owner;
        this.config = config;
        this.close = close;
        this.consumer = consumer;
    }

    private static ScheduledThreadPoolExecutor timers() {
        var timers =
                new ScheduledThreadPoolExecutor(
                        1,
                        work -> {
                            Thread thread = new Thread(work, "ozzy-timeouts");
                            thread.setDaemon(true);
                            return thread;
                        });
        timers.setRemoveOnCancelPolicy(true);
        return timers;
    }

    static <T> NativeFuture<T> leased(
            NativeResource owner,
            int operation,
            String topic,
            long[] options,
            byte[][] parts,
            byte[] key,
            long[] id,
            byte[] identity,
            ClientConfig config) {
        NativeFuture<T> future;
        try {
            future = new NativeFuture<>(owner, config, false, null);
        } catch (Throwable error) {
            owner.exit();
            throw error;
        }
        future.start(owner.pointer(), operation, topic, options, parts, key, id, identity);
        return future;
    }

    void start(
            long handle,
            int op,
            String topic,
            long[] options,
            byte[][] parts,
            byte[] key,
            long[] id,
            byte[] identity) {
        // Results delivered during this call complete inline: no dependent exists yet.
        starter = Thread.currentThread();
        try {
            install(Native.start(handle, op, this, topic, options, parts, key, id, identity));
        } catch (Throwable failure) {
            failedToStart(failure);
        } finally {
            starter = null;
        }
    }

    void open(String[] endpoints, long[] ids, long[] settings) {
        starter = Thread.currentThread();
        try {
            install(Native.open(this, endpoints, ids, settings));
        } catch (Throwable failure) {
            failedToStart(failure);
        } finally {
            starter = null;
        }
    }

    // Native start never released this lease, so this thread reports the failure.
    private void failedToStart(Throwable failure) {
        if (!returned.get()) {
            error = failure;
            prepared = true;
        }
        released();
    }

    void install(long token) {
        if (token == 0) return;
        if (!task.compareAndSet(0, token)) {
            Native.cancel(token);
            return;
        }
        if (isDone()) clearTask();
    }

    private void clearTask() {
        long token = task.getAndSet(0);
        if (token != 0) Native.cancel(token);
    }

    // Native callbacks perform ownership bookkeeping only. User continuations run elsewhere.
    void completeNative(
            int kind, long pointer, long[] metadata, byte[][] payload, String diagnostic) {
        try {
            if (diagnostic != null) error = new OzzyException(diagnostic);
            else {
                value = Native.wrap(kind, pointer, metadata, payload, config, owner);
                if (close) {
                    if (consumer != null) consumer.captureClosed(owner.pointer());
                    owner.disposeLeased();
                }
            }
        } catch (Throwable failure) {
            error = failure;
        }
        prepared = true;
    }

    void failed(String message) {
        error = new OzzyException(message);
        prepared = true;
    }

    void released() {
        if (!returned.compareAndSet(false, true)) return;
        // Without a result or an abort, delivery failed; never leave callers waiting.
        if (!prepared && !aborting) {
            error = new OzzyException("native operation ended without a result");
            prepared = true;
        }
        if (owner != null) owner.exit();
        released.countDown();
        if (!prepared) return;
        if (starter == Thread.currentThread()) finish();
        else if (!blocking) ForkJoinPool.commonPool().execute(this::finish);
    }

    @SuppressWarnings("unchecked")
    private void finish() {
        if (!finished.compareAndSet(false, true)) return;
        Object result = value;
        value = null;
        try {
            boolean accepted =
                    error == null ? super.complete((T) result) : super.completeExceptionally(error);
            if (!accepted) Native.discard(result);
        } finally {
            clearTask();
        }
    }

    private void abortAndAwait() {
        aborting = true;
        clearTask();
        boolean interrupted = false;
        for (; ; ) {
            try {
                released.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /** A native result wins: deliver it and report that this completion lost. */
    private boolean preparedWins() {
        if (!prepared) return false;
        finish();
        return true;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        abortAndAwait();
        return !preparedWins() && super.cancel(interrupt);
    }

    @Override
    public boolean complete(T value) {
        abortAndAwait();
        return !preparedWins() && super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable error) {
        abortAndAwait();
        return !preparedWins() && super.completeExceptionally(error);
    }

    @Override
    public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        Objects.requireNonNull(unit);
        if (!isDone()) {
            Runnable expire =
                    () -> {
                        if (!isDone()) completeExceptionally(new TimeoutException());
                    };
            schedule(expire, timeout, unit);
        }
        return this;
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        Objects.requireNonNull(unit);
        if (!isDone()) {
            Runnable expire =
                    () -> {
                        if (!isDone()) complete(value);
                    };
            schedule(expire, timeout, unit);
        }
        return this;
    }

    private void schedule(Runnable expire, long timeout, TimeUnit unit) {
        ScheduledFuture<?> timer =
                TIMERS.schedule(
                        () -> ForkJoinPool.commonPool().execute(expire), timeout, unit);
        whenComplete((ignored, failure) -> timer.cancel(false));
    }

    @Override
    public void obtrudeValue(T value) {
        throw new UnsupportedOperationException("native operation owns completion");
    }

    @Override
    public void obtrudeException(Throwable error) {
        throw new UnsupportedOperationException("native operation owns completion");
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        throw new UnsupportedOperationException("native operation owns completion");
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        throw new UnsupportedOperationException("native operation owns completion");
    }

    // A blocking caller completes the future itself, skipping the common pool.
    private void awaitRelease() {
        blocking = true;
        try {
            released.await();
        } catch (InterruptedException interrupted) {
            // get() observes the interrupt and cancels.
            Thread.currentThread().interrupt();
            return;
        }
        if (prepared) finish();
    }

    static <T> T await(CompletableFuture<T> future) {
        if (future instanceof NativeFuture<T> pending) pending.awaitRelease();
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            boolean cancelled = future.cancel(true);
            Thread.currentThread().interrupt();
            if (!cancelled) {
                try {
                    return future.join();
                } catch (java.util.concurrent.CompletionException failed) {
                    if (failed.getCause() instanceof RuntimeException error) throw error;
                    if (failed.getCause() instanceof Error error) throw error;
                    throw new OzzyException("SDK operation failed", failed.getCause());
                }
            }
            throw new OzzyException("SDK operation interrupted", interrupted);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException error) throw error;
            if (failed.getCause() instanceof Error error) throw error;
            throw new OzzyException("SDK operation failed", failed.getCause());
        }
    }
}
