package io.ozzy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

final class NativeFuture<T> extends CompletableFuture<T> {
    private final NativeResource owner;
    private final ClientConfig config;
    private final boolean close;
    private final Consumer consumer;
    private final AtomicLong task = new AtomicLong();
    private final AtomicBoolean returned = new AtomicBoolean();
    private final CountDownLatch released = new CountDownLatch(1);
    private Object value;
    private Throwable error;
    private boolean prepared;

    NativeFuture(NativeResource owner, ClientConfig config, boolean close, Consumer consumer) {
        this.owner = owner;
        this.config = config;
        this.close = close;
        this.consumer = consumer;
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
        try {
            install(Native.start(handle, op, this, topic, options, parts, key, id, identity));
        } catch (Throwable error) {
            released();
            completeExceptionally(error);
        }
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
                value = Native.wrap(kind, pointer, metadata, payload, config);
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
        if (owner != null) owner.exit();
        released.countDown();
        if (prepared) ForkJoinPool.commonPool().execute(this::finish);
    }

    @SuppressWarnings("unchecked")
    private void finish() {
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

    @Override
    public boolean cancel(boolean interrupt) {
        abortAndAwait();
        return !prepared && super.cancel(interrupt);
    }

    @Override
    public boolean complete(T value) {
        abortAndAwait();
        return !prepared && super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable error) {
        abortAndAwait();
        return !prepared && super.completeExceptionally(error);
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

    static <T> T await(CompletableFuture<T> future) {
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
