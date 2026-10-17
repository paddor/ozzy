package io.ozzy;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** One topic reader. Delivery and checkpoints do not acknowledge application processing. */
public final class Consumer implements AutoCloseable, Iterable<Record> {
    final NativeResource resource;
    private volatile ConsumerStats finalStats;
    private volatile Checkpoint finalCheckpoint;

    Consumer(long pointer) {
        resource = new NativeResource(pointer, true);
    }

    /** Blocking iteration until closed. Break explicitly when application work is complete. */
    @Override
    public java.util.Iterator<Record> iterator() {
        return new java.util.Iterator<>() {
            @Override
            public boolean hasNext() {
                return !resource.isClosed();
            }

            @Override
            public Record next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                return Consumer.this.next();
            }
        };
    }

    /**
     * Wait for one record, preserving order within each partition. Interruption cancels a pending
     * receive and preserves interrupt status. A delivered native result wins cancellation races.
     */
    public Record next() {
        Object result = submit();
        if (result instanceof Record record) return record;
        @SuppressWarnings("unchecked")
        var future = (CompletableFuture<Record>) result;
        return NativeFuture.await(future);
    }

    /**
     * Await one record. Canceling pending work returns its native lease before cancel completes.
     * If native delivery already completed, cancellation returns false and its result remains
     * available.
     */
    public CompletableFuture<Record> nextAsync() {
        Object result = submit();
        if (result instanceof Record record) return CompletableFuture.completedFuture(record);
        @SuppressWarnings("unchecked")
        var future = (CompletableFuture<Record>) result;
        return future;
    }

    private Object submit() {
        resource.enter();
        try {
            Record record = Fast.next(resource);
            if (record != null) {
                resource.exit();
                return record;
            }
        } catch (Throwable error) {
            resource.exit();
            throw error;
        }
        return NativeFuture.leased(resource, 5, null, null, null, null, null, null, null);
    }

    /**
     * Return a ready record or empty without waiting. Overlapping mutable operations raise
     * BusyException.
     */
    public Optional<Record> tryNext() {
        resource.enter();
        try {
            return Optional.ofNullable(Fast.next(resource));
        } finally {
            resource.exit();
        }
    }

    /**
     * Snapshot first undelivered offsets. Persist only after processing succeeds. Available after
     * close.
     */
    public Checkpoint checkpoint() {
        if (resource.isClosed()) return finalCheckpoint;
        long pointer = resource.enter();
        try {
            return checkpoint(Native.inspect(pointer, 1));
        } finally {
            resource.exit();
        }
    }

    /** Return cumulative delivery and gap-repair counters, including final counters after close. */
    public ConsumerStats stats() {
        if (resource.isClosed()) return finalStats;
        long pointer = resource.enter();
        try {
            return stats(Native.inspect(pointer, 2));
        } finally {
            resource.exit();
        }
    }

    void captureClosed(long pointer) {
        finalCheckpoint = checkpoint(Native.inspect(pointer, 1));
        finalStats = stats(Native.inspect(pointer, 2));
    }

    private static Checkpoint checkpoint(long[] words) {
        Map<Integer, Long> positions = new LinkedHashMap<>();
        for (int i = 2; i < words.length; i += 2) positions.put((int) words[i], words[i + 1]);
        return new Checkpoint(new UUID(words[0], words[1]), positions);
    }

    private static ConsumerStats stats(long[] words) {
        return new ConsumerStats(words[0], words[1], words[2]);
    }

    /** Release subscriptions and retain final checkpoint/statistics; idempotent. */
    @Override
    public void close() {
        NativeFuture.await(closeAsync());
    }

    /** Release asynchronously. Cancel an outstanding receive before closing. */
    public CompletableFuture<Void> closeAsync() {
        return resource.closeAsync(this);
    }
}
