package io.ozzy;

import java.lang.ref.Reference;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Immutable observation of an admitted record. Dropping or canceling observers never withdraws its
 * write.
 */
public final class PendingRecord {
    private final long topicHi, topicLo, sequence;
    private final int partition;
    final NativeResource resource;

    PendingRecord(long pointer, long topicHi, long topicLo, int partition, long sequence) {
        this.topicHi = topicHi;
        this.topicLo = topicLo;
        this.partition = partition;
        this.sequence = sequence;
        resource = new NativeResource(pointer, false);
    }

    /** Topic UUID, constructed only when requested. */
    public UUID topicId() {
        return new UUID(topicHi, topicLo);
    }

    /** Numeric destination chosen before admission. */
    public int partition() {
        return partition;
    }

    /** Independent producer/partition retry sequence, represented as unsigned long bits. */
    public long sequence() {
        return sequence;
    }

    /**
     * Observe a receipt without waiting; raise SDK failures, otherwise return empty while pending.
     */
    public Optional<Receipt> tryConfirmed() {
        try {
            return Optional.ofNullable(Fast.confirmed(resource.pointer()));
        } finally {
            Reference.reachabilityFence(resource);
        }
    }

    /** Wait for durable confirmation. Simultaneous observers are supported. */
    public Receipt confirmed() {
        Receipt ready = tryConfirmed().orElse(null);
        return ready != null ? ready : NativeFuture.await(waiting());
    }

    /** Await confirmation; cancellation affects only this observer. */
    public CompletableFuture<Receipt> confirmedAsync() {
        Receipt ready = tryConfirmed().orElse(null);
        return ready != null ? CompletableFuture.completedFuture(ready) : waiting();
    }

    private CompletableFuture<Receipt> waiting() {
        var future = new NativeFuture<Receipt>(null, null, false, null);
        try {
            future.start(resource.pointer(), 6, null, null, null, null, null, null);
        } finally {
            Reference.reachabilityFence(resource);
        }
        return future;
    }
}
