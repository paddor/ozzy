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
    private final long pointer;
    private final PendingHandles lifetime;

    PendingRecord(
            NativeResource owner,
            long pointer,
            long topicHi,
            long topicLo,
            int partition,
            long sequence) {
        this.topicHi = topicHi;
        this.topicLo = topicLo;
        this.partition = partition;
        this.sequence = sequence;
        this.pointer = pointer;
        lifetime = owner.observe(pointer);
    }

    /**
     * Topic UUID, constructed only when requested.
     *
     * @return the topic UUID
     */
    public UUID topicId() {
        return new UUID(topicHi, topicLo);
    }

    /**
     * Numeric destination chosen before admission.
     *
     * @return the destination partition
     */
    public int partition() {
        return partition;
    }

    /**
     * Independent producer/partition retry sequence, represented as unsigned long bits.
     *
     * @return the retry sequence as unsigned long bits
     */
    public long sequence() {
        return sequence;
    }

    /**
     * Observe a receipt without waiting; raise SDK failures, otherwise return empty while pending.
     *
     * @return the receipt, or empty while pending
     */
    public Optional<Receipt> tryConfirmed() {
        try {
            return Optional.ofNullable(Fast.confirmed(pointer));
        } finally {
            Reference.reachabilityFence(lifetime);
        }
    }

    /**
     * Wait for durable confirmation. Simultaneous observers are supported.
     *
     * @return the durable confirmation
     */
    public Receipt confirmed() {
        Receipt ready = tryConfirmed().orElse(null);
        return ready != null ? ready : NativeFuture.await(waiting());
    }

    /**
     * Await confirmation; cancellation affects only this observer. A failure, even one known
     * already, completes the future exceptionally.
     *
     * @return a future that completes with the durable confirmation
     */
    public CompletableFuture<Receipt> confirmedAsync() {
        Receipt ready;
        try {
            ready = tryConfirmed().orElse(null);
        } catch (OzzyException error) {
            return CompletableFuture.failedFuture(error);
        }
        return ready != null ? CompletableFuture.completedFuture(ready) : waiting();
    }

    private CompletableFuture<Receipt> waiting() {
        var future = new NativeFuture<Receipt>(null, null, false, null);
        try {
            future.start(pointer, 6, null, null, null, null, null, null);
        } finally {
            Reference.reachabilityFence(lifetime);
        }
        return future;
    }
}
