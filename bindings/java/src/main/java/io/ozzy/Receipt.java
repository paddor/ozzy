package io.ozzy;

import java.util.UUID;

/** Immutable durable confirmation. It does not establish exactly-once application processing. */
public final class Receipt {
    /** Broker durability boundary. Single-broker mode has no failover. */
    public enum Policy {
        /** One broker's durable journal. */
        LOCAL_DURABLE,
        /** Two of three eligible durable copies. */
        QUORUM_DURABLE
    }

    /** Stored, first-wins duplicate, or scheduled result. */
    public enum Disposition {
        /** Newly stored event. */
        STORED,
        /** Existing event, using its original offset, when broker deduplication is enabled. */
        DUPLICATE,
        /** Held until its due time; the offset is its schedule sequence. */
        SCHEDULED
    }

    private final long[] metadata;

    Receipt(long[] metadata) {
        this.metadata = metadata;
    }

    /**
     * Topic UUID, constructed only when requested.
     *
     * @return the topic UUID
     */
    public UUID topicId() {
        return new UUID(metadata[0], metadata[1]);
    }

    /**
     * Numeric destination partition.
     *
     * @return the destination partition
     */
    public int partition() {
        return (int) metadata[2];
    }

    /**
     * Partition incarnation UUID.
     *
     * @return the partition incarnation UUID
     */
    public UUID incarnation() {
        return new UUID(metadata[3], metadata[4]);
    }

    /**
     * Partition-global offset, or the schedule sequence of a scheduled record, represented as
     * unsigned long bits.
     *
     * @return the offset or schedule sequence as unsigned long bits
     */
    public long offset() {
        return metadata[5];
    }

    /**
     * Application event UUID.
     *
     * @return the event UUID
     */
    public UUID messageId() {
        return new UUID(metadata[6], metadata[7]);
    }

    /**
     * Submitting producer UUID.
     *
     * @return the producer UUID
     */
    public UUID producerId() {
        return new UUID(metadata[8], metadata[9]);
    }

    /**
     * Producer fencing epoch, represented as unsigned long bits.
     *
     * @return the producer epoch as unsigned long bits
     */
    public long producerEpoch() {
        return metadata[10];
    }

    /**
     * Confirming partition owner epoch, represented as unsigned long bits.
     *
     * @return the owner epoch as unsigned long bits
     */
    public long ownerEpoch() {
        return metadata[11];
    }

    /**
     * Retry sequence for this producer and partition, represented as unsigned long bits.
     *
     * @return the retry sequence as unsigned long bits
     */
    public long sequence() {
        return metadata[12];
    }

    /**
     * Stored, first-wins duplicate, or scheduled result.
     *
     * @return {@link Disposition#STORED}, {@link Disposition#DUPLICATE}, or {@link
     *     Disposition#SCHEDULED}
     */
    public Disposition disposition() {
        return Disposition.values()[(int) metadata[13]];
    }

    /**
     * Local or quorum durability.
     *
     * @return the durability policy that confirmed the record
     */
    public Policy policy() {
        return metadata[14] == 1 ? Policy.LOCAL_DURABLE : Policy.QUORUM_DURABLE;
    }
}
