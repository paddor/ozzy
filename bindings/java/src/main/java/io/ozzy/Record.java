package io.ozzy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Immutable delivered record. Ordering and offsets are per partition. UUIDs are materialized on
 * access.
 */
public final class Record {
    private final long[] metadata;
    private final byte[][] payload;

    Record(long[] metadata, byte[][] payload) {
        this.metadata = metadata;
        this.payload = payload;
    }

    /**
     * Exact topic incarnation UUID.
     *
     * @return the topic UUID
     */
    public UUID topicId() {
        return new UUID(metadata[0], metadata[1]);
    }

    /**
     * Numeric partition.
     *
     * @return the partition number
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
     * Partition-global offset, represented as unsigned long bits.
     *
     * @return the offset as unsigned long bits
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
     * Read-only payload views with independent positions. These survive client closure.
     *
     * @return one read-only buffer per part, in order
     */
    public List<ByteBuffer> parts() {
        return Arrays.stream(payload).map(p -> ByteBuffer.wrap(p).asReadOnlyBuffer()).toList();
    }

    /**
     * Read-only view of a single-part record, rejecting multipart records.
     *
     * @return a read-only buffer of the only part
     */
    public ByteBuffer body() {
        if (payload.length != 1) throw new IllegalStateException("record is multipart");
        return ByteBuffer.wrap(payload[0]).asReadOnlyBuffer();
    }
}
