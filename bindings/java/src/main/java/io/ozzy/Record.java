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

    /** Exact topic incarnation UUID. */
    public UUID topicId() {
        return new UUID(metadata[0], metadata[1]);
    }

    /** Numeric partition. */
    public int partition() {
        return (int) metadata[2];
    }

    /** Partition incarnation UUID. */
    public UUID incarnation() {
        return new UUID(metadata[3], metadata[4]);
    }

    /** Partition-global offset, represented as unsigned long bits. */
    public long offset() {
        return metadata[5];
    }

    /** Application event UUID. */
    public UUID messageId() {
        return new UUID(metadata[6], metadata[7]);
    }

    /** Read-only payload views with independent positions. These survive client closure. */
    public List<ByteBuffer> parts() {
        return Arrays.stream(payload).map(p -> ByteBuffer.wrap(p).asReadOnlyBuffer()).toList();
    }

    /** Read-only view of a single-part record, rejecting multipart records. */
    public ByteBuffer body() {
        if (payload.length != 1) throw new IllegalStateException("record is multipart");
        return ByteBuffer.wrap(payload[0]).asReadOnlyBuffer();
    }
}
