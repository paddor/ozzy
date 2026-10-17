package io.ozzy;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Finite per-client Rust SDK reservations. Transport compression is selected by endpoint scheme.
 *
 * @param brokers one broker or all three quorum brokers
 * @param limits outbound and independent inbound bounds
 * @param capacity client role and reader-queue reservations
 * @param requestTimeout positive request timeout
 * @param compression adaptive LZ4 or raw APPEND payloads
 */
public record ClientConfig(
        List<BrokerAddress> brokers,
        Limits limits,
        Capacity capacity,
        Duration requestTimeout,
        Compression compression) {
    /** Payload packing; consumers automatically decode both encodings. */
    public enum Compression {
        /** Adaptive whole-APPEND LZ4, falling back to raw when smaller. */
        LZ4,
        /** Uncompressed APPEND payloads. */
        RAW
    }

    /**
     * Checked record/frame bounds. Sizes count uncompressed record bytes.
     *
     * @param packetBytes maximum envelope payload bytes
     * @param recordBytes maximum bytes in one record
     * @param records maximum producer batch records
     * @param parts maximum producer batch parts
     * @param readerRecords maximum records in an inbound frame
     * @param readerParts maximum parts in an inbound frame
     */
    public record Limits(
            int packetBytes,
            int recordBytes,
            int records,
            int parts,
            int readerRecords,
            int readerParts) {
        /** Use 1 MiB packets, 64 KiB records, 64/256 outbound and 2048/2048 inbound bounds. */
        public Limits() {
            this(1048576, 65536, 64, 256, 2048, 2048);
        }

        /** Reject zero or negative bounds before opening native state. */
        public Limits {
            if (packetBytes <= 0
                    || recordBytes <= 0
                    || records <= 0
                    || parts <= 0
                    || readerRecords <= 0
                    || readerParts <= 0)
                throw new IllegalArgumentException("limits must be positive");
        }
    }

    /**
     * Capacity shared across this client's handles.
     *
     * @param partitions maximum topic partitions
     * @param producers reserved producer handles
     * @param consumers reserved consumer handles
     * @param readerQueueMessages OMQ receive window, 1 through 64
     */
    public record Capacity(int partitions, int producers, int consumers, int readerQueueMessages) {
        /** Reserve 16 partitions, one producer, one consumer, and a 32-frame queue. */
        public Capacity() {
            this(16, 1, 1, 32);
        }

        /** Validate positive roles and a bounded publication window. */
        public Capacity {
            if (partitions <= 0
                    || producers <= 0
                    || consumers <= 0
                    || readerQueueMessages < 1
                    || readerQueueMessages > 64)
                throw new IllegalArgumentException("invalid client capacity");
        }
    }

    /** Construct with default finite limits and adaptive LZ4. */
    public ClientConfig(List<BrokerAddress> brokers) {
        this(brokers, new Limits(), new Capacity(), Duration.ofSeconds(5), Compression.LZ4);
    }

    /** Snapshot addresses and validate local fields; Rust validates aggregate reservations. */
    public ClientConfig {
        brokers = List.copyOf(brokers);
        Objects.requireNonNull(limits);
        Objects.requireNonNull(capacity);
        Objects.requireNonNull(requestTimeout);
        Objects.requireNonNull(compression);
        if (brokers.size() != 1 && brokers.size() != 3)
            throw new IllegalArgumentException("supply one or three brokers");
        if (brokers.stream().map(BrokerAddress::nodeId).distinct().count() != brokers.size())
            throw new IllegalArgumentException("duplicate broker identity");
        if (requestTimeout.isNegative() || requestTimeout.isZero())
            throw new IllegalArgumentException("request timeout must be positive");
        requestTimeout.toNanos();
    }

    /** Return a copy selecting payload compression. */
    public ClientConfig withCompression(Compression value) {
        return new ClientConfig(brokers, limits, capacity, requestTimeout, value);
    }

    /** Return a copy with new record/frame bounds. */
    public ClientConfig withLimits(Limits value) {
        return new ClientConfig(brokers, value, capacity, requestTimeout, compression);
    }

    /** Return a copy with new role and queue capacity. */
    public ClientConfig withCapacity(Capacity value) {
        return new ClientConfig(brokers, limits, value, requestTimeout, compression);
    }

    /** Return a copy with a positive request timeout. */
    public ClientConfig withRequestTimeout(Duration value) {
        return new ClientConfig(brokers, limits, capacity, value, compression);
    }

    long[] settings() {
        return new long[] {
            limits.packetBytes,
            limits.recordBytes,
            limits.records,
            limits.parts,
            limits.readerRecords,
            limits.readerParts,
            capacity.partitions,
            capacity.producers,
            capacity.consumers,
            capacity.readerQueueMessages,
            requestTimeout.toNanos(),
            compression == Compression.LZ4 ? 1 : 0
        };
    }
}
