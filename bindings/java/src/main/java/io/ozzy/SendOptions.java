package io.ozzy;

import java.util.Objects;
import java.util.UUID;

/** Immutable routing and optional application identity. Reuse options across records. */
public final class SendOptions {
    /** Rust SDK automatic routing and generated UUIDv7 identity. */
    public static final SendOptions AUTOMATIC = new SendOptions(null, null, null);

    final byte[] key;
    final Integer partition;
    final UUID id;

    /**
     * Choose at most one of key and partition. Reuse a nonzero ID for the same event after an
     * unknown outcome; broker deduplication is optional.
     *
     * @param key stable routing key, copied; or null
     * @param partition exact numeric partition, or null
     * @param id nonzero event UUID, or null for a generated UUIDv7
     */
    public SendOptions(byte[] key, Integer partition, UUID id) {
        if (key != null && partition != null)
            throw new IllegalArgumentException("key and partition are exclusive");
        if (partition != null && partition < 0)
            throw new IllegalArgumentException("negative partition");
        if (id != null && id.getMostSignificantBits() == 0 && id.getLeastSignificantBits() == 0)
            throw new IllegalArgumentException("message ID must be nonzero");
        this.key = key == null ? null : key.clone();
        this.partition = partition;
        this.id = id;
    }

    /**
     * Route to an exact numeric partition.
     *
     * @param partition numeric partition
     * @return options routing to that partition
     */
    public static SendOptions partition(int partition) {
        return new SendOptions(null, partition, null);
    }

    /**
     * Use Rust SDK stable key routing, snapshotting the key.
     *
     * @param key routing key
     * @return options routing by that key
     */
    public static SendOptions key(byte[] key) {
        return new SendOptions(Objects.requireNonNull(key), null, null);
    }

    /**
     * Return a copy with a caller-selected nonzero event UUID.
     *
     * @param id nonzero event UUID
     * @return the updated copy
     */
    public SendOptions withId(UUID id) {
        return new SendOptions(key, partition, Objects.requireNonNull(id));
    }

    int route() {
        return partition != null ? partition : key != null ? -2 : -1;
    }
}
