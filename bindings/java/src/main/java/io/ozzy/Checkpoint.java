package io.ozzy;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Delivered progress, not a commit to the broker or a transaction with application effects. Persist
 * only after processing succeeds. Retention can invalidate older offsets.
 *
 * @param topicId exact topic incarnation
 * @param positions numeric partition to first undelivered offset (unsigned long bits)
 */
public record Checkpoint(UUID topicId, Map<Integer, Long> positions) {
    /** Snapshot immutable positions; duplicate map keys cannot be represented. */
    public Checkpoint {
        Objects.requireNonNull(topicId);
        positions = Map.copyOf(positions);
        if (positions.keySet().stream().anyMatch(p -> p < 0))
            throw new IllegalArgumentException("negative partition");
    }
}
