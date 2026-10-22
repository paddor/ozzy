package io.ozzy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Initial history selection. Subscription and latest-end selection resolve lazily when polled.
 *
 * @param start earliest retained history or latest confirmed ends
 * @param checkpoint saved delivered progress, or null; requires EARLIEST
 * @param partitions selected numeric partitions, or null for all
 */
public record ReaderOptions(Start start, Checkpoint checkpoint, List<Integer> partitions) {
    /** Initial position when no checkpoint is supplied. */
    public enum Start {
        /** First retained record in each selected partition. */
        EARLIEST,
        /** Current confirmed ends, followed by new records. */
        LATEST
    }

    /** Read all retained history. */
    public ReaderOptions() {
        this(Start.EARLIEST, null, null);
    }

    /**
     * Resume a checkpoint using its partition selection.
     *
     * @param checkpoint saved delivered progress
     */
    public ReaderOptions(Checkpoint checkpoint) {
        this(
                Start.EARLIEST,
                Objects.requireNonNull(checkpoint),
                List.copyOf(checkpoint.positions().keySet()));
    }

    /** Validate and snapshot filters. Rust checks topic incarnation and complete coverage. */
    public ReaderOptions {
        Objects.requireNonNull(start);
        if (checkpoint != null && start != Start.EARLIEST)
            throw new IllegalArgumentException("start and checkpoint are exclusive");
        if (partitions != null) {
            partitions = List.copyOf(partitions);
            if (partitions.isEmpty()
                    || partitions.stream().anyMatch(p -> p < 0)
                    || partitions.stream().distinct().count() != partitions.size())
                throw new IllegalArgumentException("invalid partition selection");
        }
    }

    long[] settings() {
        var values = new ArrayList<Long>();
        values.add(checkpoint != null ? 2L : start == Start.EARLIEST ? 0L : 1L);
        values.add(partitions == null ? -1L : (long) partitions.size());
        if (partitions != null) for (int p : partitions) values.add((long) p);
        if (checkpoint != null) {
            values.add(checkpoint.topicId().getMostSignificantBits());
            values.add(checkpoint.topicId().getLeastSignificantBits());
            checkpoint
                    .positions()
                    .forEach(
                            (p, o) -> {
                                values.add((long) p);
                                values.add(o);
                            });
        }
        return values.stream().mapToLong(Long::longValue).toArray();
    }
}
