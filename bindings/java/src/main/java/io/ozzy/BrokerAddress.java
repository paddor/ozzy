package io.ozzy;

import java.util.Objects;
import java.util.UUID;

/**
 * Persistent broker identity and configured control/data endpoints.
 *
 * @param nodeId UUID from the broker identity file
 * @param peer control PEER endpoint
 * @param dataPeer record and replay PEER endpoint
 */
public record BrokerAddress(UUID nodeId, String peer, String dataPeer) {
    /** Validate nonzero identity and nonnull endpoints; Rust validates schemes. */
    public BrokerAddress {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(peer);
        Objects.requireNonNull(dataPeer);
        if (nodeId.getMostSignificantBits() == 0 && nodeId.getLeastSignificantBits() == 0)
            throw new IllegalArgumentException("broker identity must be nonzero");
    }
}
