package io.ozzy;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Shared Rust broker links. Close child handles first, then this client. Blocking methods park Java
 * and virtual threads while native SDK futures wait.
 */
public final class Client implements AutoCloseable {
    final NativeResource resource;
    private final ClientConfig config;

    Client(long pointer, ClientConfig config) {
        this.config = config;
        resource = new NativeResource(pointer);
    }

    /**
     * Open shared connections, reserving the configured finite roles and bounds.
     *
     * @param config brokers, bounds, and reservations
     * @return the connected client
     */
    public static Client connect(ClientConfig config) {
        return NativeFuture.await(connectAsync(config));
    }

    /**
     * Open without a Java worker thread polling the SDK operation. Cancellation releases native
     * state.
     *
     * @param config brokers, bounds, and reservations
     * @return a future that completes with the connected client
     */
    public static CompletableFuture<Client> connectAsync(ClientConfig config) {
        Objects.requireNonNull(config);
        var future = new NativeFuture<Client>(null, config, false, null);
        String[] endpoints = new String[config.brokers().size() * 2];
        long[] ids = new long[endpoints.length];
        for (int i = 0; i < config.brokers().size(); i++) {
            BrokerAddress b = config.brokers().get(i);
            endpoints[2 * i] = b.peer();
            endpoints[2 * i + 1] = b.dataPeer();
            ids[2 * i] = b.nodeId().getMostSignificantBits();
            ids[2 * i + 1] = b.nodeId().getLeastSignificantBits();
        }
        future.open(endpoints, ids, config.settings());
        return future;
    }

    /**
     * Open a new producer with a generated identity.
     *
     * @param topic topic name
     * @return a producer with a new identity
     */
    public Producer producer(String topic) {
        return producer(topic, null, false);
    }

    /**
     * Resume a saved opaque identity; takeover explicitly fences the old epoch and can partially
     * succeed.
     *
     * @param topic topic name
     * @param identity saved 32-byte identity from {@link Producer#identity()}, or null for a new
     *     one
     * @param takeover fence the identity's current epoch; requires an identity
     * @return the opened producer
     */
    public Producer producer(String topic, byte[] identity, boolean takeover) {
        return NativeFuture.await(producerAsync(topic, identity, takeover));
    }

    /**
     * Open a new producer asynchronously.
     *
     * @param topic topic name
     * @return a future that completes with a producer with a new identity
     */
    public CompletableFuture<Producer> producerAsync(String topic) {
        return producerAsync(topic, null, false);
    }

    /**
     * Open or resume asynchronously. A failed takeover may already have fenced some partitions.
     *
     * @param topic topic name
     * @param identity saved 32-byte identity from {@link Producer#identity()}, or null for a new
     *     one
     * @param takeover fence the identity's current epoch; requires an identity
     * @return a future that completes with the opened producer
     */
    public CompletableFuture<Producer> producerAsync(
            String topic, byte[] identity, boolean takeover) {
        Objects.requireNonNull(topic);
        if (identity != null && identity.length != 32)
            throw new IllegalArgumentException("identity must be 32 bytes");
        if (takeover && identity == null)
            throw new IllegalArgumentException("takeover requires identity");
        return open(1, topic, new long[] {takeover ? 1 : 0}, identity);
    }

    /**
     * Read earliest retained history from all partitions.
     *
     * @param topic topic name
     * @return a reader of all partitions from the earliest retained record
     */
    public Consumer consumer(String topic) {
        return consumer(topic, new ReaderOptions());
    }

    /**
     * Open with explicit history, checkpoint, and partition selection.
     *
     * @param topic topic name
     * @param options history start, checkpoint, and partition selection
     * @return the opened consumer
     */
    public Consumer consumer(String topic, ReaderOptions options) {
        return NativeFuture.await(consumerAsync(topic, options));
    }

    /**
     * Open a default reader asynchronously.
     *
     * @param topic topic name
     * @return a future that completes with a reader of all partitions from the earliest retained
     *     record
     */
    public CompletableFuture<Consumer> consumerAsync(String topic) {
        return consumerAsync(topic, new ReaderOptions());
    }

    /**
     * Open asynchronously. Subscription selection resolves when first polled.
     *
     * @param topic topic name
     * @param options history start, checkpoint, and partition selection
     * @return a future that completes with the opened consumer
     */
    public CompletableFuture<Consumer> consumerAsync(String topic, ReaderOptions options) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(options);
        return open(2, topic, options.settings(), null);
    }

    // An open clones the client's shared links during the native call, so the
    // client's lease ends on return and other opens may run concurrently.
    private <T> CompletableFuture<T> open(int op, String topic, long[] options, byte[] identity) {
        long pointer = resource.enter();
        try {
            var future = new NativeFuture<T>(null, config, false, null);
            future.start(pointer, op, topic, options, null, null, null, identity);
            return future;
        } finally {
            resource.exit();
        }
    }

    /** Shut down shared links and native runtime; idempotent. */
    @Override
    public void close() {
        NativeFuture.await(closeAsync());
    }

    /**
     * Shut down asynchronously. Canceling a close leaves the handle sealed.
     *
     * @return a future that completes once shutdown finishes
     */
    public CompletableFuture<Void> closeAsync() {
        return resource.closeAsync(null);
    }
}
