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

    /** Open shared connections, reserving the configured finite roles and bounds. */
    public static Client connect(ClientConfig config) {
        return NativeFuture.await(connectAsync(config));
    }

    /**
     * Open without a Java worker thread polling the SDK operation. Cancellation releases native
     * state.
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
        try {
            future.install(Native.open(future, endpoints, ids, config.settings()));
        } catch (Throwable error) {
            future.released();
            future.completeExceptionally(error);
        }
        return future;
    }

    /** Open a new producer with a generated identity. */
    public Producer producer(String topic) {
        return producer(topic, null, false);
    }

    /**
     * Resume a saved opaque identity; takeover explicitly fences the old epoch and can partially
     * succeed.
     */
    public Producer producer(String topic, byte[] identity, boolean takeover) {
        return NativeFuture.await(producerAsync(topic, identity, takeover));
    }

    /** Open a new producer asynchronously. */
    public CompletableFuture<Producer> producerAsync(String topic) {
        return producerAsync(topic, null, false);
    }

    /** Open or resume asynchronously. A failed takeover may already have fenced some partitions. */
    public CompletableFuture<Producer> producerAsync(
            String topic, byte[] identity, boolean takeover) {
        Objects.requireNonNull(topic);
        if (identity != null && identity.length != 32)
            throw new IllegalArgumentException("identity must be 32 bytes");
        if (takeover && identity == null)
            throw new IllegalArgumentException("takeover requires identity");
        resource.enter();
        return NativeFuture.leased(
                resource,
                1,
                topic,
                new long[] {takeover ? 1 : 0},
                null,
                null,
                null,
                identity,
                config);
    }

    /** Read earliest retained history from all partitions. */
    public Consumer consumer(String topic) {
        return consumer(topic, new ReaderOptions());
    }

    /** Open with explicit history, checkpoint, and partition selection. */
    public Consumer consumer(String topic, ReaderOptions options) {
        return NativeFuture.await(consumerAsync(topic, options));
    }

    /** Open a default reader asynchronously. */
    public CompletableFuture<Consumer> consumerAsync(String topic) {
        return consumerAsync(topic, new ReaderOptions());
    }

    /** Open asynchronously. Subscription selection resolves when first polled. */
    public CompletableFuture<Consumer> consumerAsync(String topic, ReaderOptions options) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(options);
        long[] settings = options.settings();
        resource.enter();
        return NativeFuture.leased(resource, 2, topic, settings, null, null, null, null, config);
    }

    /** Shut down shared links and native runtime; idempotent. */
    @Override
    public void close() {
        NativeFuture.await(closeAsync());
    }

    /** Shut down asynchronously. Canceling a close leaves the handle sealed. */
    public CompletableFuture<Void> closeAsync() {
        return resource.closeAsync(null);
    }
}
