package io.ozzy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** One topic writer. Admission is not durable confirmation. Use try-with-resources. */
public final class Producer implements AutoCloseable {
    final NativeResource resource;
    private final ClientConfig config;

    Producer(long pointer, ClientConfig config) {
        this.config = config;
        resource = new NativeResource(pointer, true);
    }

    /** Opaque 32-byte resume identity. Persist once, then resume after the old process stops. */
    public byte[] identity() {
        long pointer = resource.enter();
        try {
            return Native.identity(pointer);
        } finally {
            resource.exit();
        }
    }

    /** Number of topic partitions. */
    public int partitionCount() {
        long pointer = resource.enter();
        try {
            return (int) Native.inspect(pointer, 0)[0];
        } finally {
            resource.exit();
        }
    }

    /** Admit one buffer using automatic routing; waits for bounded capacity. */
    public PendingRecord send(byte[] body) {
        return send(body, SendOptions.AUTOMATIC);
    }

    /** Admit bytes with explicit routing and optional ID; input is snapshotted before return. */
    public PendingRecord send(byte[] body, SendOptions options) {
        return send(ByteBuffer.wrap(body), options);
    }

    /** Admit the buffer's remaining bytes using automatic routing. */
    public PendingRecord send(ByteBuffer body) {
        return send(body, SendOptions.AUTOMATIC);
    }

    /** Admit multipart data using automatic routing. */
    public PendingRecord send(List<ByteBuffer> parts) {
        return send(parts, SendOptions.AUTOMATIC);
    }

    /** Admit the buffer's remaining bytes without changing its position. */
    public PendingRecord send(ByteBuffer body, SendOptions options) {
        Object result = submit(body, options);
        if (result instanceof PendingRecord pending) return pending;
        @SuppressWarnings("unchecked")
        var future = (CompletableFuture<PendingRecord>) result;
        return NativeFuture.await(future);
    }

    /** Admit nonempty multipart data, preserving empty parts. */
    public PendingRecord send(List<ByteBuffer> parts, SendOptions options) {
        return NativeFuture.await(sendAsync(parts, options));
    }

    /** Asynchronous automatic-route admission. Ready calls stay inline. */
    public CompletableFuture<PendingRecord> sendAsync(byte[] body) {
        return sendAsync(body, SendOptions.AUTOMATIC);
    }

    /** Asynchronous byte admission. Cancel before admission assigns no sequence. */
    public CompletableFuture<PendingRecord> sendAsync(byte[] body, SendOptions options) {
        return sendAsync(ByteBuffer.wrap(body), options);
    }

    /** Admit remaining buffer bytes asynchronously using automatic routing. */
    public CompletableFuture<PendingRecord> sendAsync(ByteBuffer body) {
        return sendAsync(body, SendOptions.AUTOMATIC);
    }

    /** Admit multipart data asynchronously using automatic routing. */
    public CompletableFuture<PendingRecord> sendAsync(List<ByteBuffer> parts) {
        return sendAsync(parts, SendOptions.AUTOMATIC);
    }

    /** Asynchronous admission of remaining buffer bytes, preserving position. */
    public CompletableFuture<PendingRecord> sendAsync(ByteBuffer body, SendOptions options) {
        Object result = submit(body, options);
        if (result instanceof PendingRecord pending)
            return CompletableFuture.completedFuture(pending);
        @SuppressWarnings("unchecked")
        var future = (CompletableFuture<PendingRecord>) result;
        return future;
    }

    private Object submit(ByteBuffer body, SendOptions options) {
        Objects.requireNonNull(body);
        Objects.requireNonNull(options);
        if (body.remaining() > config.limits().recordBytes())
            throw new IllegalArgumentException("record exceeds client limits");
        resource.enter();
        try {
            PendingRecord pending = Fast.send(resource, body, options);
            if (pending != null) {
                resource.exit();
                return pending;
            }
        } catch (Throwable error) {
            resource.exit();
            throw error;
        }
        byte[][] parts;
        try {
            parts = new byte[][] {copy(body)};
        } catch (Throwable error) {
            resource.exit();
            throw error;
        }
        return NativeFuture.leased(
                resource,
                3,
                null,
                new long[] {options.route()},
                parts,
                options.key,
                ids(options),
                null,
                null);
    }

    /** Snapshot and admit multipart data asynchronously through the same Rust SDK path. */
    public CompletableFuture<PendingRecord> sendAsync(List<ByteBuffer> parts, SendOptions options) {
        Objects.requireNonNull(parts);
        Objects.requireNonNull(options);
        if (parts.isEmpty() || parts.size() > config.limits().parts())
            throw new IllegalArgumentException("invalid part count");
        long total = 0;
        for (ByteBuffer part : parts) total += Objects.requireNonNull(part).remaining();
        if (total > config.limits().recordBytes())
            throw new IllegalArgumentException("record exceeds client limits");
        byte[][] data = parts.stream().map(Producer::copy).toArray(byte[][]::new);
        resource.enter();
        return NativeFuture.leased(
                resource,
                3,
                null,
                new long[] {options.route()},
                data,
                options.key,
                ids(options),
                null,
                null);
    }

    private static byte[] copy(ByteBuffer body) {
        byte[] copy = new byte[body.remaining()];
        body.duplicate().get(copy);
        return copy;
    }

    private static long[] ids(SendOptions options) {
        return options.id == null
                ? null
                : new long[] {
                    options.id.getMostSignificantBits(), options.id.getLeastSignificantBits()
                };
    }

    /** Wait for all admitted records to receive durable confirmation. */
    public void flush() {
        NativeFuture.await(flushAsync());
    }

    /** Wait asynchronously; canceling this observation leaves the writer usable. */
    public CompletableFuture<Void> flushAsync() {
        resource.enter();
        return NativeFuture.leased(resource, 4, null, null, null, null, null, null, null);
    }

    /** Seal admission and drain admitted records; idempotent. */
    @Override
    public void close() {
        NativeFuture.await(closeAsync());
    }

    /** Seal and drain asynchronously. Cancellation leaves the writer sealed. */
    public CompletableFuture<Void> closeAsync() {
        return resource.closeAsync(null);
    }
}
