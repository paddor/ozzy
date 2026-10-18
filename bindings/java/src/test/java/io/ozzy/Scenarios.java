package io.ozzy;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public final class Scenarios {
    private static ClientConfig config;
    private static boolean asynchronous;
    private static boolean quorum;

    private Scenarios() {}

    public static void run(String[] addresses, String codec, boolean async, String name)
            throws Exception {
        var brokers = new ArrayList<BrokerAddress>();
        for (int i = 0; i < addresses.length; i += 3)
            brokers.add(
                    new BrokerAddress(
                            UUID.fromString(addresses[i]), addresses[i + 1], addresses[i + 2]));
        config =
                new ClientConfig(brokers)
                        .withCompression(
                                ClientConfig.Compression.valueOf(
                                        codec.toUpperCase(java.util.Locale.ROOT)))
                        .withCapacity(new ClientConfig.Capacity(4, 3, 3, 1))
                        .withLimits(new ClientConfig.Limits(32768, 4096, 8, 32, 2048, 2048));
        asynchronous = async;
        quorum = brokers.size() == 3;
        try (var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            var task =
                    worker.submit(
                            () -> {
                                try (Client client =
                                        async
                                                ? await(Client.connectAsync(config))
                                                : Client.connect(config)) {
                                    switch (name) {
                                        case "roundtrip" -> roundtrip(client);
                                        case "payloads" -> payloads(client);
                                        case "routing" -> routing(client);
                                        case "checkpoints" -> checkpoints(client);
                                        case "identities" -> identities(client);
                                        case "validation" -> validation(client);
                                        case "lifetimes" -> lifetimes(client);
                                        case "mixed" -> mixed(client);
                                        case "cancellation" -> cancellation(client);
                                        case "pressure" -> pressure(client);
                                        case "observers" -> observers(client);
                                        case "callbacks" -> callbacks(client);
                                        case "cancel_race" -> cancelRace(client);
                                        case "completion_race" -> completionRace(client);
                                        case "close_cancel" -> closeCancel(client);
                                        default -> throw new AssertionError(name);
                                    }
                                } finally {
                                    Harness.holdWrites(false);
                                }
                            });
            try {
                task.get(9, TimeUnit.SECONDS);
            } finally {
                task.cancel(true);
            }
        }
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception error) {
            throw new AssertionError(error);
        }
    }

    private static void check(boolean condition) {
        if (!condition) throw new AssertionError();
    }

    private static byte[] body(int n) {
        byte[] bytes = new byte[16 + n % 1000];
        new java.util.Random(n).nextBytes(bytes);
        ByteBuffer.wrap(bytes).putInt(n);
        return bytes;
    }

    private static byte[] bytes(ByteBuffer view) {
        byte[] b = new byte[view.remaining()];
        view.duplicate().get(b);
        return b;
    }

    private static Producer producer(Client c) {
        return asynchronous ? await(c.producerAsync("orders")) : c.producer("orders");
    }

    private static Consumer consumer(Client c, ReaderOptions options) {
        return asynchronous
                ? await(c.consumerAsync("orders", options))
                : c.consumer("orders", options);
    }

    private static Consumer consumer(Client c) {
        return consumer(c, new ReaderOptions());
    }

    private static PendingRecord send(Producer p, byte[] body, SendOptions o) {
        return asynchronous ? await(p.sendAsync(body, o)) : p.send(body, o);
    }

    private static Receipt confirm(PendingRecord p) {
        return asynchronous ? await(p.confirmedAsync()) : p.confirmed();
    }

    private static Record next(Consumer c) {
        return asynchronous ? await(c.nextAsync()) : c.next();
    }

    private static void flush(Producer p) {
        if (asynchronous) await(p.flushAsync());
        else p.flush();
    }

    private static void until(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new AssertionError("no progress");
            Thread.yield();
        }
    }

    private static void rejects(Class<? extends Throwable> expected, Runnable action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (expected.isInstance(failure)) return;
            throw new AssertionError("unexpected failure", failure);
        }
        throw new AssertionError("operation unexpectedly succeeded");
    }

    private static void roundtrip(Client client) {
        try (Producer p = producer(client);
                Consumer c = consumer(client)) {
            var pending = new ArrayList<PendingRecord>();
            var receipts = new HashMap<UUID, Receipt>();
            for (int i = 0; i < 96; i++) {
                UUID id = new UUID(Long.MIN_VALUE | 1234, Long.MIN_VALUE | (i + 1L));
                pending.add(send(p, body(i), SendOptions.partition(i % 4).withId(id)));
            }
            flush(p);
            for (PendingRecord item : pending) {
                Receipt r = confirm(item);
                receipts.put(r.messageId(), r);
                check(r.topicId().equals(item.topicId()) && r.partition() == item.partition());
                check(r.sequence() == item.sequence());
                check(
                        r.policy()
                                == (quorum
                                        ? Receipt.Policy.QUORUM_DURABLE
                                        : Receipt.Policy.LOCAL_DURABLE));
                check(r.disposition() == Receipt.Disposition.STORED);
                check(r.producerId() != null && r.producerEpoch() >= 0 && r.ownerEpoch() >= 0);
            }
            long[] offsets = new long[4];
            var iterator = c.iterator();
            for (int i = 0; i < 96; i++) {
                check(iterator.hasNext());
                Record r = asynchronous ? next(c) : iterator.next();
                int number = r.body().getInt();
                check(
                        r.messageId()
                                .equals(
                                        new UUID(
                                                Long.MIN_VALUE | 1234,
                                                Long.MIN_VALUE | (number + 1L))));
                Receipt receipt = receipts.remove(r.messageId());
                check(
                        receipt != null
                                && receipt.offset() == r.offset()
                                && receipt.partition() == r.partition());
                check(r.offset() == offsets[r.partition()]++ && number % 4 == r.partition());
                check(r.incarnation().equals(receipt.incarnation()));
                check(Arrays.equals(bytes(r.body()), body(number)));
            }
            check(receipts.isEmpty());
            ConsumerStats s = c.stats();
            check(s.liveRecords() + s.replayedRecords() == 96);
        }
    }

    private static void payloads(Client client) {
        try (Producer p = producer(client);
                Consumer c =
                        consumer(
                                client,
                                new ReaderOptions(
                                        ReaderOptions.Start.EARLIEST, null, List.of(2)))) {
            byte[] maximum = new byte[4096];
            Arrays.fill(maximum, (byte) 7);
            var payloads =
                    List.of(
                            List.of(new byte[0]),
                            List.of(maximum),
                            List.of(new byte[] {1}, new byte[0], new byte[] {2, 3}),
                            java.util.Collections.nCopies(32, new byte[0]));
            for (var parts : payloads) {
                var input = parts.stream().map(ByteBuffer::wrap).toList();
                PendingRecord pending =
                        asynchronous
                                ? await(p.sendAsync(input, SendOptions.partition(2)))
                                : p.send(input, SendOptions.partition(2));
                confirm(pending);
                Record r = next(c);
                check(r.parts().size() == parts.size());
                for (int i = 0; i < parts.size(); i++) {
                    check(Arrays.equals(bytes(r.parts().get(i)), parts.get(i)));
                    check(r.parts().get(i).isReadOnly());
                }
            }
            ByteBuffer direct = ByteBuffer.allocateDirect(16).putLong(1).putLong(2).flip();
            direct.position(8);
            PendingRecord item =
                    asynchronous
                            ? await(
                                    p.sendAsync(
                                            direct.asReadOnlyBuffer(), SendOptions.partition(2)))
                            : p.send(direct.asReadOnlyBuffer(), SendOptions.partition(2));
            check(direct.position() == 8);
            confirm(item);
            check(next(c).body().getLong() == 2);
            ByteBuffer heap = ByteBuffer.wrap(new byte[] {9, 10, 11, 12});
            heap.position(1).limit(3);
            ByteBuffer slice = heap.slice().asReadOnlyBuffer();
            confirm(
                    asynchronous
                            ? await(p.sendAsync(slice, SendOptions.partition(2)))
                            : p.send(slice, SendOptions.partition(2)));
            check(slice.position() == 0 && heap.position() == 1);
            check(Arrays.equals(bytes(next(c).body()), new byte[] {10, 11}));
            byte[] source = {4, 5, 6};
            CompletableFuture<PendingRecord> snapshot =
                    p.sendAsync(source, SendOptions.partition(2));
            Arrays.fill(source, (byte) 0);
            confirm(await(snapshot));
            check(Arrays.equals(bytes(next(c).body()), new byte[] {4, 5, 6}));
        }
    }

    private static void routing(Client client) {
        try (Producer p = producer(client);
                Producer other = producer(client);
                Consumer c = consumer(client)) {
            check(p.partitionCount() == 4);
            rejects(OzzyException.class, () -> p.send(new byte[] {0}, SendOptions.partition(4)));
            Map<Integer, Long> offsets = new HashMap<>();
            int keyed = -1;
            for (int i = 0; i < 24; i++) {
                SendOptions route =
                        i < 12 ? SendOptions.key(new byte[] {9}) : SendOptions.partition(i % 4);
                Receipt r = confirm(send(i % 2 == 0 ? p : other, body(i), route));
                if (i < 12) {
                    if (keyed < 0) keyed = r.partition();
                    check(r.partition() == keyed);
                }
                long offset = offsets.getOrDefault(r.partition(), 0L);
                check(r.offset() == offset);
                offsets.put(r.partition(), offset + 1);
            }
            int[] counts = new int[4];
            for (int i = 0; i < 24; i++) {
                Record r = next(c);
                check(r.offset() == counts[r.partition()]++);
            }
        }
    }

    private static void checkpoints(Client client) {
        try (Producer p = producer(client)) {
            for (int i = 0; i < 5; i++) confirm(send(p, body(i), SendOptions.partition(0)));
            Checkpoint saved;
            ConsumerStats stats;
            Consumer c =
                    consumer(
                            client,
                            new ReaderOptions(ReaderOptions.Start.EARLIEST, null, List.of(0)));
            next(c);
            next(c);
            saved = c.checkpoint();
            stats = c.stats();
            c.close();
            check(c.checkpoint().equals(saved) && c.stats().equals(stats));
            rejects(ClosedException.class, c::tryNext);
            try (Consumer resumed = consumer(client, new ReaderOptions(saved))) {
                for (int i = 2; i < 5; i++) check(next(resumed).body().getInt() == i);
            }
            try (Consumer latest =
                    consumer(
                            client,
                            new ReaderOptions(ReaderOptions.Start.LATEST, null, List.of(0)))) {
                until(
                        () -> {
                            check(latest.tryNext().isEmpty());
                            return latest.checkpoint().positions().get(0) == 5;
                        });
                confirm(send(p, body(5), SendOptions.partition(0)));
                check(next(latest).offset() == 5);
            }
        }
    }

    private static void identities(Client client) {
        UUID id = UUID.randomUUID();
        byte[] identity;
        Receipt original;
        try (Producer p = producer(client);
                Producer redundant = producer(client)) {
            identity = p.identity();
            check(identity.length == 32);
            original = confirm(send(p, new byte[] {1}, SendOptions.partition(0).withId(id)));
            Receipt duplicate =
                    confirm(send(redundant, new byte[] {9}, SendOptions.partition(0).withId(id)));
            check(
                    duplicate.disposition() == Receipt.Disposition.DUPLICATE
                            && duplicate.offset() == original.offset());
        }
        try (Producer resumed = client.producer("orders", identity, false)) {
            Receipt r = confirm(send(resumed, new byte[] {2}, SendOptions.partition(0)));
            check(
                    r.producerId().equals(original.producerId())
                            && r.sequence() == 1
                            && r.offset() == 1);
        }
        try (Producer takeover = client.producer("orders", identity, true)) {
            Receipt r = confirm(send(takeover, new byte[] {3}, SendOptions.partition(0)));
            check(r.producerEpoch() > original.producerEpoch() && r.offset() == 2);
        }
    }

    private static void validation(Client client) {
        rejects(IllegalArgumentException.class, () -> new SendOptions(new byte[0], 0, null));
        rejects(IllegalArgumentException.class, () -> SendOptions.partition(-1));
        rejects(IllegalArgumentException.class, () -> SendOptions.AUTOMATIC.withId(new UUID(0, 0)));
        rejects(IllegalArgumentException.class, () -> new ClientConfig.Capacity(0, 1, 1, 1));
        rejects(IllegalArgumentException.class, () -> new ClientConfig.Capacity(1, 1, 1, 65));
        rejects(IllegalArgumentException.class, () -> new ClientConfig.Limits(0, 1, 1, 1, 1, 1));
        rejects(
                IllegalArgumentException.class,
                () -> new ReaderOptions(ReaderOptions.Start.EARLIEST, null, List.of(0, 0)));
        rejects(
                IllegalArgumentException.class,
                () -> client.producer("orders", new byte[1], false));
        rejects(IllegalArgumentException.class, () -> client.producer("orders", null, true));
        try (Producer p = producer(client)) {
            rejects(IllegalArgumentException.class, () -> p.send(new byte[4097]));
            rejects(IllegalArgumentException.class, () -> p.send(List.of(), SendOptions.AUTOMATIC));
            rejects(
                    IllegalArgumentException.class,
                    () ->
                            p.send(
                                    java.util.Collections.nCopies(33, ByteBuffer.allocate(0)),
                                    SendOptions.AUTOMATIC));
            check(confirm(send(p, body(0), SendOptions.partition(0))).sequence() == 0);
        }
    }

    private static void lifetimes(Client client) {
        Record record;
        PendingRecord pending;
        var retained = new ArrayList<PendingRecord>();
        try (Producer p = producer(client);
                Consumer c = consumer(client)) {
            pending = send(p, body(19), SendOptions.partition(1));
            confirm(pending);
            record = next(c);
            // Cross multiple cleanup groups, retaining sparse observations and the
            // final partially filled group. Unobserved admissions still flush.
            for (int i = 0; i < 257; i++) {
                PendingRecord item = send(p, body(i), SendOptions.partition(i % 4));
                if (i % 128 == 0) retained.add(item);
            }
            flush(p);
        }
        System.gc();
        for (PendingRecord item : retained) check(confirm(item).sequence() == item.sequence());
        Receipt receipt = confirm(pending);
        check(
                record.offset() == receipt.offset()
                        && record.messageId().equals(receipt.messageId()));
        rejects(ReadOnlyBufferException.class, () -> record.body().put((byte) 0));
        check(Arrays.equals(bytes(record.body()), body(19)));
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 16; i++)
                tasks.add(
                        threads.submit(
                                () -> {
                                    check(confirm(pending).offset() == receipt.offset());
                                    check(Arrays.equals(bytes(record.body()), body(19)));
                                }));
            for (var task : tasks) {
                try {
                    task.get();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    private static void mixed(Client client) {
        var otherConfig =
                config.withCompression(
                        config.compression() == ClientConfig.Compression.RAW
                                ? ClientConfig.Compression.LZ4
                                : ClientConfig.Compression.RAW);
        try (Client other = Client.connect(otherConfig);
                Producer a = producer(client);
                Producer b = producer(other);
                Consumer c = consumer(client)) {
            for (int i = 0; i < 48; i++)
                confirm(send(i % 2 == 0 ? a : b, body(i), SendOptions.partition(3)));
            for (int i = 0; i < 48; i++) {
                Record r = next(c);
                check(r.offset() == i && r.body().getInt() == i);
            }
        }
    }

    private static void cancellation(Client client) {
        try (Consumer c =
                        consumer(
                                client,
                                new ReaderOptions(ReaderOptions.Start.EARLIEST, null, List.of(0)));
                Producer p = producer(client)) {
            Checkpoint before = c.checkpoint();
            for (int i = 0; i < 32; i++) {
                CompletableFuture<Record> receive = c.nextAsync();
                check(!receive.isDone());
                rejects(BusyException.class, c::checkpoint);
                rejects(BusyException.class, c::stats);
                rejects(BusyException.class, c::tryNext);
                rejects(
                        UnsupportedOperationException.class,
                        () -> receive.completeAsync(() -> null));
                rejects(
                        UnsupportedOperationException.class,
                        () -> receive.completeAsync(() -> null, Runnable::run));
                check(receive.cancel(true) && receive.isCancelled());
                check(c.checkpoint().equals(before));
            }
            CompletableFuture<Record> forced = c.nextAsync();
            check(forced.completeExceptionally(new IllegalStateException("external completion")));
            check(c.checkpoint().equals(before));
            confirm(send(p, body(0), SendOptions.partition(0)));
            check(next(c).offset() == 0);
        }
    }

    private static void observers(Client client) {
        try (Producer p = producer(client)) {
            Harness.holdWrites(true);
            PendingRecord pending = send(p, body(0), SendOptions.partition(0));
            until(() -> Harness.heldWrites() > 0);
            var observers = new ArrayList<CompletableFuture<Receipt>>();
            for (int i = 0; i < 16; i++) observers.add(pending.confirmedAsync());
            for (int i = 0; i < 16; i += 2) check(observers.get(i).cancel(true));
            check(pending.tryConfirmed().isEmpty());
            CompletableFuture<Void> flush = p.flushAsync();
            check(flush.cancel(true));
            Harness.holdWrites(false);
            for (int i = 1; i < 16; i += 2) check(await(observers.get(i)).offset() == 0);
            check(confirm(pending).offset() == 0);
            flush(p);
        } finally {
            Harness.holdWrites(false);
        }
    }

    private static void pressure(Client client) {
        try (Producer p = producer(client)) {
            Harness.holdWrites(true);
            int accepted = 0;
            boolean blocked = false;
            for (int i = 0; i < 8192; i++) {
                CompletableFuture<PendingRecord> item =
                        p.sendAsync(new byte[4096], SendOptions.partition(0));
                if (!item.isDone() && item.cancel(true)) {
                    blocked = true;
                    break;
                }
                await(item);
                accepted++;
            }
            check(blocked);
            for (int i = 0; i < 8; i++)
                check(p.sendAsync(new byte[4096], SendOptions.partition(0)).cancel(true));
            Harness.holdWrites(false);
            flush(p);
            Receipt receipt = confirm(send(p, new byte[] {1}, SendOptions.partition(0)));
            check(receipt.sequence() == accepted && receipt.offset() == accepted);
        } finally {
            Harness.holdWrites(false);
        }
    }

    private static void callbacks(Client client) {
        try (Producer p = producer(client);
                Consumer c = consumer(client)) {
            PendingRecord first = send(p, body(0), SendOptions.partition(0));
            Receipt second =
                    await(
                            first.confirmedAsync()
                                    .thenCompose(
                                            ignored ->
                                                    p.sendAsync(body(1), SendOptions.partition(0)))
                                    .thenCompose(PendingRecord::confirmedAsync));
            check(second.offset() == 1);
            Record firstRead =
                    await(
                            c.nextAsync()
                                    .thenApply(
                                            value -> {
                                                check(c.checkpoint().positions().get(0) == 1);
                                                return value;
                                            }));
            check(firstRead.offset() == 0 && next(c).offset() == 1);
        }
    }

    private static void cancelRace(Client client) {
        try (Producer p = producer(client);
                Consumer c =
                        consumer(
                                client,
                                new ReaderOptions(ReaderOptions.Start.EARLIEST, null, List.of(0)));
                var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 64; i++) {
                CompletableFuture<Record> read = c.nextAsync();
                int n = i;
                var write =
                        worker.submit(() -> confirm(send(p, body(n), SendOptions.partition(0))));
                boolean cancelled = read.cancel(true);
                Record record = cancelled ? next(c) : await(read);
                try {
                    check(write.get().offset() == i);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                check(record.offset() == i && record.body().getInt() == i);
            }
        }
    }

    private static void closeCancel(Client client) {
        Producer p = producer(client);
        try {
            Harness.holdWrites(true);
            PendingRecord pending = send(p, body(0), SendOptions.partition(0));
            until(() -> Harness.heldWrites() > 0);
            CompletableFuture<Void> closing = p.closeAsync();
            check(!closing.isDone() && closing.cancel(true));
            rejects(ClosedException.class, () -> p.send(body(1)));
            Harness.holdWrites(false);
            p.close();
            p.close();
            try {
                pending.confirmed();
            } catch (OzzyException unknown) {
                check(unknown.getMessage().contains("unknown"));
            }
        } finally {
            Harness.holdWrites(false);
            p.close();
        }
        Consumer c = consumer(client);
        c.tryNext();
        CompletableFuture<Void> closing = c.closeAsync();
        closing.cancel(true);
        c.close();
        check(c.checkpoint() != null && c.stats() != null);
        rejects(ClosedException.class, c::next);
    }

    private static void completionRace(Client client) {
        try (Producer p = producer(client);
                Consumer c =
                        consumer(
                                client,
                                new ReaderOptions(
                                        ReaderOptions.Start.EARLIEST, null, List.of(0)))) {
            confirm(send(p, body(0), SendOptions.partition(0)));
            next(c);
            CompletableFuture<Record> read = c.nextAsync();
            var parked = new java.util.concurrent.CountDownLatch(1);
            var resume = new java.util.concurrent.CountDownLatch(1);
            var blocker =
                    java.util.concurrent.ForkJoinPool.commonPool()
                            .submit(
                                    () -> {
                                        parked.countDown();
                                        try {
                                            resume.await();
                                        } catch (InterruptedException error) {
                                            throw new AssertionError(error);
                                        }
                                    });
            try {
                try {
                    check(parked.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    throw new AssertionError(error);
                }
                PendingRecord pending = p.send(body(1), SendOptions.partition(0));
                until(
                        () -> {
                            try {
                                return c.checkpoint().positions().get(0) == 2;
                            } catch (BusyException busy) {
                                return false;
                            }
                        });
                check(!read.isDone() && !read.cancel(true));
                try (var worker = Executors.newVirtualThreadPerTaskExecutor()) {
                    var interrupted =
                            worker.submit(
                                    () -> {
                                        Thread.currentThread().interrupt();
                                        Record record = NativeFuture.await(read);
                                        check(Thread.currentThread().isInterrupted());
                                        return record;
                                    });
                    resume.countDown();
                    try {
                        check(interrupted.get(5, TimeUnit.SECONDS).offset() == 1);
                    } catch (Exception error) {
                        throw new AssertionError(error);
                    }
                }
                confirm(pending);
            } finally {
                resume.countDown();
            }
            blocker.join();
        }
    }
}
