package io.ozzy.smoke;

import io.ozzy.BrokerAddress;
import io.ozzy.Client;
import io.ozzy.ClientConfig;
import io.ozzy.PendingRecord;
import io.ozzy.ReaderOptions;
import io.ozzy.Receipt;
import io.ozzy.SendOptions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class Smoke {
    private Smoke() {}

    private static void check(boolean value) {
        if (!value) throw new AssertionError();
    }

    // Explicit close checks final snapshots and idempotence before scope exit.
    @SuppressWarnings("try")
    public static void main(String[] args) throws Exception {
        boolean halt = args[0].equals("halt");
        var brokers = new ArrayList<BrokerAddress>();
        for (int i = 2; i < args.length; i += 3)
            brokers.add(new BrokerAddress(UUID.fromString(args[i]), args[i + 1], args[i + 2]));
        ClientConfig config =
                new ClientConfig(brokers)
                        .withCompression(
                                ClientConfig.Compression.valueOf(
                                        args[1].toUpperCase(java.util.Locale.ROOT)));
        try (Client client = Client.connect(config);
                var p = client.producer("orders")) {
            if (halt) {
                p.send(new byte[] {99}, SendOptions.partition(0)).confirmed();
                Runtime.getRuntime().halt(0);
            }
            byte[] identity = p.identity();
            try (var consumer = client.consumer("orders")) {
                long[] offsets = new long[4];
                boolean[] seen = new boolean[512];
                for (int round = 0; round < 8; round++) {
                    var pending = new ArrayList<PendingRecord>();
                    for (int i = 0; i < 64; i++) {
                        int number = round * 64 + i;
                        byte[] body = new byte[16 + number % 3000];
                        Arrays.fill(body, (byte) number);
                        ByteBuffer.wrap(body).putInt(number);
                        SendOptions options =
                                SendOptions.partition(number % 4)
                                        .withId(new UUID(123, number + 1L));
                        pending.add(
                                round % 2 == 0
                                        ? p.send(body, options)
                                        : p.sendAsync(body, options).get(10, TimeUnit.SECONDS));
                    }
                    p.flush();
                    for (var item : pending) {
                        Receipt receipt = item.confirmedAsync().get(10, TimeUnit.SECONDS);
                        check(
                                receipt.policy()
                                        == (brokers.size() == 1
                                                ? Receipt.Policy.LOCAL_DURABLE
                                                : Receipt.Policy.QUORUM_DURABLE));
                        check(receipt.sequence() == item.sequence());
                    }
                    for (int i = 0; i < 64; i++) {
                        var record =
                                round % 2 == 0
                                        ? consumer.next()
                                        : consumer.nextAsync().get(10, TimeUnit.SECONDS);
                        int number = record.body().getInt();
                        check(number >= round * 64 && number < (round + 1) * 64 && !seen[number]);
                        seen[number] = true;
                        check(
                                record.partition() == number % 4
                                        && record.offset() == offsets[record.partition()]++);
                        check(record.messageId().equals(new UUID(123, number + 1L)));
                        byte[] expected = new byte[16 + number % 3000];
                        Arrays.fill(expected, (byte) number);
                        ByteBuffer.wrap(expected).putInt(number);
                        byte[] found = new byte[record.body().remaining()];
                        record.body().get(found);
                        check(Arrays.equals(expected, found));
                    }
                }
                p.send(
                                List.of(
                                        ByteBuffer.wrap(new byte[] {1}),
                                        ByteBuffer.allocate(0),
                                        ByteBuffer.wrap(new byte[] {2})),
                                SendOptions.partition(2))
                        .confirmed();
                check(consumer.next().parts().size() == 3);
                offsets[2]++;
                var childArgs = new ArrayList<String>();
                childArgs.add(
                        java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java")
                                .toString());
                childArgs.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
                String modulePath = System.getProperty("jdk.module.path");
                if (modulePath != null) {
                    childArgs.addAll(
                            List.of(
                                    "--module-path",
                                    modulePath,
                                    "--add-modules",
                                    "io.ozzy",
                                    "--enable-native-access=io.ozzy"));
                }
                childArgs.addAll(
                        List.of(
                                "--enable-native-access=ALL-UNNAMED",
                                "-Xmx128m",
                                "-XX:+UseSerialGC",
                                "-XX:ActiveProcessorCount=2",
                                "-cp",
                                System.getProperty("java.class.path"),
                                Smoke.class.getName()));
                childArgs.addAll(Arrays.asList(args));
                childArgs.set(childArgs.size() - args.length, "halt");
                Process child = new ProcessBuilder(childArgs).inheritIO().start();
                try {
                    check(child.waitFor(15, TimeUnit.SECONDS) && child.exitValue() == 0);
                } finally {
                    child.destroyForcibly().waitFor();
                }
                var afterCrash = consumer.next();
                check(
                        afterCrash.partition() == 0
                                && afterCrash.offset() == offsets[0]++
                                && afterCrash.body().get() == 99);
                var checkpoint = consumer.checkpoint();
                var stats = consumer.stats();
                consumer.close();
                check(consumer.checkpoint().equals(checkpoint) && consumer.stats().equals(stats));
                p.close();
                try (var resumed = client.producer("orders", identity, false);
                        var reader = client.consumer("orders", new ReaderOptions(checkpoint))) {
                    var receipt =
                            resumed.send(new byte[] {42}, SendOptions.partition(1)).confirmed();
                    check(receipt.sequence() == 128 && reader.next().offset() == offsets[1]);
                }
            }
        }
    }
}
