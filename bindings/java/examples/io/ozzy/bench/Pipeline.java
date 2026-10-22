package io.ozzy.bench;

import io.ozzy.BrokerAddress;
import io.ozzy.Client;
import io.ozzy.ClientConfig;
import io.ozzy.Consumer;
import io.ozzy.ConsumerStats;
import io.ozzy.PendingRecord;
import io.ozzy.Producer;
import io.ozzy.Record;
import io.ozzy.SendOptions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/** One benchmark application role. The Rust runner owns brokers, barriers, and credit. */
public final class Pipeline {
    private static final BufferedReader INPUT =
            new BufferedReader(new InputStreamReader(System.in));
    private static final Pattern DEADLINE = Pattern.compile("\"deadline\":(\\d+)");
    private static final com.sun.management.OperatingSystemMXBean CPU =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final boolean producer, async, each;
    private final int identity, producers, size, window;
    private final ClientConfig config;
    private final byte[][] bodies = new byte[256][];
    private final ByteBuffer[] bodyViews = new ByteBuffer[256];
    private final SendOptions[] routes = new SendOptions[4];
    private final long[] positions = new long[4];
    private final long[][] sequences;
    private long sequence;

    private Pipeline(String[] args) {
        producer = args[0].equals("producer");
        identity = Integer.parseInt(args[1]);
        producers = Integer.parseInt(args[2]);
        async = args[3].equals("async");
        size = Integer.parseInt(args[5]);
        each = args[6].equals("each");
        int queue = Integer.parseInt(args[7]);
        window = Integer.parseInt(args[8]);
        var brokers = new ArrayList<BrokerAddress>();
        for (int i = 9; i < args.length; i += 3)
            brokers.add(new BrokerAddress(UUID.fromString(args[i]), args[i + 1], args[i + 2]));
        config =
                new ClientConfig(brokers)
                        .withCompression(
                                ClientConfig.Compression.valueOf(args[4].toUpperCase(Locale.ROOT)))
                        .withCapacity(new ClientConfig.Capacity(4, 1, 1, queue));
        sequences = new long[producers][4];
        for (int i = 0; i < 4; i++) {
            routes[i] = SendOptions.partition(i);
            for (int p = 0; p < producers; p++) sequences[p][i] = i;
        }
        for (int i = 0; i < 256; i++) {
            byte[] text =
                    ("{\"customer\": "
                                    + i
                                    + ", \"event\": \"purchase\", \"items\": ["
                                    + i
                                    + ", "
                                    + (i + 1)
                                    + "]}")
                            .getBytes(StandardCharsets.UTF_8);
            bodies[i] = new byte[size - 16];
            for (int j = 0; j < bodies[i].length; j++) bodies[i][j] = text[j % text.length];
            bodyViews[i] = ByteBuffer.wrap(bodies[i]).asReadOnlyBuffer();
        }
    }

    private static void check(boolean valid) {
        if (!valid) throw new AssertionError("benchmark verification failed");
    }

    private static String command(String expected) throws Exception {
        String line = INPUT.readLine();
        check(line != null && line.contains("\"command\":\"" + expected + "\""));
        return line;
    }

    private static long deadline() throws Exception {
        var match = DEADLINE.matcher(command("run"));
        check(match.find());
        Instant now = Instant.now();
        long wall = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        return System.nanoTime() + Long.parseLong(match.group(1)) - wall;
    }

    private static void report(String json) {
        System.out.println(json);
        System.out.flush();
    }

    private byte[] payload(long n, int p, int phase) {
        byte[] result = new byte[size];
        ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(n).putInt(p).putInt(phase);
        byte[] body = bodies[(int) ((n * producers + p) & 255)];
        System.arraycopy(body, 0, result, 16, body.length);
        return result;
    }

    private long produce(Producer writer, int phase, long stop) throws Exception {
        long start = sequence;
        PendingRecord[] pending = each ? new PendingRecord[256] : null;
        while (System.nanoTime() < stop) {
            for (int i = 0; i < 256; i++) {
                byte[] body = payload(sequence, identity, phase);
                SendOptions route = routes[(int) (sequence & 3)];
                PendingRecord record =
                        async ? writer.sendAsync(body, route).join() : writer.send(body, route);
                if (each) pending[i] = record;
                sequence++;
            }
            if (each)
                for (PendingRecord record : pending) {
                    if (async) record.confirmedAsync().join();
                    else record.confirmed();
                }
            if ((sequence - start) % window == 0) command("credit");
        }
        for (SendOptions route : routes) {
            byte[] body = payload(sequence, identity, phase + 2);
            if (async) writer.sendAsync(body, route).join();
            else writer.send(body, route);
        }
        if (async) writer.flushAsync().join();
        else writer.flush();
        return sequence - start;
    }

    private long consume(Consumer reader, int phase) {
        boolean[][] ended = new boolean[producers][4];
        long[] credits = new long[producers];
        int markers = 0;
        long count = 0;
        while (markers < producers * 4) {
            Record record = async ? reader.nextAsync().join() : reader.next();
            ByteBuffer body = record.body().order(ByteOrder.LITTLE_ENDIAN);
            check(body.remaining() == size);
            long n = body.getLong();
            int p = body.getInt(), foundPhase = body.getInt(), partition = record.partition();
            check(p >= 0 && p < producers && partition >= 0 && partition < 4);
            check(record.offset() == positions[partition]++);
            check(body.mismatch(bodyViews[(int) ((n * producers + p) & 255)]) == -1);
            if (foundPhase == phase + 2) {
                check(!ended[p][partition] && n == sequences[p][partition] - partition);
                ended[p][partition] = true;
                markers++;
            } else {
                check(foundPhase == phase && partition == (n & 3) && n == sequences[p][partition]);
                sequences[p][partition] += 4;
                count++;
                if (++credits[p] % window == 0)
                    report("{\"event\":\"credit\",\"producer\":" + p + "}");
            }
        }
        return count;
    }

    private void done(
            int phase, long records, long cpu, ConsumerStats before, ConsumerStats after) {
        String stats = "";
        if (before != null) {
            long live = after.liveRecords() - before.liveRecords();
            long replay = after.replayedRecords() - before.replayedRecords();
            long gaps = after.liveGaps() - before.liveGaps();
            check(live + replay == records + producers * 4L);
            stats =
                    ",\"reader\":{\"live_records\":"
                            + live
                            + ",\"replayed_records\":"
                            + replay
                            + ",\"live_gaps\":"
                            + gaps
                            + "}";
        }
        report(
                "{\"event\":\"done\",\"phase\":"
                        + phase
                        + ",\"records\":"
                        + records
                        + ",\"cpu_seconds\":"
                        + (CPU.getProcessCpuTime() - cpu) / 1e9
                        + stats
                        + "}");
    }

    private void run() throws Exception {
        try (Client client = async ? Client.connectAsync(config).join() : Client.connect(config)) {
            if (producer) {
                try (Producer writer =
                        async ? client.producerAsync("orders").join() : client.producer("orders")) {
                    report("{\"event\":\"ready\"}");
                    for (int phase = 0; phase < 2; phase++) {
                        long stop = deadline(), cpu = CPU.getProcessCpuTime();
                        long records = produce(writer, phase, stop);
                        done(phase, records, cpu, null, null);
                    }
                    command("finish");
                }
            } else {
                try (Consumer reader =
                        async ? client.consumerAsync("orders").join() : client.consumer("orders")) {
                    report("{\"event\":\"ready\"}");
                    for (int phase = 0; phase < 2; phase++) {
                        deadline();
                        ConsumerStats before = reader.stats();
                        long cpu = CPU.getProcessCpuTime();
                        long records = consume(reader, phase);
                        done(phase, records, cpu, before, reader.stats());
                    }
                    command("finish");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Pipeline(args).run();
        Path sdk =
                Path.of(Client.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        String jarHash = HexFormat.of().formatHex(digest.digest(Files.readAllBytes(sdk)));
        String library = System.getProperty("ozzy.native.path");
        String nativeHash =
                library == null
                        ? jarHash
                        : HexFormat.of()
                                .formatHex(digest.digest(Files.readAllBytes(Path.of(library))));
        byte[] workload;
        try (var stream = Pipeline.class.getResourceAsStream("Pipeline.class")) {
            workload = stream.readAllBytes();
        }
        String hash = HexFormat.of().formatHex(digest.digest(workload));
        report(
                "{\"event\":\"finished\",\"java\":\""
                        + System.getProperty("java.runtime.version")
                        + "\",\"sdk_version\":\""
                        + Client.class.getPackage().getImplementationVersion()
                        + "\",\"sdk_jar_sha256\":\""
                        + jarHash
                        + "\",\"native_sha256\":\""
                        + nativeHash
                        + "\",\"workload_sha256\":\""
                        + hash
                        + "\"}");
    }
}
