# Ozzy Java SDK

Java 22+ blocking and asynchronous producers and consumers over Ozzy's Rust SDK.
JNI handles lifecycle and async setup; FFM handles ready scalar operations.
There is no OMQ.java dependency or Java implementation of the wire protocol,
routing, retries, compression, or batching.

## Build and run

Requires Linux, JDK 22+, Rust 1.93+, Maven, and cargo-nextest. Build and check:

```sh
export OZZY_ARTIFACT_ROOT=/path/to/artifacts
source scripts/ozzy_tools.sh
scripts/check-java.sh
# OZZY_JAVA_RELEASE=1 scripts/check-java.sh
```

Use `OZZY_JAVA_RELEASE=1` for optimized native libraries.

Artifacts are under `$CARGO_TARGET_DIR/java`: a Java-only JAR, a platform JAR
such as `ozzy-java-0.3.0-linux-x86_64.jar`, and `javadoc/`. The platform JAR
contains the native library and loads it automatically. Packages are built
locally; no Maven publication is automated. JPMS module: `io.ozzy`.

Enable native access when running:

```sh
java --enable-native-access=ALL-UNNAMED -cp ozzy-java-0.3.0-linux-x86_64.jar:app.jar MyApp
# On the module path: --enable-native-access=io.ozzy
```

The Java-only JAR can load an explicit library with
`-Dozzy.native.path=/absolute/path/libozzy_java.so` or `java.library.path`.
All public types and methods have Javadoc.

## Produce and consume

Supply persistent broker UUIDs and matching control/data endpoints: one broker
or all three quorum brokers. Provision topics on the broker first.

```java
import io.ozzy.BrokerAddress;
import io.ozzy.Client;
import io.ozzy.ClientConfig;
import io.ozzy.SendOptions;
import java.util.List;
import java.util.UUID;

var config = new ClientConfig(List.of(new BrokerAddress(
    UUID.fromString("11111111-1111-1111-1111-111111111111"),
    "tcp://127.0.0.1:7000", "tcp://127.0.0.1:7001")));

try (var client = Client.connect(config);
     var producer = client.producer("orders");
     var consumer = client.consumer("orders")) {
    var pending = producer.send(new byte[]{1, 2, 3}, SendOptions.partition(0));
    var receipt = pending.confirmed();
    var record = consumer.next();
    System.out.println(receipt.offset() + " " + record.messageId());
    var checkpoint = consumer.checkpoint();
    // Persist the checkpoint after application processing succeeds.
}
```

`send` accepts `byte[]`, the remaining bytes of a `ByteBuffer`, or a nonempty
`List<ByteBuffer>` for multipart records. Empty parts are preserved. Inputs are
copied before admission completes; positions are unchanged. Records expose
read-only `body()` and `parts()` views with independent positions and survive
handle closure. `body()` requires a single part.

Use `SendOptions.partition(n)`, `SendOptions.key(bytes)`, or `AUTOMATIC`.
Keys and partitions are exclusive. `withId(UUID)` sets a nonzero application
identity; omission generates a Rust UUIDv7. UUID objects are created only on
access; no UUID strings are formatted per record.

Admission does not prove durability. `PendingRecord.confirmed()` waits;
`tryConfirmed()` returns an Optional. Receipts carry partition-global offsets,
producer identity/epoch/sequence, disposition, and durability policy.
Keep several records in flight before confirming. `flush()` confirms the
admitted prefix without holding the producer, so sends may continue while it
waits; producer close seals and drains it. Single-broker durable
mode has no failover; disk quorum requires two of three eligible copies.

Save `producer.identity()` once and resume with
`client.producer("orders", identity, false)`. Explicit takeover uses `true` and
can partially fence the old producer even if opening fails. There is no SDK
durable outbox. Optional broker message-ID deduplication returns original
offsets; it does not establish exactly-once application processing.

## Async and cancellation

`Client.connectAsync`, `producerAsync`, `consumerAsync`, `sendAsync`,
`confirmedAsync`, `flushAsync`, `nextAsync`, and `closeAsync` return
`CompletableFuture`. Ready sends (single or multipart), reads, and
confirmations avoid native task setup, and other ready results complete before
the call returns. Waiting futures are polled by one native binding executor,
with no Java worker per operation. Blocking waits park Java and virtual threads
and complete the future themselves. `orTimeout` and `completeOnTimeout` cancel
on the common pool, never on a shared timer thread.

```java
var receipt = producer.sendAsync(new byte[]{42}, SendOptions.partition(0))
    .thenCompose(io.ozzy.PendingRecord::confirmedAsync);
receipt.join();
```

Canceling a pending operation returns its native lease before cancellation
completes. If Rust already produced its result, cancellation returns false and
that result remains available. Canceling an observer never withdraws an
admitted record. Cancel an outstanding receive before closing its handle.
An interrupted blocking wait cancels pending work and preserves interrupt
status; an already completed native result wins the race.

Mutable handles allow one active operation; overlapping calls raise
`BusyException`. Producer and consumer opens on one client may run
concurrently. Pending confirmations support simultaneous observers. Close
is idempotent, canceled closes remain sealed, and Cleaner is only a fallback.
Close child handles before the client. SDK failures raise `OzzyException`;
closed handles raise `ClosedException`; invalid arguments are rejected locally
or by Rust validation. Java continuations run outside native owner threads.
`obtrudeValue`, `obtrudeException`, and `completeAsync` are unsupported on SDK
futures because they bypass native completion ownership.

## History, bounds, and compression

`ReaderOptions` selects earliest retained records, latest confirmed ends,
partition filters, or a `Checkpoint`. Latest-end selection resolves when
first polled. Checkpoints contain the exact topic UUID and each selected
partition's first undelivered offset. They record delivery, not application
transactions, and cannot restore retired history. `stats()` reports live,
replayed, and gap counters. Final checkpoint/statistics remain available after
close. Offsets, epochs, sequences, and counters use unsigned long bits;
use `Long.toUnsignedString` when formatting values beyond signed range.

`ClientConfig.Limits` separates producer batch bounds from receive-frame
bounds. Defaults match Python: 1 MiB packets, 64 KiB records, 64 records/256
parts outbound, 2048/2048 inbound. `Capacity` defaults to 16 partitions, one
producer, one consumer, and a 32-frame reader queue (range 1..64). Rust charges
queued and retained backing through the same reservation helpers as Python.

`withCompression(Compression.RAW)` disables adaptive APPEND LZ4; consumers
decode both encodings, including mixed history. The default is adaptive LZ4.
`lz4+tcp://` selects independent OMQ transport compression; use raw APPENDs to
avoid applying both layers.

`scripts/check-java.sh` tests both APIs, codecs, and broker modes over OMQ
inproc and memory storage, then tests packaged JARs in separate JVMs over TCP
and LZ4 TCP, including module-path loading, abrupt producer exit, and native
extraction cleanup. The shared harness remains the
real broker; these tests do not establish physical-disk or power-loss safety.

## Benchmarks

The [SDK process benchmark](../../ozzy-bench/README.md#java-sdk) runs independent
producer JVMs, the real broker, and a consumer JVM on separate CPU sets. It
verifies payloads and offsets, bounds backlog, and records CPU and memory.
