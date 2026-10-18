# Ozzy Java SDK

Blocking and `CompletableFuture` producers and consumers for
[Ozzy](../../README.md). Java 22+ on Linux, as a thin layer over the Rust SDK.

## Build

Requires JDK 22+, Rust 1.93+, Maven, and cargo-nextest. From the repository
root, in bash:

```sh
source scripts/ozzy_tools.sh  # sets CARGO_TARGET_DIR
OZZY_JAVA_RELEASE=1 scripts/check-java.sh  # builds and tests the JARs
JAR=$(ls "$CARGO_TARGET_DIR"/java/ozzy-java-*-linux-*.jar)
```

The platform JAR bundles `libozzy_java.so` and loads it on first use. JPMS
module: `io.ozzy`.

## Try it

Start a local broker with topic `orders`
([getting started](../../GETTING_STARTED.md#try-it)):

```sh
cargo run --release -p ozzy-broker -- quickstart tmp/try
```

Save as `Quickstart.java` and run it from the repository root:

```sh
java --enable-native-access=ALL-UNNAMED -cp "$JAR" Quickstart.java
```

```java
import io.ozzy.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

public class Quickstart {
    // Broker ID and endpoints written by `quickstart tmp/try`.
    static String value(String file, String key) throws Exception {
        var text = Files.readString(Path.of("tmp/try", file));
        var match = Pattern.compile("(?m)^" + key + " = \"(.+)\"").matcher(text);
        if (!match.find()) throw new IllegalStateException(key);
        return match.group(1);
    }

    public static void main(String[] args) throws Exception {
        var broker = new BrokerAddress(
                UUID.fromString(value("shared.identity", "local")),
                value("deployment.toml", "peer"),
                value("deployment.toml", "data_peer"));
        var config = new ClientConfig(List.of(broker));

        try (var client = Client.connect(config)) {
            try (var producer = client.producer("orders")) {
                var key = SendOptions.key("customer-17".getBytes());
                var pending = new ArrayList<PendingRecord>();
                for (int n = 0; n < 3; n++)
                    pending.add(producer.send(("order-" + n).getBytes(), key));
                for (var record : pending) {
                    var receipt = record.confirmed(); // durable on the broker
                    System.out.println("stored " + receipt.partition() + " " + receipt.offset());
                }
            }
            try (var consumer = client.consumer("orders")) {
                for (int n = 0; n < 3; n++) {
                    var record = consumer.next();
                    var body = StandardCharsets.UTF_8.decode(record.body());
                    System.out.println("read " + record.partition() + " " + record.offset() + " " + body);
                }
                var checkpoint = consumer.checkpoint(); // persist after processing
            }
        }
    }
}
```

Each blocking call has an `*Async` twin that returns a `CompletableFuture`:

```java
var receipt = producer.sendAsync("order-42".getBytes(), SendOptions.partition(0))
        .thenCompose(PendingRecord::confirmedAsync)
        .orTimeout(5, TimeUnit.SECONDS)
        .join();
```

## How it works

- `io.ozzy` calls the Rust SDK in `libozzy_java.so`. Routing, batching,
  retries, LZ4, and OMQ transport stay in Rust. There is no Java wire protocol.
- Each `Client` runs a Rust SDK thread and OMQ I/O threads. Brokers own the
  log; the client owns no journal and binds no socket.
- Ready sends, reads, and confirmations are direct FFM downcalls. They create
  no future and no native task.
- Lifecycle calls and waits use JNI. One process-wide native thread
  (`ozzy/java`) polls all pending operations; there is no Java thread per
  operation. Blocking calls park the caller, so virtual threads work.
- Future continuations never run on native threads.
- `ClientConfig` bounds memory before connecting. Under pressure, `send`
  waits; no Java queue grows.

## Essentials

| Type | Use |
| --- | --- |
| `ClientConfig(brokers)` | Brokers; `withLimits`, `withCapacity`, `withCompression`, `withRequestTimeout` |
| `BrokerAddress(nodeId, peer, dataPeer)` | Broker UUID and its two endpoints |
| `Client` | `connect(config)`, `producer(topic)`, `consumer(topic, ReaderOptions)`, `close()` |
| `Producer` | `send(body, SendOptions)` returns `PendingRecord`; `flush()`; `identity()` |
| `SendOptions` | `AUTOMATIC`, `key(bytes)`, `partition(n)`, `withId(uuid)` |
| `PendingRecord` | `confirmed()` returns `Receipt`; `tryConfirmed()` |
| `Receipt` | `partition()`, `offset()`, `messageId()`, `disposition()`, `policy()` |
| `Consumer` | `next()`, iteration, `tryNext()`, `checkpoint()`, `stats()` |
| `Record` | `partition()`, `offset()`, `messageId()`, `body()`, `parts()` |
| `ReaderOptions` | Earliest or latest start, a `Checkpoint`, or a partition filter |
| `OzzyException` | SDK failure; `ClosedException` and `BusyException` are subclasses |

`send` takes `byte[]`, a `ByteBuffer`, or a `List<ByteBuffer>` of parts. Every
public API has Javadoc.

## Good to know

- `send` returns once the record is admitted locally. `confirmed()` waits for
  durability: the local disk in single-broker mode, two of three copies in disk
  quorum. Keep many records in flight; confirm or `flush()` later.
- A key always routes to the same partition. Order holds per partition.
- Save `producer.identity()` and resume with `client.producer(topic, identity,
  false)` after a restart. `true` fences an old process still using it.
- Persist a checkpoint only after processing succeeds, then reopen with
  `new ReaderOptions(checkpoint)`. It tracks delivery, not application effects.
  Ozzy gives no exactly-once processing.
- One active operation per handle; an overlapping call throws
  `BusyException`. Opens may overlap. Confirmations allow many observers.
- Canceling a future frees its handle. It never withdraws an admitted record.
- `Capacity` defaults to one producer and one consumer per client. Raise it
  for more open handles.
- Offsets, epochs, and sequences are unsigned 64-bit; format them with
  `Long.toUnsignedString`.
- Close producers and consumers before their client. The Cleaner is only a
  fallback.
- `Compression.LZ4` (the default) packs APPENDs adaptively. With `lz4+tcp://`
  endpoints, use `Compression.RAW`.
- On the module path, use `--enable-native-access=io.ozzy`. The Java-only JAR
  loads `-Dozzy.native.path=/path/libozzy_java.so`.

## Development

`scripts/check-java.sh` tests both APIs against the real broker over OMQ
inproc and memory storage, then tests the packaged JARs in separate JVMs over
TCP. The [SDK process benchmark](../../ozzy-bench/README.md#java-sdk) runs
producer and consumer JVMs against a real broker.
