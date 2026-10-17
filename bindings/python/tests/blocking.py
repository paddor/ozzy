import dataclasses
import gc
import json
import pickle
import sys
import threading
import time
import weakref
from collections.abc import Callable
from uuid import UUID, uuid4

from ozzy import BrokerAddress, Checkpoint, Client, ClientConfig, ClosedError, OzzyError

BROKERS: list[tuple[str, str, str]] = globals()["BROKERS"]
POLICY: str = globals()["POLICY"]
hold_writes: Callable[[bool], None] = globals()["hold_writes"]
held_writes: Callable[[], int] = globals()["held_writes"]

assert pickle.loads(pickle.dumps(OzzyError("example"))).args == ("example",)

config = ClientConfig(
    tuple(BrokerAddress(*broker) for broker in BROKERS),
    max_partitions=4,
    max_producers=2,
    max_consumers=2,
    max_packet_bytes=32768,
    max_record_bytes=4096,
    max_records=8,
    max_parts=32,
)


def raises(error, action):
    try:
        action()
    except error:
        return
    raise AssertionError(f"expected {error}")


# Configuration is validated without connecting or opening SDK owners.
for changes in [
    dict(brokers=()),
    dict(max_parts=0),
    dict(max_partitions=0),
    dict(request_timeout=0),
    dict(request_timeout=float("nan")),
    dict(max_producers=2**63),
]:
    raises(
        (ValueError, OzzyError, OverflowError),
        lambda changes=changes: Client(dataclasses.replace(config, **changes)),
    )

with Client(config) as client:
    with client.producer("orders") as producer:
        assert producer.partition_count == 4
        identity = producer.identity
        assert len(identity) == 32
        for payload in [[], ["text"], "text", b"x" * 4097]:
            raises(
                (TypeError, ValueError), lambda payload=payload: producer.send(payload)
            )
        raises(ValueError, lambda: producer.send(b"x", key=b"", partition=0))
        raises(ValueError, lambda: producer.send(b"x", id=UUID(int=0)))
        raises(OzzyError, lambda: producer.send(b"x", partition=99))
        raises(OverflowError, lambda: producer.send(b"x", partition=-1))
        expected = {}
        pending = []
        mutable = bytearray(b"snapshot")
        first = producer.send(mutable, partition=0)
        mutable[:] = b"changed!"
        receipt = first.confirmed()
        assert receipt.offset == 0 and receipt.sequence == 0
        assert receipt.policy == POLICY and receipt.disposition == "stored"
        expected[receipt.partition, receipt.offset] = (
            receipt.message_id,
            (b"snapshot",),
        )
        assert first.try_confirmed().offset == 0
        duplicate = producer.send(
            b"discarded", partition=0, id=receipt.message_id
        ).confirmed()
        assert (
            duplicate.offset == receipt.offset and duplicate.disposition == "duplicate"
        )
        for number in range(24):
            record_id = uuid4()
            payload = (b"", number.to_bytes(2), b"\x00\xff")
            item = producer.send(payload, partition=number % 4, id=record_id)
            pending.append((item, str(record_id), payload))
        keyed = [producer.send(b"keyed", key=b"customer") for _ in range(3)]
        assert len({item.partition for item in keyed}) == 1
        producer.flush()
        for item, record_id, payload in pending:
            receipt = item.confirmed()
            assert (
                receipt.partition == item.partition and receipt.message_id == record_id
            )
            expected[receipt.partition, receipt.offset] = (record_id, payload)
        for item in keyed:
            receipt = item.confirmed()
            expected[receipt.partition, receipt.offset] = (
                receipt.message_id,
                (b"keyed",),
            )

    with client.producer("orders", identity=identity) as resumed:
        receipt = resumed.send(b"resumed", partition=0).confirmed()
        expected[receipt.partition, receipt.offset] = (
            receipt.message_id,
            (b"resumed",),
        )
    raises(ClosedError, lambda: resumed.send(b"closed"))
    raises(ValueError, lambda: client.producer("orders", identity=b"short"))
    raises(ValueError, lambda: client.producer("orders", takeover=True))
    original = client.producer("orders")
    receipt = original.send(b"before fence", partition=0).confirmed()
    expected[receipt.partition, receipt.offset] = receipt.message_id, (b"before fence",)
    with client.producer(
        "orders", identity=original.identity, takeover=True
    ) as replacement:
        receipt = replacement.send(b"after fence", partition=0).confirmed()
        expected[receipt.partition, receipt.offset] = (
            receipt.message_id,
            (b"after fence",),
        )
        raises(OzzyError, lambda: original.send(b"fenced", partition=0).confirmed())
    raises(OzzyError, original.close)

    with client.consumer("orders") as consumer:
        observed = {}
        last = {}
        for _ in range(len(expected)):
            record = next(consumer)
            assert record.offset == last.get(record.partition, -1) + 1
            last[record.partition] = record.offset
            observed[record.partition, record.offset] = record.message_id, record.parts
        assert observed == expected
        assert consumer.try_next() is None
        saved = consumer.checkpoint()
        # JSON roundtrip without a language-specific checkpoint serialization.
        raw = json.loads(json.dumps(dataclasses.asdict(saved)))
        saved = Checkpoint(raw["topic_id"], tuple(map(tuple, raw["positions"])))
    consumer.close()
    raises(OzzyError, consumer.next)
    with client.consumer("orders", checkpoint=saved) as consumer:
        assert consumer.try_next() is None
    with client.consumer("orders", partitions=[2]) as consumer:
        assert next(consumer).partition == 2
    raises(OzzyError, lambda: client.consumer("orders", partitions=[99]))
    raises(OzzyError, lambda: client.consumer("orders", partitions=[0, 0]))
    raises(ValueError, lambda: client.consumer("orders", start="invalid"))
    wrong_topic = Checkpoint(str(uuid4()), saved.positions)
    raises(OzzyError, lambda: client.consumer("orders", checkpoint=wrong_topic))

    # A separate Python thread releases the held disk while confirmed() blocks.
    # It must acquire the GIL for the blocking API to make progress.
    hold_writes(True)
    with client.producer("orders") as producer:
        item = producer.send(b"GIL released", partition=0)

        def release():
            deadline = time.monotonic() + 5
            while not held_writes():
                assert time.monotonic() < deadline
                time.sleep(0)
            hold_writes(False)

        worker = threading.Thread(target=release)
        worker.start()
        assert item.confirmed().policy == POLICY
        worker.join(timeout=5)
        assert not worker.is_alive()

    # Python may collect a handle on a thread other than its creating thread.
    failures = []
    previous_hook = sys.unraisablehook
    sys.unraisablehook = failures.append
    try:
        orphan = client.producer("orders")
        orphan.close()
        reference = weakref.ref(orphan)
        holder = [orphan]
        del orphan
        collector = threading.Thread(target=lambda: holder.pop())
        collector.start()
        collector.join(timeout=5)
        assert not collector.is_alive() and reference() is None
        gc.collect()
        assert not failures
    finally:
        sys.unraisablehook = previous_hook

client.close()
raises(ClosedError, lambda: client.producer("orders"))
