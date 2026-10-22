"""Independent SDK scenarios. Rust runs each API/mode/codec cell separately."""

import asyncio
import contextlib
import dataclasses
import inspect
import random
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

from ozzy import BrokerAddress, Checkpoint, ClientConfig, ClosedError, OzzyError
from ozzy import Client as BlockingClient
from ozzy.asyncio import Client as AsyncClient

CASE = globals()["CASE"]
ASYNC = globals()["ASYNC"]
CODEC = globals()["CODEC"]
POLICY = globals()["POLICY"]
config = ClientConfig(
    tuple(BrokerAddress(*broker) for broker in globals()["BROKERS"]),
    max_partitions=4,
    max_producers=3,
    max_consumers=3,
    max_packet_bytes=32768,
    max_record_bytes=4096,
    max_records=8,
    max_parts=32,
    compression=CODEC,
)


async def call(function, *args, **kwargs):
    result = function(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@contextlib.asynccontextmanager
async def opened(factory, *args, **kwargs):
    handle = await call(factory, *args, **kwargs)
    try:
        yield handle
    finally:
        await call(handle.close)


def rejected(errors, action):
    try:
        action()
    except errors:
        return
    raise AssertionError("invalid operation succeeded")


async def payload_boundaries(client):
    # Includes zero-length records, exact byte/part limits, typed and strided views.
    payloads = [
        b"",
        b"x" * 4096,
        tuple(b"" for _ in range(32)),
        (b"a", b"", b"b"),
        memoryview(bytearray(range(32)))[::2],
        memoryview(bytearray(range(32))).cast("I"),
        (bytearray(b"mutable"), memoryview(b"view")),
    ]
    expected = [
        (bytes(p),) if isinstance(p, (bytes, memoryview)) else tuple(map(bytes, p))
        for p in payloads
    ]
    async with opened(client.producer, "orders") as producer:
        pending = [await call(producer.send, p, partition=0) for p in payloads]
        await call(producer.flush)
        receipts = [await call(p.confirmed) for p in pending]
        assert [r.sequence for r in receipts] == list(range(len(payloads)))
    async with opened(client.consumer, "orders", partitions=[0]) as consumer:
        for index, wanted in enumerate(expected):
            record = await call(consumer.next)
            assert record.parts == wanted and record.offset == index
            assert record.message_id == receipts[index].message_id


async def mutable_snapshot(client):
    async with opened(client.producer, "orders") as producer:
        source = bytearray(b"original")
        parts = [source, memoryview(source), b""]
        pending = await call(producer.send, parts, partition=2)
        source[:] = b"modified"
        parts.clear()
        await call(pending.confirmed)
    async with opened(client.consumer, "orders", partitions=[2]) as consumer:
        record = await call(consumer.next)
        assert record.parts == (b"original", b"original", b"")
        assert record.parts == record.parts
        rejected(AttributeError, lambda: setattr(record, "offset", 99))


async def invalid_admission(client):
    async with opened(client.producer, "orders") as producer:
        invalid = [
            ([], {}),
            ([b""] * 33, {}),
            ("text", {}),
            ([b"a", 1], {}),
            (b"x" * 4097, {}),
            (b"x", {"key": "text"}),
            (b"x", {"key": b"", "partition": 0}),
            (b"x", {"id": UUID(int=0)}),
            (b"x", {"id": "bad UUID"}),
            (b"x", {"partition": -1}),
            (b"x", {"partition": 2**32}),
            (b"x", {"partition": 4}),
        ]
        for payload, options in invalid:
            try:
                await call(producer.send, payload, **options)
            except (ValueError, TypeError, OverflowError, OzzyError):
                pass
            else:
                raise AssertionError(f"invalid send accepted: {options}")
        pending = await call(producer.send, b"valid", partition=0)
        receipt = await call(pending.confirmed)
        assert receipt.sequence == receipt.offset == 0
        assert pending.sequence == 0


async def identity_formats(client):
    async with opened(client.producer, "orders") as producer:
        ids = [uuid4(), str(uuid4()), str(uuid4()).upper()]
        pending = [await call(producer.send, b"id", id=i, partition=1) for i in ids]
        for item, original in zip(pending, ids, strict=True):
            receipt = await call(item.confirmed)
            assert receipt.message_id == str(UUID(str(original)))
        generated = await call(producer.send, b"generated", partition=1)
        receipt = await call(generated.confirmed)
        assert UUID(receipt.message_id).version == 7
        assert receipt.producer_id != receipt.message_id


async def routing(client):
    async with opened(client.producer, "orders") as producer:
        auto = []
        for _ in range(16):
            pending = await call(producer.send, b"auto")
            await call(pending.confirmed)  # Form an APPEND before sticky rotation.
            auto.append(pending)
        assert {p.partition for p in auto} == set(range(4))
        for key in (b"", b"customer", b"\x00\xff"):
            pending = [await call(producer.send, b"keyed", key=key) for _ in range(8)]
            assert len({p.partition for p in pending}) == 1
        for partition in range(4):
            pending = await call(producer.send, b"explicit", partition=partition)
            receipt = await call(pending.confirmed)
            assert pending.partition == receipt.partition == partition
        await call(producer.flush)


async def shared_partition(client):
    async with opened(client.producer, "orders") as first:
        async with opened(client.producer, "orders") as second:
            expected = []
            for index in range(32):
                producer = first if index % 2 == 0 else second
                pending = await call(producer.send, index.to_bytes(2), partition=3)
                expected.append((index, await call(pending.confirmed)))
            assert len({r.producer_id for _, r in expected}) == 2
            assert [r.offset for _, r in expected] == list(range(32))
            assert [r.sequence for _, r in expected] == [n // 2 for n in range(32)]
    async with opened(client.consumer, "orders", partitions=[3]) as consumer:
        for index, receipt in expected:
            record = await call(consumer.next)
            assert record.offset == receipt.offset and record.parts == (
                index.to_bytes(2),
            )


async def confirmation_lifetime(client):
    producer = await call(client.producer, "orders")
    pending = await call(producer.send, b"retained", partition=1)
    await call(producer.close)
    for _ in range(8):
        receipt = await call(pending.confirmed)
        immediate = pending.try_confirmed()
        assert receipt.message_id == immediate.message_id
        assert receipt.offset == immediate.offset == 0
        assert receipt.policy == POLICY
        assert receipt.topic_id == pending.topic_id
        rejected(AttributeError, lambda receipt=receipt: setattr(receipt, "offset", 9))
    for action in (producer.flush, lambda: producer.send(b"closed")):
        try:
            await call(action)
        except ClosedError:
            pass
        else:
            raise AssertionError("closed producer remained usable")


async def duplicate_identity(client):
    async with opened(client.producer, "orders") as first:
        event = uuid4()
        original = await call(first.send, (b"original", b""), partition=2, id=event)
        receipt = await call(original.confirmed)
        async with opened(client.producer, "orders") as second:
            duplicate = await call(second.send, b"replacement", partition=2, id=event)
            result = await call(duplicate.confirmed)
            assert result.disposition == "duplicate" and result.offset == receipt.offset
            other = await call(second.send, b"other partition", partition=3, id=event)
            assert (await call(other.confirmed)).disposition == "stored"
    async with opened(client.consumer, "orders", partitions=[2]) as consumer:
        assert (await call(consumer.next)).parts == (b"original", b"")
        assert consumer.try_next() is None


async def checkpoint_prefix(client):
    async with opened(client.producer, "orders") as producer:
        for index in range(12):
            await call(producer.send, bytes([index]), partition=index % 2)
        await call(producer.flush)
    async with opened(client.consumer, "orders", partitions=[0, 1]) as consumer:
        delivered = [(await call(consumer.next)).message_id for _ in range(5)]
        checkpoint = consumer.checkpoint()
        assert sum(offset for _, offset in checkpoint.positions) == 5
    async with opened(
        client.consumer, "orders", checkpoint=checkpoint, partitions=[0, 1]
    ) as consumer:
        remaining = [(await call(consumer.next)).message_id for _ in range(7)]
        assert not set(delivered) & set(remaining)
        assert consumer.try_next() is None
        assert consumer.checkpoint().positions == ((0, 6), (1, 6))


async def latest(client):
    async with opened(client.producer, "orders") as producer:
        for partition in range(4):
            pending = await call(producer.send, b"old", partition=partition)
            await call(pending.confirmed)
        async with opened(client.consumer, "orders", start="latest") as consumer:
            # Rust subscribes lazily. Drive each latest cursor before new writes.
            async with asyncio.timeout(5):
                while consumer.checkpoint().positions != tuple(
                    (p, 1) for p in range(4)
                ):
                    assert consumer.try_next() is None
                    await asyncio.sleep(0)
            pending = await call(producer.send, b"new", partition=2)
            receipt = await call(pending.confirmed)
            record = await call(consumer.next)
            assert record.parts == (b"new",) and record.offset == receipt.offset == 1


async def independent_readers(client):
    async with opened(client.producer, "orders") as producer:
        pending = await call(producer.send, b"shared", partition=0)
        receipt = await call(pending.confirmed)
    async with opened(client.consumer, "orders", partitions=[0]) as first:
        async with opened(client.consumer, "orders", partitions=[0]) as second:
            a = await call(first.next)
            assert second.checkpoint().positions == ((0, 0),)
            b = await call(second.next)
            assert a.message_id == b.message_id == receipt.message_id
            assert a.parts == b.parts == (b"shared",)


async def checkpoint_rejections(client):
    async with opened(client.consumer, "orders") as consumer:
        saved = consumer.checkpoint()
    for options in [
        dict(partitions=[0, 0]),
        dict(partitions=[4]),
        dict(start="invalid"),
        dict(checkpoint=Checkpoint(str(uuid4()), saved.positions)),
        dict(checkpoint=saved, partitions=[0]),
        dict(checkpoint=Checkpoint(saved.topic_id, ((0, 0), (0, 0)))),
    ]:
        try:
            handle = await call(client.consumer, "orders", **options)
        except (ValueError, OzzyError):
            pass
        else:
            await call(handle.close)
            raise AssertionError(f"invalid consumer accepted: {options}")
    async with opened(client.consumer, "orders") as consumer:
        assert consumer.checkpoint() == saved


async def context_cleanup(client):
    class Marker(Exception):
        pass

    producer = await call(client.producer, "orders")
    try:
        if ASYNC:
            async with producer:
                await producer.send(b"drained", partition=0)
                raise Marker
        else:
            with producer:
                producer.send(b"drained", partition=0)
                raise Marker
    except Marker:
        pass
    async with opened(client.consumer, "orders", partitions=[0]) as consumer:
        assert (await call(consumer.next)).parts == (b"drained",)
    await call(producer.close)


async def resume(client):
    first = await call(client.producer, "orders")
    identity = first.identity
    old = await call(first.send, b"old", partition=0)
    old_receipt = await call(old.confirmed)
    await call(first.close)
    async with opened(client.producer, "orders", identity=identity) as second:
        assert second.identity == identity
        new = await call(second.send, b"new", partition=0)
        receipt = await call(new.confirmed)
        assert receipt.sequence == 1 and receipt.offset == 1
        assert receipt.producer_id == old_receipt.producer_id


async def seeded_roundtrip(client):
    # Independent expected bytes, deterministic incompressible and compressible bodies.
    randomizer = random.Random(0x5A77)
    expected = {}
    async with opened(client.producer, "orders") as producer:
        for number in range(96):
            size = randomizer.choice([0, 1, 128, 1024, 4096])
            body = randomizer.randbytes(size) if number % 2 else b"x" * size
            cuts = sorted([0, size, randomizer.randrange(size + 1)])
            payload = (body[: cuts[1]], b"", body[cuts[1] :])
            pending = await call(producer.send, payload, partition=number % 4)
            receipt = await call(pending.confirmed)
            expected[receipt.partition, receipt.offset] = (receipt.message_id, payload)
    async with opened(client.consumer, "orders") as consumer:
        last = [-1] * 4
        for _ in range(len(expected)):
            record = await call(consumer.next)
            assert record.offset == last[record.partition] + 1
            last[record.partition] = record.offset
            assert (record.message_id, record.parts) == expected.pop(
                (record.partition, record.offset)
            )
        assert not expected and consumer.try_next() is None


async def many_ready_records(client):
    async with opened(client.producer, "orders") as producer:
        pending = [
            await call(producer.send, bytes([i]), partition=i % 4) for i in range(96)
        ]
        await call(producer.flush)
        expected = {(p.partition, (await call(p.confirmed)).offset) for p in pending}
    async with opened(client.consumer, "orders") as consumer:
        for _ in range(96):
            record = await call(consumer.next)
            assert (record.partition, record.offset) in expected
            assert record.parts == (bytes([record.offset * 4 + record.partition]),)
            expected.remove((record.partition, record.offset))
        assert not expected


async def mixed_codecs(client):
    factory = AsyncClient.connect if ASYNC else BlockingClient
    other_config = dataclasses.replace(
        config, compression="raw" if CODEC == "lz4" else "lz4"
    )
    async with opened(factory, other_config) as other:
        async with opened(client.producer, "orders") as first:
            async with opened(other.producer, "orders") as second:
                for number in range(24):
                    producer = first if number % 2 else second
                    item = await call(
                        producer.send, bytes([number]) * 4096, partition=0
                    )
                    assert (await call(item.confirmed)).offset == number
    async with opened(client.consumer, "orders", partitions=[0]) as consumer:
        for number in range(24):
            assert (await call(consumer.next)).parts == (bytes([number]) * 4096,)


async def native_values(client):
    # Lazy metadata and payload values own their data after SDK owners are gone.
    identity = uuid4()
    async with opened(client.producer, "orders") as producer:
        pending = await call(producer.send, b"value" * 512, partition=3, id=identity)
        receipt = await call(pending.confirmed)
    async with opened(client.consumer, "orders", partitions=[3]) as consumer:
        record = await call(consumer.next)
    await call(client.close)
    assert receipt.partition == record.partition == 3
    assert receipt.offset == record.offset == receipt.sequence == 0
    assert receipt.message_id == record.message_id == str(identity)
    assert receipt.topic_id == record.topic_id
    assert receipt.incarnation == record.incarnation
    assert UUID(receipt.topic_id).int and UUID(receipt.incarnation).int
    assert UUID(receipt.producer_id).int and receipt.producer_epoch == 1
    assert receipt.owner_epoch >= 1
    assert receipt.policy == POLICY and receipt.disposition == "stored"
    assert (await call(pending.confirmed)).message_id == str(identity)
    with ThreadPoolExecutor(max_workers=2) as pool:
        views = list(pool.map(lambda _, value=record: value.parts, range(8)))
    assert views == [(b"value" * 512,)] * 8
    parts = record.parts
    assert parts == record.parts == (b"value" * 512,)
    rejected(
        AttributeError, lambda value=receipt: setattr(value, "message_id", "changed")
    )
    del record, receipt, pending
    assert parts == (b"value" * 512,)


async def reader_bounds(client):
    # A consumer's writer limits cannot reject records written by another client.
    factory = AsyncClient.connect if ASYNC else BlockingClient
    tight = dataclasses.replace(config, max_records=1, max_parts=1)
    wanted = (b"left" * 128, b"", b"right" * 128)
    async with opened(client.producer, "orders") as producer:
        for _ in range(12):
            await call(producer.send, wanted, partition=0)
        await call(producer.flush)
    async with opened(factory, tight) as other:
        async with opened(other.consumer, "orders", partitions=[0]) as consumer:
            for offset in range(12):
                record = await call(consumer.next)
                assert record.parts == wanted and record.offset == offset
            async with opened(other.producer, "orders") as producer:
                try:
                    await call(producer.send, wanted, partition=0)
                except ValueError:
                    pass
                else:
                    raise AssertionError("producer part limit was ignored")
                pending = await call(producer.send, b"one", partition=0)
                assert (await call(pending.confirmed)).sequence == 0
            assert (await call(consumer.next)).parts == (b"one",)


async def reader_statistics(client):
    async with opened(client.producer, "orders") as producer:
        for _ in range(24):
            await call(producer.send, b"counted", partition=0)
        await call(producer.flush)
    consumer = await call(client.consumer, "orders", partitions=[0])
    before = consumer.stats()
    assert before == (0, 0, 0)
    for delivered in range(1, 25):
        assert (await call(consumer.next)).parts == (b"counted",)
        stats = consumer.stats()
        assert stats.live_records + stats.replayed_records == delivered
        assert all(a >= b for a, b in zip(stats, before, strict=True))
        before = stats
    await call(consumer.close)
    assert consumer.stats() == before


async def main():
    factory = AsyncClient.connect if ASYNC else BlockingClient
    async with opened(factory, config) as client:
        await globals()[CASE](client)


asyncio.run(asyncio.wait_for(main(), 9))
