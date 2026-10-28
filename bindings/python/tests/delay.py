"""Scheduled records on a topic with delay, through the blocking or asyncio API.

Payload compression stays on: scheduled records still go out raw."""

import asyncio
import contextlib
import time

import ozzy.asyncio
from ozzy import BrokerAddress, Client, ClientConfig, OzzyError

BROKERS: list[tuple[str, str, str]] = globals()["BROKERS"]
ASYNC: bool = globals()["ASYNC"]

config = ClientConfig(
    tuple(BrokerAddress(*broker) for broker in BROKERS),
    max_partitions=4,
    max_producers=2,
    max_consumers=2,
    max_packet_bytes=32768,
    max_record_bytes=4096,
    max_records=8,
    max_parts=32,
    compression="lz4",
)


def now() -> int:
    return time.time_ns() // 1_000_000


def raises(error, action):
    try:
        action()
    except error:
        return
    raise AssertionError(f"expected {error}")


def sends(start: int) -> list[tuple[bytes, int | None]]:
    """Payloads and due times: two out of due order, one stored at once, and
    one already due."""
    return [
        (b"a", start + 2_000),
        (b"b", start + 1_000),
        (b"c", None),
        (b"d", start - 1_000),
    ]


def check(start: int, receipts, arrivals: list[tuple[bytes, int]]) -> None:
    """Held records leave in due order, none before its due time."""
    dispositions = [receipt.disposition for receipt in receipts]
    expected = ["scheduled", "scheduled", "stored", "scheduled"]
    assert dispositions == expected, dispositions
    # A scheduled receipt's offset is its schedule sequence.
    assert sorted(receipts[i].offset for i in (0, 1, 3)) == [0, 1, 2]
    order = [payload for payload, _ in arrivals]
    assert set(order[:2]) == {b"c", b"d"} and order[2:] == [b"b", b"a"], order
    due = {payload: due for payload, due in sends(start)}
    for payload, arrived in arrivals:
        held = due[payload]
        assert held is None or arrived >= held, (payload, held, arrived)


def blocking() -> None:
    with Client(config) as client:
        with client.producer("orders") as producer:
            for error, values in [
                (TypeError, ["1", 1.5, True]),
                (ValueError, [-1, 2**64]),
            ]:
                for value in values:
                    raises(
                        error, lambda value=value: producer.send(b"x", due_millis=value)
                    )
            with client.consumer("orders", partitions=[0]) as consumer:
                start = now()
                pending = [
                    producer.send(payload, partition=0, due_millis=due)
                    for payload, due in sends(start)
                ]
                receipts = [record.confirmed(timeout=10) for record in pending]
                arrivals = []
                for _ in range(4):
                    record = consumer.next(timeout=10)
                    arrivals.append((record.parts[0], now()))
                check(start, receipts, arrivals)
        # Past the topic's maximum delay fails permanently. The refusal fails
        # its writer, so closing it may report the refusal again.
        producer = client.producer("orders")
        late = producer.send(b"late", partition=0, due_millis=now() + 120_000)
        raises(OzzyError, lambda: late.confirmed(timeout=10))
        with contextlib.suppress(OzzyError):
            producer.close()


async def asynchronous() -> None:
    async with ozzy.asyncio.Client(config) as client:
        async with await client.producer("orders") as producer:
            async with await client.consumer("orders", partitions=[0]) as consumer:
                start = now()
                pending = [
                    await producer.send(payload, partition=0, due_millis=due)
                    for payload, due in sends(start)
                ]
                receipts = [await record.confirmed() for record in pending]
                arrivals = []
                async with asyncio.timeout(10):
                    for _ in range(4):
                        record = await consumer.next()
                        arrivals.append((record.parts[0], now()))
                check(start, receipts, arrivals)
        producer = await client.producer("orders")
        late = await producer.send(b"late", partition=0, due_millis=now() + 120_000)
        try:
            await late.confirmed()
        except OzzyError:
            pass
        else:
            raise AssertionError("a due time past the maximum delay was scheduled")
        with contextlib.suppress(OzzyError):
            await producer.close()


if ASYNC:
    asyncio.run(asynchronous())
else:
    blocking()
