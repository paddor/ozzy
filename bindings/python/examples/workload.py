"""Benchmark client only. Run through the Rust ozzy_python_bench example."""

import asyncio
import cProfile
import faulthandler
import hashlib
import json
import os
import platform
import pstats
import time
from pathlib import Path

from ozzy import BrokerAddress, Client, ClientConfig, __version__, _native
from ozzy.asyncio import Client as AsyncClient

options = json.loads(os.environ["OZZY_BENCH_OPTIONS"])
workload_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
native_sha256 = hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest()
sdk_sha256 = hashlib.sha256(
    b"".join(
        path.read_bytes() for path in sorted(Path(_native.__file__).parent.glob("*.py"))
    )
).hexdigest()
faulthandler.dump_traceback_later(60, exit=True)
config = ClientConfig(
    tuple(BrokerAddress(*b) for b in json.loads(os.environ["OZZY_TEST_BROKERS"])),
    max_partitions=4,
    compression=options["codec"],
    reader_queue_messages=options["reader_queue_messages"],
)
size = options["size"]
assert size >= 16
# Vary JSON event text within records. Full bytes are reconstructed by the reader.
bodies = tuple(
    (
        json.dumps(
            {"customer": i, "event": "purchase", "currency": "USD", "items": [i, i + 1]}
        )
        * (size // 64 + 2)
    ).encode()[: size - 8]
    for i in range(256)
)


def payload(number):
    return number.to_bytes(8, "little") + bodies[number % 256]


def verify(records, expected):
    for record in records:
        number = int.from_bytes(record.parts[0][:8], "little")
        assert record.parts == (payload(number),)
        receipt = expected.pop(number)
        assert record.message_id == receipt.message_id
        assert record.offset == receipt.offset and record.partition == number % 4
    assert not expected


def blocking_round(producer, consumer, number):
    pending = [
        producer.send(payload(i), partition=i % 4) for i in range(number, number + 256)
    ]
    receipts = {i: p.confirmed() for i, p in enumerate(pending, number)}
    verify((consumer.next() for _ in range(256)), receipts)


async def async_round(producer, consumer, number):
    pending = [
        await producer.send(payload(i), partition=i % 4)
        for i in range(number, number + 256)
    ]
    receipts = {i: await p.confirmed() for i, p in enumerate(pending, number)}
    for _ in range(256):
        record = await consumer.next()
        number = int.from_bytes(record.parts[0][:8], "little")
        verify((record,), {number: receipts.pop(number)})
    assert not receipts


def measure(producer, consumer):
    number = 0
    for seconds in (options["warmup"], options["duration"]):
        count = 0
        start = time.perf_counter()
        while time.perf_counter() - start < seconds:
            blocking_round(producer, consumer, number)
            number += 256
            count += 256
        elapsed = time.perf_counter() - start
    return count, elapsed


async def measure_async(producer, consumer):
    number = 0
    for seconds in (options["warmup"], options["duration"]):
        count = 0
        start = time.perf_counter()
        while time.perf_counter() - start < seconds:
            await async_round(producer, consumer, number)
            number += 256
            count += 256
        elapsed = time.perf_counter() - start
    return count, elapsed


async def asynchronous():
    async with AsyncClient(config) as client:
        async with await client.producer("orders") as producer:
            async with await client.consumer("orders") as consumer:
                return await measure_async(producer, consumer)


def workload():
    if options["api"] == "asyncio":
        return asyncio.run(asynchronous())
    with Client(config) as client:
        with client.producer("orders") as producer:
            with client.consumer("orders") as consumer:
                return measure(producer, consumer)


if options["profile"]:
    profiler = cProfile.Profile()
    profiler.enable()
count, elapsed = workload()
if options["profile"]:
    profiler.disable()
    profiler.dump_stats(options["profile"])
    with open(options["profile"] + ".txt", "w") as report:
        pstats.Stats(profiler, stream=report).sort_stats("tottime").print_stats(40)
print(
    json.dumps(
        {
            **options,
            "records": count,
            "seconds": elapsed,
            "records_per_second": count / elapsed,
            "payload_bytes_per_second": count * size / elapsed,
            "python": platform.python_version(),
            "sdk_version": __version__,
            "native_sha256": native_sha256,
            "python_sdk_sha256": sdk_sha256,
            "workload_sha256": workload_sha256,
        }
    )
)
faulthandler.cancel_dump_traceback_later()
