"""One application role per process. The Rust runner owns barriers and brokers."""

import asyncio
import cProfile
import faulthandler
import hashlib
import json
import os
import platform
import pstats
import resource
import struct
import sys
import time
from pathlib import Path

from ozzy import BrokerAddress, Client, ClientConfig, __version__, _native
from ozzy.asyncio import Client as AsyncClient

options = json.loads(os.environ["OZZY_BENCH_OPTIONS"])
config = ClientConfig(
    tuple(BrokerAddress(*b) for b in options["brokers"]),
    max_partitions=4,
    compression=options["codec"],
    reader_queue_messages=options["reader_queue_messages"],
)
header = struct.Struct("<QII")
bodies = tuple(
    (
        json.dumps({"customer": i, "event": "purchase", "items": [i, i + 1]})
        * (options["size"] // 32 + 2)
    ).encode()[: options["size"] - header.size]
    for i in range(256)
)
faulthandler.dump_traceback_later(
    options["warmup"] + options["duration"] + 60, exit=True
)


def report(value):
    print(json.dumps(value), flush=True)


def cpu_time():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def read_stats(handle):
    stats = getattr(handle, "stats", None)
    return stats() if stats else None


def completed(handle, before, phase, count, cpu):
    result = {
        "event": "done",
        "phase": phase,
        "records": count,
        "cpu_seconds": cpu_time() - cpu,
    }
    after = read_stats(handle)
    if before is not None:
        result["reader"] = dict(
            zip(
                after._fields,
                (a - b for a, b in zip(after, before, strict=True)),
                strict=True,
            )
        )
        assert (
            result["reader"]["live_records"] + result["reader"]["replayed_records"]
            == count + options["producers"] * 4
        )
    report(result)


def command():
    value = json.loads(sys.stdin.readline())
    assert value["command"] == "run"
    return value


def payload(sequence, producer, phase):
    return (
        header.pack(sequence, producer, phase)
        + bodies[(sequence * options["producers"] + producer) % 256]
    )


class Verifier:
    def __init__(self):
        self.positions = [0] * 4
        self.sequences = [[p for p in range(4)] for _ in range(options["producers"])]

    def record(self, record, phase):
        parts = record.parts
        assert len(parts) == 1 and len(parts[0]) == options["size"]
        sequence, producer, found_phase = header.unpack_from(parts[0])
        assert producer < options["producers"]
        assert parts[0] == payload(sequence, producer, found_phase)
        partition = record.partition
        assert record.offset == self.positions[partition]
        self.positions[partition] += 1
        if found_phase == phase + 2:
            return producer, partition
        assert found_phase == phase and partition == sequence % 4
        assert sequence == self.sequences[producer][partition]
        self.sequences[producer][partition] += 4
        return producer


def consume(consumer, verifier, phase):
    ended = set()
    count = 0
    credits = [0] * options["producers"]
    while len(ended) < options["producers"] * 4:
        record = consumer.next()
        marker = verifier.record(record, phase)
        if isinstance(marker, int):
            count += 1
            credit(credits, marker)
        else:
            assert marker not in ended
            ended.add(marker)
    return count


async def aconsume(consumer, verifier, phase):
    ended = set()
    count = 0
    credits = [0] * options["producers"]
    while len(ended) < options["producers"] * 4:
        record = await consumer.next()
        marker = verifier.record(record, phase)
        if isinstance(marker, int):
            count += 1
            credit(credits, marker)
        else:
            assert marker not in ended
            ended.add(marker)
    return count


def credit(credits, producer):
    credits[producer] += 1
    if credits[producer] % options["window"] == 0:
        report({"event": "credit", "producer": producer})


def admission_credit(count):
    if count % options["window"] == 0:
        assert json.loads(sys.stdin.readline())["command"] == "credit"


def produce(producer, sequence, phase, deadline):
    start = sequence
    identity = options["producer"]
    while time.time_ns() < deadline:
        pending = []
        for _ in range(256):
            p = producer.send(
                payload(sequence, identity, phase), partition=sequence % 4
            )
            if options["confirmation"] == "each":
                pending.append(p)
            sequence += 1
        for p in pending:
            p.confirmed()
        admission_credit(sequence - start)
    for partition in range(4):
        producer.send(payload(sequence, identity, phase + 2), partition=partition)
    producer.flush()
    return sequence, sequence - start


async def aproduce(producer, sequence, phase, deadline):
    start = sequence
    identity = options["producer"]
    while time.time_ns() < deadline:
        pending = []
        for _ in range(256):
            p = await producer.send(
                payload(sequence, identity, phase), partition=sequence % 4
            )
            if options["confirmation"] == "each":
                pending.append(p)
            sequence += 1
        for p in pending:
            await p.confirmed()
        admission_credit(sequence - start)
    for partition in range(4):
        await producer.send(payload(sequence, identity, phase + 2), partition=partition)
    await producer.flush()
    return sequence, sequence - start


def blocking():
    with Client(config) as client:
        handle = (
            client.producer("orders")
            if options["role"] == "producer"
            else client.consumer("orders")
        )
        with handle:
            report({"event": "ready"})
            verifier, sequence = Verifier(), 0
            for phase in (0, 1):
                cmd = command()
                before = read_stats(handle)
                cpu = cpu_time()
                if options["role"] == "producer":
                    sequence, count = produce(handle, sequence, phase, cmd["deadline"])
                else:
                    count = consume(handle, verifier, phase)
                completed(handle, before, phase, count, cpu)
            assert json.loads(sys.stdin.readline())["command"] == "finish"


async def asynchronous():
    async with AsyncClient(config) as client:
        handle = (
            await client.producer("orders")
            if options["role"] == "producer"
            else await client.consumer("orders")
        )
        async with handle:
            report({"event": "ready"})
            verifier, sequence = Verifier(), 0
            for phase in (0, 1):
                cmd = command()
                before = read_stats(handle)
                cpu = cpu_time()
                if options["role"] == "producer":
                    sequence, count = await aproduce(
                        handle, sequence, phase, cmd["deadline"]
                    )
                else:
                    count = await aconsume(handle, verifier, phase)
                completed(handle, before, phase, count, cpu)
            assert json.loads(sys.stdin.readline())["command"] == "finish"


profiler = cProfile.Profile() if options["profile"] else None
if profiler:
    profiler.enable()
if options["api"] == "blocking":
    blocking()
else:
    asyncio.run(asynchronous())
if profiler:
    profiler.disable()
    path = options["profile"] + "." + options["role"] + str(options["producer"])
    profiler.dump_stats(path)
    with open(path + ".txt", "w") as output:
        pstats.Stats(profiler, stream=output).sort_stats("tottime").print_stats(40)
report(
    {
        "event": "finished",
        "python": platform.python_version(),
        "sdk_version": __version__,
        "native_sha256": hashlib.sha256(
            Path(_native.__file__).read_bytes()
        ).hexdigest(),
        "python_sdk_sha256": hashlib.sha256(
            b"".join(
                p.read_bytes()
                for p in sorted(Path(_native.__file__).parent.glob("*.py"))
            )
        ).hexdigest(),
        "workload_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
)
faulthandler.cancel_dump_traceback_later()
