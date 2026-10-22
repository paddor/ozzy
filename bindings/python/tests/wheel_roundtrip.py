"""Runs in a fresh interpreter, importing the installed wheel."""

import asyncio
import faulthandler
import gc
import json
import os
import random
import signal
from uuid import uuid4

from ozzy import BrokerAddress, Client, ClientConfig, OzzyError
from ozzy.asyncio import Client as AsyncClient

faulthandler.dump_traceback_later(20, exit=True)


def body(number):
    tail = (
        random.Random(number).randbytes(32767)
        if number % 2
        else bytes([number]) * 32767
    )
    return bytes([number]) + tail


config = ClientConfig(
    tuple(BrokerAddress(*b) for b in json.loads(os.environ["OZZY_TEST_BROKERS"])),
    max_partitions=4,
    compression=os.environ["OZZY_TEST_CODEC"],
)
with Client(config) as client:
    with client.producer("orders") as producer:
        pending = [producer.send((b"", body(i)), partition=i % 4) for i in range(32)]
        for item in pending:
            assert item.confirmed().policy == os.environ["OZZY_TEST_POLICY"]
        # Inherited Rust thread owners fail before access and can be collected.
        pid = os.fork()
        if pid == 0:
            signal.alarm(7)
            for action in (
                lambda: producer.identity,
                pending[0].confirmed,
                lambda: client.producer("orders"),
            ):
                try:
                    action()
                except OzzyError as error:
                    assert "fork" in str(error)
                else:
                    os._exit(1)
            del producer, pending, client
            gc.collect()
            fresh = Client(config)
            writer = fresh.producer("orders")
            receipt = writer.send(b"crashed producer", partition=3).confirmed()
            assert receipt.offset == 8
            # Abrupt producer process exit after confirmation, without SDK close.
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
    with client.consumer("orders") as consumer:
        seen = {p: [] for p in range(4)}
        crashed = 0
        for _ in range(33):
            record = next(consumer)
            if record.parts == (b"crashed producer",):
                assert record.partition == 3 and record.offset == 8
                crashed += 1
                continue
            assert record.parts[0] == b""
            assert record.parts[1] == body(record.parts[1][0])
            seen[record.partition].append(record.parts[1][0])
        assert seen == {p: list(range(p, 32, 4)) for p in range(4)}
        assert crashed == 1
        saved = consumer.checkpoint()


async def main():
    async with AsyncClient(config) as client:
        async with await client.consumer("orders", checkpoint=saved) as consumer:
            assert consumer.try_next() is None
            task = asyncio.create_task(consumer.next())
            await asyncio.sleep(0)
            async with await client.producer("orders") as producer:
                record_id = uuid4()
                pending = await producer.send(b"async wheel", partition=3, id=record_id)
                receipt = await pending.confirmed()
                assert receipt.message_id == str(record_id)
            record = await task
            assert record.parts == (b"async wheel",)
            assert (
                record.message_id == str(record_id) and record.offset == receipt.offset
            )


asyncio.run(main())
faulthandler.cancel_dump_traceback_later()
