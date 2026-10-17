"""Cancellation and overlapping operations under explicitly held physical I/O."""

import asyncio
import contextlib

from ozzy import BrokerAddress, BusyError, ClientConfig, ClosedError, OzzyError
from ozzy.asyncio import Client

CASE = globals()["CASE"]
hold_writes = globals()["hold_writes"]
held_writes = globals()["held_writes"]
config = ClientConfig(
    tuple(BrokerAddress(*broker) for broker in globals()["BROKERS"]),
    max_partitions=4,
    max_producers=2,
    max_consumers=2,
    max_packet_bytes=32768,
    max_record_bytes=4096,
    max_records=8,
    compression=globals()["CODEC"],
    reader_queue_messages=1,
)


async def until(condition):
    async with asyncio.timeout(5):
        while not condition():
            await asyncio.sleep(0)


async def cancel(task):
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def receive_cancel(client):
    async with await client.consumer("orders", partitions=[1]) as consumer:
        before = consumer.checkpoint()
        for _ in range(32):
            task = asyncio.create_task(consumer.next())
            await asyncio.sleep(0)
            for action in (consumer.try_next, consumer.checkpoint, consumer.stats):
                try:
                    action()
                except BusyError:
                    pass
                else:
                    raise AssertionError("reader lease bypassed")
            await cancel(task)
            assert consumer.checkpoint() == before
        async with await client.producer("orders") as producer:
            item = await producer.send(b"after repeated cancellation", partition=1)
            receipt = await item.confirmed()
            record = await consumer.next()
            assert record.offset == receipt.offset == 0
            assert record.parts == (b"after repeated cancellation",)


async def confirmation_observers(client):
    async with await client.producer("orders") as producer:
        hold_writes(True)
        pending = await producer.send(b"one record", partition=0)
        await until(held_writes)
        observers = [asyncio.create_task(pending.confirmed()) for _ in range(16)]
        await asyncio.sleep(0)
        for task in observers[::2]:
            await cancel(task)
        assert pending.try_confirmed() is None
        hold_writes(False)
        receipts = await asyncio.gather(*observers[1::2])
        assert {r.offset for r in receipts} == {0}
        assert len({r.message_id for r in receipts}) == 1
        assert (await pending.confirmed()).offset == 0


async def flush_cancel(client):
    async with await client.producer("orders") as producer:
        hold_writes(True)
        pending = await producer.send(b"flush", partition=2)
        await until(held_writes)
        task = asyncio.create_task(producer.flush())
        await asyncio.sleep(0)
        assert not task.done()
        await cancel(task)
        assert producer.partition_count == 4
        hold_writes(False)
        await producer.flush()
        assert (await pending.confirmed()).offset == 0
        extra = await producer.send(b"after flush cancellation", partition=2)
        assert extra.sequence == 1
        await extra.confirmed()


async def close_cancel(client):
    producer = await client.producer("orders")
    hold_writes(True)
    pending = await producer.send(b"sealed", partition=0)
    await until(held_writes)
    task = asyncio.create_task(producer.close())
    await asyncio.sleep(0)
    assert not task.done()
    await cancel(task)
    try:
        await producer.send(b"closed", partition=0)
    except ClosedError:
        pass
    else:
        raise AssertionError("canceled close reopened producer")
    hold_writes(False)
    try:
        await pending.confirmed()
    except OzzyError as error:
        assert "unknown" in str(error)
    else:
        raise AssertionError("canceled drain retained a confirmation promise")
    await producer.close()


async def independent_owners(client):
    async with await client.consumer("orders", partitions=[0]) as consumer:
        task = asyncio.create_task(consumer.next())
        await asyncio.sleep(0)
        async with await client.producer("orders") as producer:
            pending = await producer.send(b"independent", partition=0)
            receipt = await pending.confirmed()
            record = await task
            assert record.offset == receipt.offset
            assert record.parts == (b"independent",)


async def early_cancel(client):
    async with await client.producer("orders") as producer:
        # Never poll the native coroutine; no ownership or sequence was taken.
        for _ in range(32):
            task = asyncio.create_task(producer.send(b"unpolled", partition=0))
            await cancel(task)
        pending = await producer.send(b"first admitted", partition=0)
        assert pending.sequence == (await pending.confirmed()).offset == 0


async def owner_error_recovery(client):
    for _ in range(8):
        try:
            await client.producer("orders", identity=b"invalid")
        except ValueError:
            pass
        else:
            raise AssertionError("invalid identity accepted")
    async with await client.producer("orders") as producer:
        for _ in range(8):
            try:
                await producer.send(b"invalid", partition=99)
            except OzzyError:
                pass
            else:
                raise AssertionError("invalid partition accepted")
        pending = await producer.send(b"valid", partition=0)
        assert pending.sequence == (await pending.confirmed()).offset == 0


async def next_timeout(client):
    async with await client.consumer("orders", partitions=[0]) as consumer:
        try:
            async with asyncio.timeout(0):
                await consumer.next()
        except TimeoutError:
            pass
        else:
            raise AssertionError("empty reader did not time out")
        assert consumer.checkpoint().positions == ((0, 0),)
        async with await client.producer("orders") as producer:
            pending = await producer.send(b"after timeout", partition=0)
            await pending.confirmed()
        assert (await consumer.next()).parts == (b"after timeout",)


async def retained_backing(client):
    async with await client.producer("orders") as producer:
        for number in range(96):
            pending = await producer.send(bytes([number]) * 4096, partition=0)
            await pending.confirmed()
    async with await client.consumer("orders", partitions=[0]) as consumer:
        retained = []
        copied = []
        for _ in range(96):
            task = asyncio.create_task(consumer.next())
            done, _ = await asyncio.wait([task], timeout=0.1)
            if not done:
                await cancel(task)
                break
            retained.append(task.result())
            copied.append(retained[-1].parts)
        else:
            raise AssertionError("retained frames did not apply pressure")
        assert retained
        consumed = len(retained)
        assert consumer.checkpoint().positions == ((0, consumed),)
        retained.clear()
        for number in range(consumed, 96):
            assert (await consumer.next()).parts == (bytes([number]) * 4096,)
        assert copied == [(bytes([number]) * 4096,) for number in range(consumed)]
        assert consumer.checkpoint().positions == ((0, 96),)


async def admission_pressure(client):
    async with await client.producer("orders") as producer:
        hold_writes(True)
        expected = []
        for number in range(128):
            body = bytes([number]) * 4096
            task = asyncio.create_task(producer.send(body, partition=0))
            await asyncio.sleep(0)
            if not task.done():
                break
            assert task.result().sequence == len(expected)
            expected.append(body)
        else:
            raise AssertionError("held disk did not exhaust bounded admission")
        await until(held_writes)
        for _ in range(4):
            assert not task.done()
            await cancel(task)
            task = asyncio.create_task(producer.send(b"z" * 4096, partition=0))
            await asyncio.sleep(0)
        hold_writes(False)
        pending = await task
        assert pending.sequence == len(expected)
        expected.append(b"z" * 4096)
        await producer.flush()
    async with await client.consumer("orders", partitions=[0]) as consumer:
        for offset, body in enumerate(expected):
            record = await consumer.next()
            assert record.offset == offset and record.parts == (body,)


async def main():
    async with Client(config) as client:
        try:
            await globals()[CASE](client)
        finally:
            hold_writes(False)


asyncio.run(asyncio.wait_for(main(), 9))
