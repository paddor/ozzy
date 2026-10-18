import asyncio
from collections.abc import Callable
from uuid import uuid4

from ozzy import BrokerAddress, BusyError, ClientConfig, OzzyError
from ozzy.asyncio import Client

BROKERS: list[tuple[str, str, str]] = globals()["BROKERS"]
POLICY: str = globals()["POLICY"]
hold_writes: Callable[[bool], None] = globals()["hold_writes"]
held_writes: Callable[[], int] = globals()["held_writes"]

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


async def until(condition):
    async with asyncio.timeout(5):
        while not condition():
            await asyncio.sleep(0)


async def cancelled(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return
    raise AssertionError("task was not cancelled")


async def main():
    async with Client(config) as client:
        # Opens share the client's links, so they may run concurrently.
        opened = await asyncio.gather(
            client.consumer("orders", partitions=[2]),
            client.consumer("orders", partitions=[3]),
        )
        for consumer in opened:
            await consumer.close()
        async with await client.producer("orders") as producer:
            # Cancellation of confirmation stops only its observation.
            hold_writes(True)
            record_id = uuid4()
            pending = await producer.send((b"", b"async"), partition=1, id=record_id)
            await until(lambda: held_writes() > 0)
            observer = asyncio.create_task(pending.confirmed())
            await asyncio.sleep(0)
            await cancelled(observer)
            assert pending.try_confirmed() is None
            hold_writes(False)
            receipts = await asyncio.gather(pending.confirmed(), pending.confirmed())
            assert all(
                r.message_id == str(record_id) and r.policy == POLICY for r in receipts
            )
            async with await client.consumer("orders", partitions=[1]) as consumer:
                record = await anext(consumer)
                assert record.parts == (b"", b"async") and record.message_id == str(
                    record_id
                )
                assert consumer.try_next() is None
                # Pending next retains the reader lease; cancellation returns it.
                task = asyncio.create_task(consumer.next())
                await asyncio.sleep(0)
                try:
                    await consumer.next()
                except BusyError:
                    pass
                else:
                    raise AssertionError("concurrent receive accepted")
                await cancelled(task)
                saved = consumer.checkpoint()
                next_pending = await producer.send(b"after cancel", partition=1)
                receipt = await next_pending.confirmed()
                record = await consumer.next()
                assert record.offset == receipt.offset
                assert record.parts == (b"after cancel",)
            async with await client.consumer(
                "orders", checkpoint=saved, partitions=[1]
            ) as consumer:
                assert (await consumer.next()).parts == (b"after cancel",)

            # Concurrent observers each resolve once their own record confirms.
            hold_writes(True)
            pending = [await producer.send(bytes([n]), partition=3) for n in range(64)]
            await until(lambda: held_writes() > 0)
            waiting = [asyncio.ensure_future(p.confirmed()) for p in pending]
            # A waiting flush does not hold the producer, so sends continue.
            flushing = asyncio.create_task(producer.flush())
            await asyncio.sleep(0)
            late = await producer.send(b"during flush", partition=3)
            assert not flushing.done() and not any(task.done() for task in waiting)
            hold_writes(False)
            receipts = await asyncio.gather(*waiting)
            assert [r.sequence for r in receipts] == list(range(64))
            await flushing
            assert (await late.confirmed()).sequence == 64
            # Native awaitables follow the coroutine protocol.
            native = producer._native.aflush()
            assert asyncio.iscoroutine(native)
            await asyncio.create_task(native)

            # Bound intake under held storage. Cancel the first blocked send,
            # then verify every admitted record and its contiguous sequence.
            hold_writes(True)
            accepted = []
            bulk = b"bounded".ljust(4096, b"x")
            for _ in range(256):
                task = asyncio.create_task(producer.send(bulk, partition=0))
                done, _ = await asyncio.wait([task], timeout=0.1)
                if not done:
                    await cancelled(task)
                    break
                accepted.append(task.result())
            else:
                raise AssertionError("intake did not apply backpressure")
            assert accepted
            hold_writes(False)
            await producer.flush()
            receipts = await asyncio.gather(*(item.confirmed() for item in accepted))
            assert [r.sequence for r in receipts] == list(range(len(receipts)))
            last = await producer.send(b"after pressure", partition=0)
            assert last.sequence == len(receipts)
            await last.confirmed()
            try:
                await producer.send(b"invalid", partition=99)
            except OzzyError:
                pass
            else:
                raise AssertionError("unknown partition accepted")
            await producer.flush()
        async with await client.consumer("orders", partitions=[0]) as consumer:
            for _ in range(len(receipts)):
                assert (await consumer.next()).parts == (bulk,)
            assert (await consumer.next()).parts == (b"after pressure",)
            assert consumer.try_next() is None
    await client.close()
    connected = await Client.connect(config)
    await connected.close()


asyncio.run(asyncio.wait_for(main(), 9))
