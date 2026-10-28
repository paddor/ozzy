"""Asyncio scheduling of the same Rust SDK used by ozzy's blocking API."""

from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import Literal, Self

from . import Checkpoint, ClientConfig, ConsumerStats, Receipt, Record, _native
from ._types import MessageId, Payload, destination, due, message_id, parts


class PendingRecord:
    """Observation of an admitted record; dropping it does not cancel the write."""

    __slots__ = ("_native",)

    def __init__(self, native: _native.PendingRecord) -> None:
        self._native = native

    @property
    def topic_id(self) -> str:
        """Topic UUID, formatted on access."""
        return self._native.topic_id

    @property
    def partition(self) -> int:
        """Numeric destination partition chosen before admission."""
        return self._native.partition

    @property
    def sequence(self) -> int:
        """Retry sequence for this producer and partition, independent of batching."""
        return self._native.sequence

    async def confirmed(self) -> Receipt:
        """Wait for durable confirmation. Cancellation cancels only this observation."""
        receipt = self._native.try_confirmed()
        if receipt is not None:
            return receipt
        return await self._native.aconfirmed()

    def try_confirmed(self) -> Receipt | None:
        """Return a durable receipt if ready, None if pending, or raise on failure."""
        return self._native.try_confirmed()


class Producer:
    """Async topic writer with the admission and identity rules of ozzy.Producer.

    Obtain from Client.producer(), then use async with or await close(). An
    overlapping operation on a leased handle raises BusyError.
    """

    def __init__(self, native: _native.Producer, config: ClientConfig) -> None:
        self._native = native
        self._config = config

    @property
    def identity(self) -> bytes:
        """Opaque 32-byte resume identity. Persist it to reopen this producer."""
        return self._native.identity

    @property
    def partition_count(self) -> int:
        """Number of partitions in this writer's topic metadata."""
        return self._native.partition_count

    async def send(
        self,
        payload: Payload,
        *,
        key: bytes | None = None,
        partition: int | None = None,
        id: MessageId | None = None,
        due_millis: int | None = None,
    ) -> PendingRecord:
        """Admit one record, yielding under pressure; return its confirmation handle.

        key and partition are mutually exclusive; omitting both lets Rust route.
        id is a nonzero UUID, generated if omitted. Mutable payloads are copied
        before admission. due_millis holds the record until that absolute Unix
        time in milliseconds, as in ozzy.Producer.send. Cancellation before
        admission assigns no sequence; admitted records remain active
        independently of this coroutine.
        """
        body = parts(payload, self._config)
        identity = (message_id(id), due(due_millis))
        route = destination(key, partition)
        pending = self._native.try_send(body, identity, route)
        if pending is None:
            # TODO: Under backpressure the probe copies the payload into a
            # record and asend copies it again. Hand the probe's record over.
            pending = await self._native.asend(body, identity, route)
        return PendingRecord(pending)

    async def flush(self) -> None:
        """Wait for all admitted records. Cancellation leaves the writer usable."""
        await self._native.aflush()

    async def close(self) -> None:
        """Seal and drain this writer; idempotent. Cancellation leaves it sealed."""
        await self._native.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


class Consumer(AsyncIterator[Record]):
    """Async reader with partition ordering and checkpoints as in ozzy.Consumer.

    Obtain from Client.consumer(). One receive or close may be active; overlapping
    operations on a leased handle raise BusyError. Use async with for cleanup.
    """

    def __init__(self, native: _native.Consumer) -> None:
        self._native = native

    async def next(self) -> Record:
        """Wait for a record and advance delivery; cancellation preserves position."""
        record = self._native.try_next()
        if record is not None:
            return record
        return await self._native.anext()

    __anext__ = next

    def try_next(self) -> Record | None:
        """Return an available record and advance, or None without waiting."""
        return self._native.try_next()

    def checkpoint(self) -> Checkpoint:
        """Snapshot first undelivered offsets. Save only after processing succeeds."""
        return Checkpoint._from_native(self._native.checkpoint())

    def stats(self) -> ConsumerStats:
        """Return cumulative delivery counters, including final counters after close."""
        return ConsumerStats(*self._native.stats())

    async def close(self) -> None:
        """Stop delivery and release subscriptions; idempotent."""
        await self._native.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


class Client:
    """Shared Rust broker links, opened by async with or await Client.connect().

    Construction alone does not connect. Close child handles before this client
    and construct a fresh client after fork.
    """

    def __init__(self, config: ClientConfig) -> None:
        self._config = config
        self._native = _native.Client(config._native())

    @classmethod
    async def connect(cls, config: ClientConfig) -> Self:
        """Create and connect a client; pair with async with or await close()."""
        client = cls(config)
        await client._native.aconnect()
        return client

    async def producer(
        self, topic: str, *, identity: bytes | None = None, takeover: bool = False
    ) -> Producer:
        """Open a writer, optionally resuming identity or taking over its epoch.

        takeover requires a saved identity; failure can still fence partitions.
        """
        return Producer(
            await self._native.aproducer(topic, identity, takeover), self._config
        )

    async def consumer(
        self,
        topic: str,
        *,
        start: Literal["earliest", "latest"] = "earliest",
        checkpoint: Checkpoint | None = None,
        partitions: Sequence[int] | None = None,
    ) -> Consumer:
        """Open retained history, the current end, or checkpoint offsets.

        checkpoint requires start='earliest'. partitions=None selects all;
        otherwise only those numeric partitions are delivered. Retired history
        cannot be recovered by a checkpoint.
        """
        return Consumer(
            await self._native.aconsumer(
                topic,
                (
                    start,
                    None if checkpoint is None else checkpoint._native(),
                    partitions,
                ),
            )
        )

    async def close(self) -> None:
        """Shut down shared links and the native runtime; idempotent."""
        await self._native.aclose()

    async def __aenter__(self) -> Self:
        await self._native.aconnect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
