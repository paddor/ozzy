"""Blocking Ozzy SDK. See ozzy.asyncio for the asyncio API."""

from collections.abc import Iterator, Sequence
from types import TracebackType
from typing import Literal, Self

from . import _native
from ._native import (
    BusyError,
    ClosedError,
    OzzyError,
    PendingRecord,
    Receipt,
    Record,
    __version__,
)
from ._types import (
    BrokerAddress,
    Checkpoint,
    ClientConfig,
    ConsumerStats,
    MessageId,
    Payload,
    destination,
    message_id,
    parts,
)

__all__ = [
    "BrokerAddress",
    "BusyError",
    "Checkpoint",
    "Client",
    "ClientConfig",
    "ClosedError",
    "Consumer",
    "ConsumerStats",
    "MessageId",
    "OzzyError",
    "Payload",
    "PendingRecord",
    "Producer",
    "Receipt",
    "Record",
    "__version__",
]


class Producer:
    """One topic writer. Obtain it from Client.producer(), then close or use with.

    Calls admit individual records into bounded Rust queues. Admission does not
    prove persistence; observe a pending confirmation or call flush(). Only one
    send, flush, or close may be active on this handle at a time.
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

    def send(
        self,
        payload: Payload,
        *,
        key: bytes | None = None,
        partition: int | None = None,
        id: MessageId | None = None,
    ) -> PendingRecord:
        """Admit one record, waiting for capacity; return its confirmation handle.

        Supply either key for stable routing or an exact partition number.
        With neither, Rust selects a partition. id is a nonzero UUID; omission
        generates one. Mutable payloads are copied before admission. Reuse the
        same id when retrying the same application event after an unknown result.
        """
        return self._native.send(
            parts(payload, self._config),
            message_id(id),
            destination(key, partition),
        )

    def flush(self) -> None:
        """Wait until all records admitted by this producer are confirmed."""
        self._native.flush()

    def close(self) -> None:
        """Seal admission, drain confirmations, and release this writer; idempotent."""
        self._native.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class Consumer(Iterator[Record]):
    """One ordered topic reader, obtained from Client.consumer().

    Ordering and offsets are per partition. Delivery is not acknowledgment of
    application processing. Use a checkpoint only after processing succeeds.
    One receive or close may be active on this handle at a time.
    """

    def __init__(self, native: _native.Consumer) -> None:
        self._native = native

    def next(self) -> Record:
        """Wait for the next record and advance the delivered checkpoint."""
        return self._native.next()

    __next__ = next

    def try_next(self) -> Record | None:
        """Return an available record and advance, or None without waiting."""
        return self._native.try_next()

    def checkpoint(self) -> Checkpoint:
        """Snapshot each partition's first undelivered offset, without committing it."""
        return Checkpoint._from_native(self._native.checkpoint())

    def stats(self) -> ConsumerStats:
        """Return cumulative delivery counters, including final counters after close."""
        return ConsumerStats(*self._native.stats())

    def close(self) -> None:
        """Stop delivery and release subscriptions; idempotent."""
        self._native.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class Client:
    """Connect immediately and share bounded Rust broker links among handles.

    Use as a context manager or call close(). Close producers and consumers
    before their client. Construct a fresh client after fork.
    """

    def __init__(self, config: ClientConfig) -> None:
        self._config = config
        self._native = _native.Client(config._native())
        self._native.connect()

    def producer(
        self, topic: str, *, identity: bytes | None = None, takeover: bool = False
    ) -> Producer:
        """Open a topic producer, optionally resuming its saved opaque identity.

        takeover requires identity and fences the previous producer epoch.
        A failed takeover may already have fenced some partitions.
        """
        return Producer(self._native.producer(topic, identity, takeover), self._config)

    def consumer(
        self,
        topic: str,
        *,
        start: Literal["earliest", "latest"] = "earliest",
        checkpoint: Checkpoint | None = None,
        partitions: Sequence[int] | None = None,
    ) -> Consumer:
        """Open a reader from retained history, the current end, or a checkpoint.

        checkpoint requires start='earliest' (the default). partitions restricts
        delivery to those numeric partitions; None selects all. A checkpoint
        cannot recover history that the broker has already retired.
        """
        return Consumer(
            self._native.consumer(
                topic,
                (
                    start,
                    None if checkpoint is None else checkpoint._native(),
                    partitions,
                ),
            )
        )

    def close(self) -> None:
        """Shut down shared broker links and the native runtime; idempotent."""
        self._native.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
