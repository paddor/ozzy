from collections.abc import Coroutine, Sequence
from typing import Any, Self, final

type _Buffer = bytes | bytearray | memoryview

__version__: str
__all__ = [
    "BusyError",
    "Checkpoint",
    "Client",
    "ClosedError",
    "Config",
    "Consumer",
    "OzzyError",
    "PendingRecord",
    "Producer",
    "Receipt",
    "Record",
    "__version__",
]

class OzzyError(Exception):
    """Rust SDK failure. An unknown write outcome must be resolved using record
    identity."""

class ClosedError(OzzyError):
    """Operation on a closed handle."""

class BusyError(OzzyError):
    """Another asynchronous operation exclusively leases this handle."""

@final
class Config:
    def __new__(
        cls,
        brokers: Sequence[tuple[bytes, str, str]],
        bounds: tuple[int, int, int, int, int, int],
        capacity: tuple[int, int, int, int],
        timeout: float,
        compression: str,
    ) -> Self: ...

@final
class Client:
    def __new__(cls, config: Config) -> Self: ...
    def connect(self) -> None: ...
    def aconnect(self) -> Coroutine[Any, Any, None]: ...
    def producer(
        self, topic: str, identity: bytes | None = None, takeover: bool = False
    ) -> Producer: ...
    def aproducer(
        self, topic: str, identity: bytes | None = None, takeover: bool = False
    ) -> Coroutine[Any, Any, Producer]: ...
    def consumer(
        self, topic: str, options: tuple[str, Checkpoint | None, Sequence[int] | None]
    ) -> Consumer: ...
    def aconsumer(
        self, topic: str, options: tuple[str, Checkpoint | None, Sequence[int] | None]
    ) -> Coroutine[Any, Any, Consumer]: ...
    def close(self) -> None: ...
    def aclose(self) -> Coroutine[Any, Any, None]: ...

@final
class Producer:
    @property
    def identity(self) -> bytes: ...
    @property
    def partition_count(self) -> int: ...
    def send(
        self,
        parts: _Buffer | Sequence[_Buffer],
        id: bytes | None,
        destination: tuple[bytes | None, int | None],
        timeout: float | None = None,
    ) -> PendingRecord: ...
    def asend(
        self,
        parts: _Buffer | Sequence[_Buffer],
        id: bytes | None,
        destination: tuple[bytes | None, int | None],
    ) -> Coroutine[Any, Any, PendingRecord]: ...
    def try_send(
        self,
        parts: _Buffer | Sequence[_Buffer],
        id: bytes | None,
        destination: tuple[bytes | None, int | None],
    ) -> PendingRecord | None: ...
    def flush(self, timeout: float | None = None) -> None: ...
    def aflush(self) -> Coroutine[Any, Any, None]: ...
    def close(self) -> None: ...
    def aclose(self) -> Coroutine[Any, Any, None]: ...

@final
class PendingRecord:
    """Confirmation observer. Dropping it never cancels an admitted record."""

    @property
    def topic_id(self) -> str:
        """Topic UUID, formatted on access."""
    @property
    def partition(self) -> int:
        """Numeric destination partition selected before admission."""
    @property
    def sequence(self) -> int:
        """Retry sequence within this producer and partition, independent of
        batching."""
    def confirmed(self, timeout: float | None = None) -> Receipt:
        """Wait for a durable receipt, releasing the GIL while waiting.

        Raises TimeoutError after timeout seconds; the record stays admitted.
        """
    def aconfirmed(self) -> Coroutine[Any, Any, Receipt]:
        """Await a durable receipt. Cancellation cancels only this observation."""
    def try_confirmed(self) -> Receipt | None:
        """Return a receipt if ready, None if pending, or raise on failure."""

@final
class Consumer:
    def next(self, timeout: float | None = None) -> Record: ...
    def anext(self) -> Coroutine[Any, Any, Record]: ...
    def try_next(self) -> Record | None: ...
    def checkpoint(self) -> Checkpoint: ...
    def stats(self) -> tuple[int, int, int]: ...
    def close(self) -> None: ...
    def aclose(self) -> Coroutine[Any, Any, None]: ...

@final
class Checkpoint:
    def __new__(cls, topic_id: bytes, positions: Sequence[tuple[int, int]]) -> Self: ...
    @property
    def topic_id(self) -> str: ...
    @property
    def positions(self) -> list[tuple[int, int]]: ...

@final
class Record:
    """Immutable delivered record with partition-global metadata and opaque payload.

    Delivery advances the reader; it does not acknowledge application processing.
    Payload parts are copied once on delivery, so a retained record never holds
    Rust receive-frame capacity.
    """

    @property
    def topic_id(self) -> str:
        """Topic UUID, formatted on access."""
    @property
    def partition(self) -> int:
        """Numeric partition containing this record."""
    @property
    def incarnation(self) -> str:
        """Partition incarnation UUID, formatted on access."""
    @property
    def offset(self) -> int:
        """Partition-global record offset."""
    @property
    def message_id(self) -> str:
        """Application event UUID, formatted on access."""
    @property
    def parts(self) -> tuple[bytes, ...]:
        """Tuple of payload bytes, copied once on delivery."""

@final
class Receipt:
    """Immutable durable confirmation. UUID strings are formatted only on access.

    Confirmation does not prove application processing.
    """

    @property
    def topic_id(self) -> str:
        """Topic UUID."""
    @property
    def partition(self) -> int:
        """Numeric destination partition."""
    @property
    def incarnation(self) -> str:
        """Partition incarnation UUID, fencing older histories."""
    @property
    def offset(self) -> int:
        """Partition-global record offset."""
    @property
    def message_id(self) -> str:
        """Application event UUID used for record identity."""
    @property
    def producer_id(self) -> str:
        """UUID of the producer that submitted this record."""
    @property
    def producer_epoch(self) -> int:
        """Fencing epoch of the submitting producer."""
    @property
    def owner_epoch(self) -> int:
        """Partition owner epoch that confirmed the record."""
    @property
    def sequence(self) -> int:
        """Retry sequence within this producer and partition, independent of
        batching."""
    @property
    def disposition(self) -> str:
        """'stored' for a new event or 'duplicate' for an existing deduplicated
        event."""
    @property
    def policy(self) -> str:
        """Durability boundary: 'local_durable' or 'quorum_durable'."""
