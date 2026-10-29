"""Python values and input conversion. All log behavior lives in Rust."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, NamedTuple, Self, cast
from uuid import UUID

from . import _native

type Buffer = bytes | bytearray | memoryview
type Payload = Buffer | Sequence[Buffer]
type MessageId = UUID | str


class ConsumerStats(NamedTuple):
    """Cumulative counters from one reader, preserved after close.

    live_records counts live PUB delivery; replayed_records counts PEER repair
    and history. Their sum counts records returned to the application.
    live_gaps counts transitions into repair, not missing records.
    """

    live_records: int
    replayed_records: int
    live_gaps: int


@dataclass(frozen=True)
class BrokerAddress:
    """Persistent broker identity and its two configured OMQ endpoints.

    node_id is the nonzero UUID from the broker identity file. peer carries
    control; data_peer carries records and replay. Use matching endpoint schemes,
    for example tcp:// or lz4+tcp://. Clients never bind broker endpoints.
    """

    node_id: UUID | str
    peer: str
    data_peer: str


@dataclass(frozen=True)
class ClientConfig:
    """Finite Rust SDK windows shared by this client's handles.

    brokers lists broker identities and endpoints. max_partitions bounds topic
    metadata; max_producers and max_consumers reserve handle capacity.
    max_packet_bytes bounds an envelope payload; max_record_bytes bounds one
    uncompressed record. max_records and max_parts bound outbound batches.
    reader_max_records and reader_max_parts independently bound inbound frames.
    reader_queue_messages (1..64) bounds OMQ publication buffering.
    request_timeout is positive seconds; compression is adaptive 'lz4' or 'raw'.

    Bounds reserve memory before opening. Invalid or overflowing configurations
    raise ValueError, OverflowError, or OzzyError when constructing a client.
    Transport compression is selected separately by endpoint scheme.
    """

    brokers: Sequence[BrokerAddress]
    max_partitions: int = 16
    max_producers: int = 1
    max_consumers: int = 1
    max_packet_bytes: int = 1024 * 1024
    max_record_bytes: int = 64 * 1024
    max_records: int = 64
    max_parts: int = 256
    request_timeout: float = 5.0
    compression: Literal["lz4", "raw"] = "lz4"
    reader_queue_messages: int = 32
    reader_max_records: int = 2048
    reader_max_parts: int = 2048

    def _native(self) -> _native.Config:
        return _native.Config(
            [(UUID(str(b.node_id)).bytes, b.peer, b.data_peer) for b in self.brokers],
            (
                self.max_packet_bytes,
                self.max_record_bytes,
                self.max_records,
                self.max_parts,
                self.reader_max_records,
                self.reader_max_parts,
            ),
            (
                self.max_partitions,
                self.max_producers,
                self.max_consumers,
                self.reader_queue_messages,
            ),
            self.request_timeout,
            self.compression,
        )


@dataclass(frozen=True)
class Checkpoint:
    """Topic UUID and (numeric partition, first undelivered offset) pairs.

    Persist only after processing succeeds. This value does not commit progress
    to the broker and cannot restore records removed by retention. Positions
    describe partition-global offsets, not a producer's retry sequence.
    """

    topic_id: str
    positions: tuple[tuple[int, int], ...]

    @classmethod
    def _from_native(cls, value: _native.Checkpoint) -> Self:
        return cls(value.topic_id, tuple(value.positions))

    def _native(self) -> _native.Checkpoint:
        return _native.Checkpoint(UUID(self.topic_id).bytes, self.positions)


def parts(payload: Payload, config: ClientConfig) -> Payload:
    """Validate shape and size. Rust copies each buffer once during send."""
    if isinstance(payload, bytes):
        if config.max_parts < 1 or len(payload) > config.max_record_bytes:
            raise ValueError("payload exceeds client limits")
        return payload
    values: Sequence[Buffer]
    if isinstance(payload, (bytearray, memoryview)):
        single = True
        values = (payload,)
    elif isinstance(payload, Sequence) and not isinstance(payload, str):
        single = False
        values = cast(Sequence[Buffer], payload)
    else:
        raise TypeError("payload must be bytes or a sequence of byte parts")
    if not 1 <= len(values) <= config.max_parts:
        raise ValueError("payload must contain between 1 and max_parts parts")
    size = 0
    for value in values:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("each payload part must be bytes-like")
        size += value.nbytes if isinstance(value, memoryview) else len(value)
    if size > config.max_record_bytes:
        raise ValueError("payload exceeds max_record_bytes")
    return payload if single else tuple(values)


def message_id(value: MessageId | None) -> bytes | None:
    """Convert a UUID or UUID string to its 16 bytes; None stays None."""
    if value is None:
        return None
    return value.bytes if isinstance(value, UUID) else UUID(value).bytes


def due(value: int | None) -> int | None:
    """Check an absolute Unix due time in milliseconds; None stays None."""
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("due_millis must be an int")
    if not 0 <= value < 2**64:
        raise ValueError("due_millis must fit an unsigned 64-bit integer")
    return value


def destination(
    key: bytes | None, partition: int | None
) -> tuple[bytes | None, int | None]:
    """Check a routing key or exact partition; at most one may be given."""
    if key is not None and not isinstance(key, bytes):
        raise TypeError("key must be bytes")
    if key is not None and partition is not None:
        raise ValueError("key and partition are mutually exclusive")
    return key, partition
