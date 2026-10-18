"""Pure Python and native input validation, before opening any SDK owners."""

import dataclasses
import math
import pickle
import unittest
from uuid import UUID, uuid4

from ozzy import (
    BrokerAddress,
    BusyError,
    Checkpoint,
    ClientConfig,
    ClosedError,
    OzzyError,
)
from ozzy._types import destination, message_id, parts


class Values(unittest.TestCase):
    def setUp(self):
        self.config = ClientConfig(
            (BrokerAddress(uuid4(), "tcp://127.0.0.1:12345", "tcp://127.0.0.1:12346"),)
        )

    def test_invalid_config(self):
        for changes in (
            dict(brokers=()),
            dict(brokers=self.config.brokers * 2),
            dict(brokers=self.config.brokers * 3),
            dict(max_partitions=0),
            dict(max_producers=0),
            dict(max_consumers=0),
            dict(max_packet_bytes=0),
            dict(max_record_bytes=0),
            dict(max_records=0),
            dict(max_parts=0),
            dict(reader_queue_messages=0),
            dict(reader_queue_messages=65),
            dict(reader_max_records=0),
            dict(reader_max_parts=0),
            dict(reader_max_records=2**64),
            dict(reader_max_parts=2**64),
            dict(max_partitions=2**64),
            dict(max_producers=2**63),
            dict(request_timeout=0),
            dict(request_timeout=-1),
            dict(request_timeout=math.inf),
            dict(request_timeout=math.nan),
            dict(compression="zstd"),
            dict(compression=""),
            dict(compression="LZ4"),
        ):
            with (
                self.subTest(changes=changes),
                self.assertRaises((ValueError, OverflowError, OzzyError)),
            ):
                dataclasses.replace(self.config, **changes)._native()

    def test_codecs(self):
        for compression in ("raw", "lz4"):
            self.assertIsNotNone(
                dataclasses.replace(self.config, compression=compression)._native()
            )

    def test_invalid_broker(self):
        for broker in (
            BrokerAddress(UUID(int=0), "tcp://127.0.0.1:1", "tcp://127.0.0.1:2"),
            BrokerAddress("invalid", "tcp://127.0.0.1:1", "tcp://127.0.0.1:2"),
            BrokerAddress(uuid4(), "bad://endpoint", "tcp://127.0.0.1:2"),
        ):
            with (
                self.subTest(broker=broker),
                self.assertRaises((ValueError, OzzyError)),
            ):
                dataclasses.replace(self.config, brokers=(broker,))._native()

    def test_limits_before_copy(self):
        small = dataclasses.replace(self.config, max_parts=2, max_record_bytes=4)
        self.assertEqual(parts([b"", b"1234"], small), (b"", b"1234"))
        for payload in ([b""] * 3, b"12345", [b"12", b"345"]):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                parts(payload, small)

    def test_payload_types(self):
        for payload in (
            None,
            1,
            "text",
            {b"a": b"b"},
            iter([b"a"]),
            [None],
            [1],
            ["s"],
        ):
            with self.subTest(payload=payload), self.assertRaises(TypeError):
                parts(payload, self.config)

    def test_byte_shapes(self):
        # Buffers pass through unchanged; Rust copies each once during send.
        for value in (b"", b"\x00\xff", bytearray(b"abc"), memoryview(b"abc")[::2]):
            self.assertIs(parts(value, self.config), value)
        source = bytearray(b"abcd")
        result = parts([source, memoryview(source)], self.config)
        self.assertEqual(tuple(map(bytes, result)), (b"abcd", b"abcd"))

    def test_typed_views_count_bytes(self):
        small = dataclasses.replace(self.config, max_record_bytes=4)
        self.assertEqual(bytes(parts(memoryview(b"1234").cast("I"), small)), b"1234")
        with self.assertRaises(ValueError):
            parts(memoryview(b"12345678").cast("I"), small)

    def test_uuid_formats(self):
        value = uuid4()
        for form in (value, str(value), value.hex, str(value).upper()):
            self.assertEqual(message_id(form), value.bytes)
        self.assertIsNone(message_id(None))
        with self.assertRaises(ValueError):
            message_id("bad")

    def test_destination(self):
        self.assertEqual(destination(b"", None), (b"", None))
        self.assertEqual(destination(None, 0), (None, 0))
        with self.assertRaises(ValueError):
            destination(b"", 0)
        for key in ("text", bytearray(b"key"), memoryview(b"key")):
            with self.assertRaises(TypeError):
                destination(key, None)

    def test_checkpoint(self):
        saved = Checkpoint(str(uuid4()), ((0, 0), (1, 2**64 - 1)))
        self.assertEqual(Checkpoint._from_native(saved._native()), saved)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            saved.positions = ()
        with self.assertRaises(OverflowError):
            Checkpoint(saved.topic_id, ((0, -1),))._native()

    def test_public_exceptions(self):
        for kind in (OzzyError, BusyError, ClosedError):
            error = pickle.loads(pickle.dumps(kind("message")))
            self.assertIsInstance(error, OzzyError)
            self.assertIs(type(error), kind)
            self.assertEqual(error.args, ("message",))


result = unittest.TextTestRunner().run(
    unittest.defaultTestLoader.loadTestsFromTestCase(Values)
)
assert result.wasSuccessful()
