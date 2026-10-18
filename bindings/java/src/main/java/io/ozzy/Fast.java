package io.ozzy;

import static java.lang.foreign.ValueLayout.*;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.List;

// Restricted operations stay here; native views never escape their exclusive lease.
@SuppressWarnings("restricted")
final class Fast {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final long[] ADDRESSES = Native.addresses();
    // Critical calls never block or call back into Java; heap inputs stay pinned.
    private static final Linker.Option CRITICAL = Linker.Option.critical(true);
    private static final MethodHandle SEND =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[0]),
                    FunctionDescriptor.of(
                            JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_INT,
                            JAVA_LONG, JAVA_LONG, JAVA_INT, ADDRESS),
                    CRITICAL);
    // Reads may decode a whole frame, so they let the JVM reach safepoints.
    private static final MethodHandle NEXT =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[1]),
                    FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS));
    private static final MethodHandle CONFIRMED =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[2]),
                    FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS),
                    CRITICAL);
    private static final MethodHandle SEND_PARTS =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[3]),
                    FunctionDescriptor.of(
                            JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS,
                            JAVA_LONG, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT, ADDRESS),
                    CRITICAL);
    private static final MethodHandle RELEASE_PENDING =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[4]),
                    FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG),
                    CRITICAL);

    static PendingRecord send(NativeResource owner, ByteBuffer body, SendOptions options) {
        MemorySegment data = MemorySegment.ofBuffer(body);
        MemorySegment key = key(options);
        try {
            int status =
                    (int)
                            SEND.invokeExact(
                                    owner.pointer(),
                                    data,
                                    data.byteSize(),
                                    key,
                                    key.byteSize(),
                                    options.route(),
                                    high(options),
                                    low(options),
                                    options.id == null ? 1 : 0,
                                    owner.scratch);
            return admitted(owner, status);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native send failed", error);
        }
    }

    /** Pack parts into one heap body, copied once more into native storage. */
    static PendingRecord sendParts(
            NativeResource owner, List<ByteBuffer> parts, int total, SendOptions options) {
        byte[] body = new byte[total];
        int[] lengths = new int[parts.size()];
        int offset = 0;
        for (int i = 0; i < lengths.length; i++) {
            ByteBuffer part = parts.get(i);
            int length = part.remaining();
            part.duplicate().get(body, offset, length);
            lengths[i] = length;
            offset += length;
        }
        MemorySegment key = key(options);
        try {
            int status =
                    (int)
                            SEND_PARTS.invokeExact(
                                    owner.pointer(),
                                    MemorySegment.ofArray(body),
                                    (long) body.length,
                                    MemorySegment.ofArray(lengths),
                                    (long) lengths.length,
                                    key,
                                    key.byteSize(),
                                    options.route(),
                                    high(options),
                                    low(options),
                                    options.id == null ? 1 : 0,
                                    owner.scratch);
            return admitted(owner, status);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native send failed", error);
        }
    }

    private static PendingRecord admitted(NativeResource owner, int status) {
        check(status, owner.scratch);
        if (status == 0) return null;
        long pointer = owner.scratch.getAtIndex(JAVA_LONG, 4);
        try {
            return new PendingRecord(
                    owner,
                    pointer,
                    owner.scratch.getAtIndex(JAVA_LONG, 0),
                    owner.scratch.getAtIndex(JAVA_LONG, 1),
                    (int) owner.scratch.getAtIndex(JAVA_LONG, 2),
                    owner.scratch.getAtIndex(JAVA_LONG, 3));
        } catch (Throwable error) {
            releasePending(pointer);
            throw error;
        }
    }

    private static MemorySegment key(SendOptions options) {
        return options.key == null ? MemorySegment.NULL : MemorySegment.ofArray(options.key);
    }

    private static long high(SendOptions options) {
        return options.id == null ? 0 : options.id.getMostSignificantBits();
    }

    private static long low(SendOptions options) {
        return options.id == null ? 0 : options.id.getLeastSignificantBits();
    }

    static Record next(NativeResource owner) {
        try {
            int status = (int) NEXT.invokeExact(owner.pointer(), owner.scratch);
            check(status, owner.scratch);
            if (status == 0) return null;
            long[] words = owner.scratch.toArray(JAVA_LONG);
            int count = Math.toIntExact(words[8]);
            MemorySegment parts =
                    MemorySegment.ofAddress(words[9])
                            .reinterpret(Math.multiplyExact((long) count, 16));
            byte[][] payload = new byte[count][];
            for (int i = 0; i < count; i++) {
                long address = parts.getAtIndex(JAVA_LONG, 2L * i);
                long length = parts.getAtIndex(JAVA_LONG, 2L * i + 1);
                payload[i] =
                        length == 0
                                ? new byte[0]
                                : MemorySegment.ofAddress(address)
                                        .reinterpret(length)
                                        .toArray(JAVA_BYTE);
            }
            return new Record(words, payload);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native receive failed", error);
        }
    }

    /** The receipt keeps the heap words the native call wrote. */
    static Receipt confirmed(long pointer) {
        long[] words = new long[16];
        MemorySegment output = MemorySegment.ofArray(words);
        try {
            int status = (int) CONFIRMED.invokeExact(pointer, output);
            check(status, output);
            return status == 0 ? null : new Receipt(words);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native confirmation failed", error);
        }
    }

    /** Drop owned observer pointers; zero entries are skipped. */
    static void releasePending(long[] pointers, int count) {
        try {
            RELEASE_PENDING.invokeExact(MemorySegment.ofArray(pointers), (long) count);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native release failed", error);
        }
    }

    static void releasePending(long pointer) {
        releasePending(new long[] {pointer}, 1);
    }

    private static void check(int status, MemorySegment result) {
        if (status < 0) {
            int length = result.get(JAVA_INT, 0);
            throw new OzzyException(
                    new String(
                            result.asSlice(4, length).toArray(JAVA_BYTE),
                            java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private Fast() {}
}
