package io.ozzy;

import static java.lang.foreign.ValueLayout.*;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

// Restricted operations stay here; native views never escape their exclusive lease.
@SuppressWarnings("restricted")
final class Fast {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final long[] ADDRESSES = Native.addresses();
    private static final MethodHandle SEND =
            LINKER.downcallHandle(
                    MemorySegment.ofAddress(ADDRESSES[0]),
                    FunctionDescriptor.of(
                            JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_INT,
                            JAVA_LONG, JAVA_LONG, JAVA_INT, ADDRESS),
                    Linker.Option.critical(true));
    private static final FunctionDescriptor POLL =
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS);
    private static final MethodHandle NEXT =
            LINKER.downcallHandle(MemorySegment.ofAddress(ADDRESSES[1]), POLL);
    private static final MethodHandle CONFIRMED =
            LINKER.downcallHandle(MemorySegment.ofAddress(ADDRESSES[2]), POLL);
    private static final ThreadLocal<MemorySegment> SCRATCH =
            ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(128, 8));

    static PendingRecord send(NativeResource owner, ByteBuffer body, SendOptions options) {
        MemorySegment data = MemorySegment.ofBuffer(body);
        MemorySegment key =
                options.key == null ? MemorySegment.NULL : MemorySegment.ofArray(options.key);
        long hi = options.id == null ? 0 : options.id.getMostSignificantBits();
        long lo = options.id == null ? 0 : options.id.getLeastSignificantBits();
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
                                    hi,
                                    lo,
                                    options.id == null ? 1 : 0,
                                    owner.scratch);
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
                Native.release(pointer);
                throw error;
            }
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native send failed", error);
        }
    }

    static Record next(NativeResource owner) {
        try {
            int status = (int) NEXT.invokeExact(owner.pointer(), owner.scratch);
            check(status, owner.scratch);
            if (status == 0) return null;
            long[] words = words(owner.scratch);
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

    static Receipt confirmed(long pointer) {
        MemorySegment scratch = SCRATCH.get();
        try {
            int status = (int) CONFIRMED.invokeExact(pointer, scratch);
            check(status, scratch);
            return status == 0 ? null : new Receipt(words(scratch));
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new OzzyException("native confirmation failed", error);
        }
    }

    private static long[] words(MemorySegment segment) {
        return segment.toArray(JAVA_LONG);
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
