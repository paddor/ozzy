package io.ozzy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

final class Native {
    static {
        if (!Boolean.getBoolean("ozzy.native.registered")) load();
    }

    private static void load() {
        String path = System.getProperty("ozzy.native.path");
        if (path != null) {
            System.load(Path.of(path).toAbsolutePath().toString());
            return;
        }
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (!os.contains("linux"))
            throw new UnsupportedOperationException("Ozzy Java requires Linux");
        String arch = System.getProperty("os.arch");
        String platform =
                "linux-"
                        + (arch.equals("amd64")
                                ? "x86_64"
                                : arch.equals("arm64") ? "aarch64" : arch);
        String name = System.mapLibraryName("ozzy_java");
        try (InputStream resource =
                Native.class.getResourceAsStream("/io/ozzy/native/" + platform + "/" + name)) {
            if (resource == null) {
                System.loadLibrary("ozzy_java");
                return;
            }
            Path extracted = Files.createTempFile("ozzy-java-", "-" + name);
            try {
                Files.copy(resource, extracted, StandardCopyOption.REPLACE_EXISTING);
                System.load(extracted.toAbsolutePath().toString());
            } finally {
                // Linux keeps the mapping alive. Unlink now so abrupt exit leaves no file.
                Files.deleteIfExists(extracted);
            }
        } catch (IOException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    static native long open(
            NativeFuture<?> future, String[] endpoints, long[] ids, long[] settings);

    static native long start(
            long handle,
            int operation,
            NativeFuture<?> future,
            String topic,
            long[] options,
            byte[][] parts,
            byte[] key,
            long[] id,
            byte[] identity);

    static native void cancel(long token);

    static native void release(long handle);

    static native long[] addresses();

    static native long[] inspect(long handle, int kind);

    static native byte[] identity(long handle);

    static Object wrap(
            int kind,
            long handle,
            long[] metadata,
            byte[][] payload,
            ClientConfig config,
            NativeResource owner) {
        try {
            return switch (kind) {
                case 0 -> null;
                case 1 -> new Client(handle, config);
                case 2 -> new Producer(handle, config);
                case 3 -> new Consumer(handle);
                case 4 ->
                        new PendingRecord(
                                owner,
                                handle,
                                metadata[0],
                                metadata[1],
                                (int) metadata[2],
                                metadata[3]);
                case 5 -> new Receipt(metadata);
                case 6 -> new Record(metadata, payload);
                default -> throw new OzzyException("invalid native result");
            };
        } catch (Throwable error) {
            if (handle != 0) release(handle);
            throw error;
        }
    }

    static void discard(Object value) {
        if (value instanceof Client c) c.resource.discard();
        else if (value instanceof Producer p) p.resource.discard();
        else if (value instanceof Consumer c) c.resource.discard();
    }

    private Native() {}
}
