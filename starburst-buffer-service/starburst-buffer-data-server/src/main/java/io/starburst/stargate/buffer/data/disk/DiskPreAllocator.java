/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import io.airlift.log.Logger;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Reserves disk extents for chunk files via {@code posix_fallocate(3)} without writing data.
 * Falls back to lazy OS allocation when {@code posix_fallocate} is unavailable (macOS, tmpfs).
 */
public final class DiskPreAllocator
{
    private static final Logger log = Logger.get(DiskPreAllocator.class);

    // open(2) flags: O_WRONLY | O_CREAT | O_EXCL.
    // These are Linux-ABI numeric values; macOS/BSD define different bits. Relying on them is safe
    // only because AVAILABLE is true exclusively on platforms where the posix_fallocate symbol
    // lookup below succeeds (Linux/glibc) — macOS lacks posix_fallocate and falls back instead.
    private static final int OPEN_FLAGS = 0x1 | 0x40 | 0x80;
    private static final int FILE_MODE = 384; // 0600 octal — rw-------

    private static final MethodHandle OPEN;
    private static final MethodHandle POSIX_FALLOCATE;
    private static final MethodHandle CLOSE;
    public static final boolean AVAILABLE;

    static {
        MethodHandle openHandle = null;
        MethodHandle fallocateHandle = null;
        MethodHandle closeHandle = null;
        boolean available = false;
        try {
            Linker linker = Linker.nativeLinker();
            SymbolLookup stdlib = linker.defaultLookup();
            openHandle = linker.downcallHandle(
                    stdlib.find("open").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            fallocateHandle = linker.downcallHandle(
                    stdlib.find("posix_fallocate").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
            closeHandle = linker.downcallHandle(
                    stdlib.find("close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));
            available = true;
        }
        catch (Exception e) {
            log.warn("posix_fallocate unavailable, disk chunks will use lazy extent allocation: %s", e.getMessage());
        }
        OPEN = openHandle;
        POSIX_FALLOCATE = fallocateHandle;
        CLOSE = closeHandle;
        AVAILABLE = available;
    }

    private DiskPreAllocator() {}

    /**
     * Creates a new file at {@code file} and reserves {@code sizeInBytes} bytes of disk extents
     * via {@code posix_fallocate} without writing data.
     *
     * <p>On success the file exists on disk and the caller must open it with
     * {@link java.nio.file.StandardOpenOption#WRITE} (not {@code CREATE_NEW}).
     *
     * @return {@code true} if the file was created and extents reserved; {@code false} if
     *         {@code posix_fallocate} is unavailable — caller should use {@code CREATE_NEW} instead
     * @throws IOException if the file cannot be created (e.g. path already exists)
     */
    public static boolean tryCreateAndPreAllocate(Path file, long sizeInBytes)
            throws IOException
    {
        if (!AVAILABLE) {
            return false;
        }
        try (Arena arena = Arena.ofConfined()) {
            int fileDescriptor = (int) OPEN.invokeExact(arena.allocateFrom(file.toString()), OPEN_FLAGS, FILE_MODE);
            if (fileDescriptor < 0) {
                throw new IOException("open() failed for chunk file " + file + " (returned " + fileDescriptor + ")");
            }
            try {
                int errno = (int) POSIX_FALLOCATE.invokeExact(fileDescriptor, 0L, sizeInBytes);
                if (errno != 0) {
                    // File exists and is writable, OS will allocate extents lazily
                    log.warn("posix_fallocate returned errno=%d for %s, continuing with lazy extent allocation", errno, file);
                }
            }
            finally {
                CLOSE.invokeExact(fileDescriptor);
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (Throwable t) {
            throw new IOException("Pre-allocation failed for chunk file " + file, t);
        }
        return true;
    }
}
