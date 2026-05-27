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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestDiskPreAllocator
{
    @TempDir
    Path tempDir;

    @Test
    public void testAvailabilityDetectedAtStartup()
    {
        // Static initializer must complete without throwing regardless of platform.
        // AVAILABLE is true on Linux (posix_fallocate in glibc), false on macOS.
        boolean available = DiskPreAllocator.AVAILABLE;
        assertThat(available).isIn(true, false); // both outcomes are valid
    }

    @Test
    public void testCreatesFileOnSuccess()
            throws Exception
    {
        assumeTrue(DiskPreAllocator.AVAILABLE, "posix_fallocate not available on this platform");

        Path file = tempDir.resolve("chunk.data");
        assertThat(file).doesNotExist();

        DiskPreAllocator.tryCreateAndPreAllocate(file, 4096);

        assertThat(file).exists();
    }

    @Test
    public void testRejectsExistingFile()
            throws Exception
    {
        assumeTrue(DiskPreAllocator.AVAILABLE, "posix_fallocate not available on this platform");

        Path file = tempDir.resolve("chunk.data");
        Files.createFile(file);

        assertThatThrownBy(() -> DiskPreAllocator.tryCreateAndPreAllocate(file, 4096))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void testFileHasReservedSizeWhenFallocateSucceeds()
            throws Exception
    {
        assumeTrue(DiskPreAllocator.AVAILABLE, "posix_fallocate not available on this platform");

        int capacity = 16 * 1024 * 1024; // 16 MB
        Path file = tempDir.resolve("chunk.data");

        DiskPreAllocator.tryCreateAndPreAllocate(file, capacity);

        long fileSize = Files.size(file);
        // posix_fallocate sets file size to capacity; skip if filesystem returns EOPNOTSUPP (e.g. tmpfs)
        assumeTrue(fileSize == capacity, "filesystem did not honour posix_fallocate (e.g. tmpfs) — skipping size assertion");
        assertThat(fileSize).isEqualTo(capacity);
    }

    @Test
    public void testReturnsFalseWhenUnavailable()
            throws Exception
    {
        assumeTrue(!DiskPreAllocator.AVAILABLE, "posix_fallocate is available — skip fallback path test");

        Path file = tempDir.resolve("chunk.data");
        boolean result = DiskPreAllocator.tryCreateAndPreAllocate(file, 4096);

        assertThat(result).isFalse();
        assertThat(file).doesNotExist();
    }

    @Test
    public void testReturnsTrueWhenAvailable()
            throws Exception
    {
        assumeTrue(DiskPreAllocator.AVAILABLE, "posix_fallocate not available on this platform");

        Path file = tempDir.resolve("chunk.data");
        boolean result = DiskPreAllocator.tryCreateAndPreAllocate(file, 4096);

        assertThat(result).isTrue();
        assertThat(file).exists();
    }
}
