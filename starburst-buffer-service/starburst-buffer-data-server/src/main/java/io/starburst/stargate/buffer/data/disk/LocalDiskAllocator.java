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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class LocalDiskAllocator
{
    private static final long DISK_SPACE_CACHE_SECONDS = 1;

    private final long capacityBytes;
    private final AtomicLong allocatedBytes = new AtomicLong();
    private final Supplier<Long> availableDiskSpaceBytes;

    @Inject
    public LocalDiskAllocator(LocalDiskTierConfig config)
    {
        requireNonNull(config, "config is null");
        this.capacityBytes = requireNonNull(config.getCapacity(), "capacity is null").toBytes();
        Path directory = requireNonNull(config.getDirectory(), "directory is null");
        this.availableDiskSpaceBytes = Suppliers.memoizeWithExpiration(
                () -> Math.min(readDiskUsableSpace(directory), capacityBytes),
                DISK_SPACE_CACHE_SECONDS,
                SECONDS);
    }

    public Optional<DiskSpaceLease> allocate(long bytes)
    {
        checkArgument(bytes >= 0, "bytes must be non-negative");
        long usableSpace = availableDiskSpaceBytes.get();
        while (true) {
            long current = allocatedBytes.get();
            if (!hasSpace(current, bytes, usableSpace)) {
                return Optional.empty();
            }
            if (allocatedBytes.compareAndSet(current, current + bytes)) {
                return Optional.of(new DiskSpaceLease(this, bytes));
            }
        }
    }

    private boolean hasSpace(long current, long requested, long usableSpace)
    {
        return usableSpace >= current + requested;
    }

    void release(long bytes)
    {
        checkArgument(bytes >= 0, "bytes must be non-negative");
        long updated = allocatedBytes.addAndGet(-bytes);
        verify(updated >= 0, "allocatedBytes (%s) < release amount (%s)", updated + bytes, bytes);
    }

    @VisibleForTesting
    long getAllocatedBytes()
    {
        return allocatedBytes.get();
    }

    private static long readDiskUsableSpace(Path directory)
    {
        try {
            return Files.getFileStore(directory).getUsableSpace();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to check available disk space", e);
        }
    }
}
