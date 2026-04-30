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

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.disk.ConcurrentTestHelper.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocalDiskAllocator
{
    private static final DataSize MAX_CAPACITY = DataSize.of(1, MEGABYTE);
    private static final long MAX_BYTES = MAX_CAPACITY.toBytes();
    private static final int THREAD_COUNT = 16;

    @TempDir
    private Path tempDir;

    @Test
    public void testCapacityTracksUsage()
    {
        LocalDiskAllocator allocator = createAllocator();
        long chunkSize = 100;

        DiskSpaceLease nearFull = allocator.allocate(MAX_BYTES - chunkSize).orElseThrow();
        DiskSpaceLease full = allocator.allocate(chunkSize).orElseThrow();
        assertThat(allocator.getAllocatedBytes()).isEqualTo(MAX_BYTES);
        assertThat(allocator.allocate(1)).isEmpty();

        full.release();
        assertThat(allocator.getAllocatedBytes()).isEqualTo(MAX_BYTES - chunkSize);
        DiskSpaceLease refill = allocator.allocate(chunkSize).orElseThrow();
        assertThat(allocator.getAllocatedBytes()).isEqualTo(MAX_BYTES);

        nearFull.release();
        refill.release();
        assertThat(allocator.getAllocatedBytes()).isZero();
    }

    @Test
    public void testDoubleReleaseFails()
    {
        LocalDiskAllocator allocator = createAllocator();
        DiskSpaceLease lease = allocator.allocate(100).orElseThrow();
        lease.release();

        assertThatThrownBy(lease::release).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNegativeBytesRejected()
    {
        LocalDiskAllocator allocator = createAllocator();

        assertThatThrownBy(() -> allocator.allocate(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConcurrentAllocateNeverExceedsCapacity()
            throws Exception
    {
        assertThat(MAX_BYTES % THREAD_COUNT).isZero();
        LocalDiskAllocator allocator = createAllocator();
        long allocationPerThread = MAX_BYTES / THREAD_COUNT;
        AtomicInteger successCount = new AtomicInteger();

        runConcurrently(THREAD_COUNT, () -> {
            if (allocator.allocate(allocationPerThread).isPresent()) {
                successCount.incrementAndGet();
            }
        });

        assertThat(successCount.get()).isEqualTo(THREAD_COUNT);
        assertThat(allocator.getAllocatedBytes()).isEqualTo(MAX_BYTES);
    }

    @Test
    public void testConcurrentAllocateExceedingCapacityRejectsCorrectly()
            throws Exception
    {
        LocalDiskAllocator allocator = createAllocator();
        long allocationPerThread = MAX_BYTES / 2;
        int expectedSuccesses = 2;
        AtomicInteger successCount = new AtomicInteger();

        runConcurrently(THREAD_COUNT, () -> {
            if (allocator.allocate(allocationPerThread).isPresent()) {
                successCount.incrementAndGet();
            }
        });

        assertThat(successCount.get()).isEqualTo(expectedSuccesses);
        assertThat(allocator.getAllocatedBytes()).isEqualTo(MAX_BYTES);
    }

    @Test
    public void testConcurrentAllocateAndRelease()
            throws Exception
    {
        LocalDiskAllocator allocator = createAllocator();
        long chunkSize = 64;
        int iterationsPerThread = 100;

        runConcurrently(THREAD_COUNT, () -> {
            for (int j = 0; j < iterationsPerThread; j++) {
                Optional<DiskSpaceLease> lease = allocator.allocate(chunkSize);
                lease.ifPresent(DiskSpaceLease::release);
            }
        });

        assertThat(allocator.getAllocatedBytes()).isZero();
    }

    private LocalDiskAllocator createAllocator()
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(MAX_CAPACITY);
        return new LocalDiskAllocator(config);
    }
}
