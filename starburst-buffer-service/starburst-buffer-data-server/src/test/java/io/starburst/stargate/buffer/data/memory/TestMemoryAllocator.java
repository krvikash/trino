/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.MemoryChunkData;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMemoryAllocator
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testPendingAllocations()
    {
        long maxBytes = 100L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
        assertThat((Object) memoryAllocator.getFreeMemory()).isEqualTo(100L);

        ListenableFuture<Slice> sliceFuture1 = memoryAllocator.allocate(50);
        assertThat(sliceFuture1.isDone()).isTrue();

        ListenableFuture<Slice> sliceFuture2 = memoryAllocator.allocate(40);
        assertThat(sliceFuture2.isDone()).isTrue();

        ListenableFuture<Slice> sliceFuture3 = memoryAllocator.allocate(30);
        assertThat(sliceFuture3.isDone()).isFalse();

        ListenableFuture<Slice> sliceFuture4 = memoryAllocator.allocate(20);
        assertThat(sliceFuture4.isDone()).isFalse();

        ListenableFuture<Slice> sliceFuture5 = memoryAllocator.allocate(10);
        assertThat(sliceFuture5.isDone()).isTrue();

        memoryAllocator.release(getFutureValue(sliceFuture5));
        assertThat((Object) memoryAllocator.getFreeMemory()).isEqualTo(10L);
        assertThat(sliceFuture3.isDone()).isFalse();
        assertThat(sliceFuture4.isDone()).isFalse();

        ListenableFuture<Slice> sliceFuture6 = memoryAllocator.allocate(25);
        assertThat(sliceFuture6.isDone()).isFalse();

        sliceFuture4.cancel(true);
        memoryAllocator.release(getFutureValue(sliceFuture2));
        // allocation should happen in FIFO order
        assertThat(sliceFuture3.isDone()).isTrue();
        assertThat(sliceFuture6.isDone()).isFalse();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(20L);

        memoryAllocator.release(getFutureValue(sliceFuture1));
        assertThat(sliceFuture6.isDone()).isTrue();
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(45L);

        memoryAllocator.release(getFutureValue(sliceFuture3));
        memoryAllocator.release(getFutureValue(sliceFuture6));
        assertThat(memoryAllocator.getFreeMemory()).isEqualTo(100L);
    }

    @Test
    public void testMemoryPooling()
    {
        long maxBytes = DataSize.of(1000, KILOBYTE).toBytes();
        DataSize chunkSliceSize = DataSize.of(1, KILOBYTE);
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.ofBytes(maxBytes)),
                new MemoryAllocatorConfig()
                        .setChunkSlicePoolingFraction(0.8),
                new ChunkManagerConfig().setChunkSliceSize(chunkSliceSize),
                new DataServerStats());

        for (int i = 0; i < 10_000_000; ++i) {
            ListenableFuture<Slice> sliceFuture1 = memoryAllocator.allocate(toIntExact(chunkSliceSize.toBytes()));
            assertThat(sliceFuture1.isDone()).isTrue();

            ListenableFuture<Slice> sliceFuture2 = memoryAllocator.allocate(1);
            assertThat(sliceFuture2.isDone()).isTrue();

            memoryAllocator.release(getFutureValue(sliceFuture1));
            memoryAllocator.release(getFutureValue(sliceFuture2));
        }
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(1);

        ImmutableList.Builder<ListenableFuture<Slice>> sliceFutures = ImmutableList.builder();
        for (int i = 0; i < 1000; ++i) {
            ListenableFuture<Slice> sliceFuture = memoryAllocator.allocate(toIntExact(chunkSliceSize.toBytes()));
            assertThat(sliceFuture.isDone()).isTrue();
            sliceFutures.add(sliceFuture);
        }
        sliceFutures.build().forEach(sliceFuture -> memoryAllocator.release(getFutureValue(sliceFuture)));
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(800);
    }

    @Test
    public void testReferenceCount()
    {
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new TestingMemoryConfig(DataSize.of(64, MEGABYTE)),
                new MemoryAllocatorConfig(),
                new ChunkManagerConfig(),
                new DataServerStats());
        DataPage dataPage = new DataPage(0, 0, utf8Slice("dummy"));
        int chunkTargetSizeInBytes = toIntExact(DataSize.of(16, MEGABYTE).toBytes());
        int chunkSliceSizeInBytes = toIntExact(DataSize.of(128, KILOBYTE).toBytes());

        Chunk chunk0 = new Chunk(
                0L,
                "exchange-id",
                0,
                0,
                new MemoryChunkData(memoryAllocator, executor, chunkTargetSizeInBytes, chunkSliceSizeInBytes, true));
        getFutureValue(chunk0.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data()));
        chunk0.close();

        ChunkDataLease chunkDataLease0 = chunk0.getChunkDataLease();
        chunk0.release();
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(0);
        assertThat(memoryAllocator.getAllocatedMemory()).isEqualTo(chunkSliceSizeInBytes);

        chunkDataLease0.release();
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(1);
        assertThat(memoryAllocator.getAllocatedMemory()).isEqualTo(0);

        Chunk chunk1 = new Chunk(
                1L,
                "exchange-id",
                1,
                1,
                new MemoryChunkData(memoryAllocator, executor, chunkTargetSizeInBytes, chunkSliceSizeInBytes, true));
        getFutureValue(chunk1.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data()));
        chunk1.close();

        ChunkDataLease chunkDataLease1 = chunk1.getChunkDataLease();
        chunkDataLease1.release();
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(0);
        assertThat(memoryAllocator.getAllocatedMemory()).isEqualTo(chunkSliceSizeInBytes);

        chunk1.release();
        assertThat(memoryAllocator.getChunkSlicePoolSize()).isEqualTo(1);
        assertThat(memoryAllocator.getAllocatedMemory()).isEqualTo(0);
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
