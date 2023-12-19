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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMemoryAllocator
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testPendingAllocations()
    {
        long maxBytes = 100L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig().setHeapHeadroom(DataSize.succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes)),
                new ChunkManagerConfig(),
                new DataServerStats());
        assertEquals(100L, memoryAllocator.getFreeMemory());

        ListenableFuture<Slice> sliceFuture1 = memoryAllocator.allocate(50);
        assertTrue(sliceFuture1.isDone());

        ListenableFuture<Slice> sliceFuture2 = memoryAllocator.allocate(40);
        assertTrue(sliceFuture2.isDone());

        ListenableFuture<Slice> sliceFuture3 = memoryAllocator.allocate(30);
        assertFalse(sliceFuture3.isDone());

        ListenableFuture<Slice> sliceFuture4 = memoryAllocator.allocate(20);
        assertFalse(sliceFuture4.isDone());

        ListenableFuture<Slice> sliceFuture5 = memoryAllocator.allocate(10);
        assertTrue(sliceFuture5.isDone());

        memoryAllocator.release(getFutureValue(sliceFuture5));
        assertEquals(10L, memoryAllocator.getFreeMemory());
        assertFalse(sliceFuture3.isDone());
        assertFalse(sliceFuture4.isDone());

        ListenableFuture<Slice> sliceFuture6 = memoryAllocator.allocate(25);
        assertFalse(sliceFuture6.isDone());

        sliceFuture4.cancel(true);
        memoryAllocator.release(getFutureValue(sliceFuture2));
        // allocation should happen in FIFO order
        assertTrue(sliceFuture3.isDone());
        assertFalse(sliceFuture6.isDone());
        assertEquals(20L, memoryAllocator.getFreeMemory());

        memoryAllocator.release(getFutureValue(sliceFuture1));
        assertTrue(sliceFuture6.isDone());
        assertEquals(45L, memoryAllocator.getFreeMemory());

        memoryAllocator.release(getFutureValue(sliceFuture3));
        memoryAllocator.release(getFutureValue(sliceFuture6));
        assertEquals(100L, memoryAllocator.getFreeMemory());
    }

    @Test
    public void testMemoryPooling()
    {
        long maxBytes = DataSize.of(1000, KILOBYTE).toBytes();
        DataSize chunkSliceSize = DataSize.of(1, KILOBYTE);
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig()
                        .setHeapHeadroom(DataSize.succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes))
                        .setChunkSlicePoolingFraction(0.8),
                new ChunkManagerConfig().setChunkSliceSize(chunkSliceSize),
                new DataServerStats());

        for (int i = 0; i < 10_000_000; ++i) {
            ListenableFuture<Slice> sliceFuture1 = memoryAllocator.allocate(toIntExact(chunkSliceSize.toBytes()));
            assertTrue(sliceFuture1.isDone());

            ListenableFuture<Slice> sliceFuture2 = memoryAllocator.allocate(1);
            assertTrue(sliceFuture2.isDone());

            memoryAllocator.release(getFutureValue(sliceFuture1));
            memoryAllocator.release(getFutureValue(sliceFuture2));
        }
        assertEquals(1, memoryAllocator.getChunkSlicePoolSize());

        ImmutableList.Builder<ListenableFuture<Slice>> sliceFutures = ImmutableList.builder();
        for (int i = 0; i < 1000; ++i) {
            ListenableFuture<Slice> sliceFuture = memoryAllocator.allocate(toIntExact(chunkSliceSize.toBytes()));
            assertTrue(sliceFuture.isDone());
            sliceFutures.add(sliceFuture);
        }
        sliceFutures.build().forEach(sliceFuture -> memoryAllocator.release(getFutureValue(sliceFuture)));
        assertEquals(800, memoryAllocator.getChunkSlicePoolSize());
    }

    @Test
    public void testReferenceCount()
    {
        MemoryAllocator memoryAllocator = new MemoryAllocator(new MemoryAllocatorConfig(), new ChunkManagerConfig(), new DataServerStats());
        DataPage dataPage = new DataPage(0, 0, utf8Slice("dummy"));
        int chunkTargetSizeInBytes = toIntExact(DataSize.of(16, MEGABYTE).toBytes());
        int chunkSliceSizeInBytes = toIntExact(DataSize.of(128, KILOBYTE).toBytes());

        Chunk chunk0 = new Chunk(
                0L,
                "exchange-id",
                0,
                0,
                memoryAllocator,
                executor,
                chunkTargetSizeInBytes,
                chunkSliceSizeInBytes,
                true);
        getFutureValue(chunk0.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data()));
        chunk0.close();

        ChunkDataLease chunkDataLease0 = chunk0.getChunkDataLease();
        chunk0.release();
        assertEquals(0, memoryAllocator.getChunkSlicePoolSize());
        assertEquals(chunkSliceSizeInBytes, memoryAllocator.getAllocatedMemory());

        chunkDataLease0.release();
        assertEquals(1, memoryAllocator.getChunkSlicePoolSize());
        assertEquals(0, memoryAllocator.getAllocatedMemory());

        Chunk chunk1 = new Chunk(
                1L,
                "exchange-id",
                1,
                1,
                memoryAllocator,
                executor,
                chunkTargetSizeInBytes,
                chunkSliceSizeInBytes,
                true);
        getFutureValue(chunk1.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data()));
        chunk1.close();

        ChunkDataLease chunkDataLease1 = chunk1.getChunkDataLease();
        chunkDataLease1.release();
        assertEquals(0, memoryAllocator.getChunkSlicePoolSize());
        assertEquals(chunkSliceSizeInBytes, memoryAllocator.getAllocatedMemory());

        chunk1.release();
        assertEquals(1, memoryAllocator.getChunkSlicePoolSize());
        assertEquals(0, memoryAllocator.getAllocatedMemory());
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
