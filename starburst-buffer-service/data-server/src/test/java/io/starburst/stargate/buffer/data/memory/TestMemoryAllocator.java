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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMemoryAllocator
{
    @Test
    public void testPendingAllocations()
    {
        long maxBytes = 100L;
        MemoryAllocator memoryAllocator = new MemoryAllocator(
                new MemoryAllocatorConfig().setHeapHeadroom(DataSize.succinctBytes(Runtime.getRuntime().maxMemory() - maxBytes)),
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
}
