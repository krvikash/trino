/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.verifyChunkData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestChunk
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testHappyPath()
    {
        MemoryAllocator memoryAllocator = new MemoryAllocator(new MemoryAllocatorConfig(), new ChunkManagerConfig(), new DataServerStats());
        Chunk chunk = new Chunk(
                0L,
                "exchange-id",
                0,
                0,
                memoryAllocator,
                executor,
                35,
                7,
                true);

        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("333")),
                new DataPage(0, 0, utf8Slice("4444")),
                new DataPage(1, 0, utf8Slice("55555")));
        DataPage extraDataPage = new DataPage(1, 0, utf8Slice("666666"));

        for (DataPage dataPage : dataPages) {
            assertTrue(chunk.hasEnoughSpace(dataPage.data()));
            getFutureValue(chunk.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data()));
        }
        assertFalse(chunk.hasEnoughSpace(extraDataPage.data()));
        chunk.close();

        assertEquals(12, chunk.dataSizeInBytes());
        verifyChunkData(chunk.getChunkData(), dataPages.get(0), dataPages.get(1), dataPages.get(2));
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
