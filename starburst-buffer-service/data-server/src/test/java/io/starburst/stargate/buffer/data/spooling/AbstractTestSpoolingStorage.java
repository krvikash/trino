/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestSpoolingStorage
{
    private static final String EXCHANGE_ID = "exchange-0";
    private static final long BUFFER_NODE_ID = 1L;
    private static final long CHUNK_ID_0 = 0L;
    private static final Random random = new Random();

    private SpoolingStorage spoolingStorage;

    @BeforeAll
    public void init()
    {
        spoolingStorage = createSpoolingStorage();
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        if (spoolingStorage != null) {
            spoolingStorage.close();
            spoolingStorage = null;
        }
    }

    protected abstract SpoolingStorage createSpoolingStorage();

    @Test
    public void testHappyPath()
    {
        getFutureValue(spoolingStorage.writeChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID, new ChunkDataHolder(
                ImmutableList.of(utf8Slice("test"), utf8Slice("Spooling"), utf8Slice("Storage")),
                NO_CHECKSUM,
                3)));
        // verify we can read a single chunk multiple times
        for (int i = 0; i < 2; ++i) {
            ChunkDataLease chunkDataLease = spoolingStorage.readChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID);
            assertEquals(new ChunkDataHolder(
                    ImmutableList.of(utf8Slice("testSpoolingStorage")),
                    NO_CHECKSUM,
                    3), getFutureValue(chunkDataLease.getChunkDataHolderFuture()));
            chunkDataLease.release();
        }
        getFutureValue(spoolingStorage.removeExchange(EXCHANGE_ID));

        // verify file is actually removed
        assertThrows(RuntimeException.class, () -> spoolingStorage.readChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID));
    }

    @Test
    public void testOverwritingLargeChunks()
    {
        String randomLongString1 = getRandomLargeString();
        String randomLongString2 = getRandomLargeString();
        getFutureValue(spoolingStorage.writeChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID, new ChunkDataHolder(
                ImmutableList.of(utf8Slice(randomLongString1), utf8Slice(randomLongString2)),
                NO_CHECKSUM,
                2)));
        ChunkDataLease chunkDataLease = spoolingStorage.readChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID);
        assertEquals(new ChunkDataHolder(
                ImmutableList.of(utf8Slice(randomLongString1 + randomLongString2)),
                NO_CHECKSUM,
                2), getFutureValue(chunkDataLease.getChunkDataHolderFuture()));
        chunkDataLease.release();

        // overwriting with order reversed
        getFutureValue(spoolingStorage.writeChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID, new ChunkDataHolder(
                ImmutableList.of(utf8Slice(randomLongString2), utf8Slice(randomLongString1)),
                NO_CHECKSUM,
                2)));
        chunkDataLease = spoolingStorage.readChunk(EXCHANGE_ID, CHUNK_ID_0, BUFFER_NODE_ID);
        assertEquals(new ChunkDataHolder(
                ImmutableList.of(utf8Slice(randomLongString2 + randomLongString1)),
                NO_CHECKSUM,
                2), getFutureValue(chunkDataLease.getChunkDataHolderFuture()));
        chunkDataLease.release();

        getFutureValue(spoolingStorage.removeExchange(EXCHANGE_ID));
    }

    @Test
    public void testReadWriteManyChunks()
    {
        long numChunks = 32L;
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            getFutureValue(spoolingStorage.writeChunk(EXCHANGE_ID, chunkId, BUFFER_NODE_ID, new ChunkDataHolder(
                    ImmutableList.of(utf8Slice(String.valueOf(chunkId))),
                    NO_CHECKSUM,
                    1)));
        }
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            ChunkDataLease chunkDataLease = spoolingStorage.readChunk(EXCHANGE_ID, chunkId, BUFFER_NODE_ID);
            assertEquals(new ChunkDataHolder(
                    ImmutableList.of(utf8Slice(String.valueOf(chunkId))),
                    NO_CHECKSUM,
                    1), getFutureValue(chunkDataLease.getChunkDataHolderFuture()));
            chunkDataLease.release();
        }

        getFutureValue(spoolingStorage.removeExchange(EXCHANGE_ID));

        // verify spooling files are removed
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            long finalChunkId = chunkId;
            assertThatThrownBy(() -> getFutureValue(spoolingStorage.readChunk(EXCHANGE_ID, finalChunkId, BUFFER_NODE_ID).getChunkDataHolderFuture()))
                    .isInstanceOf(DataServerException.class)
                    .hasMessage("No closed chunk found for exchange %s, chunk %d, bufferNodeId %d".formatted(EXCHANGE_ID, chunkId, BUFFER_NODE_ID));
        }
    }

    private static String getRandomLargeString()
    {
        byte[] array = new byte[5_000_000];
        random.nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}
