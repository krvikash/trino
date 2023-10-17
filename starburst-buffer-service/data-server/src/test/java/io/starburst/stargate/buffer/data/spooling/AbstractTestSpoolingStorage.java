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
import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.SpooledChunkMapByExchange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.decodeMetadataSlice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestSpoolingStorage
{
    private static final String EXCHANGE_ID = "exchange-0";
    private static final long BUFFER_NODE_ID = 1L;
    private static final long CHUNK_ID_0 = 0L;
    private static final Random random = new Random();

    private SpoolingStorage spoolingStorage;
    private SpooledChunkReader spooledChunkReader;

    @BeforeAll
    public void init()
    {
        spoolingStorage = createSpoolingStorage();
        spooledChunkReader = createSpooledChunkReader();
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        if (spoolingStorage != null) {
            spoolingStorage.close();
            spoolingStorage = null;
        }
        if (spooledChunkReader != null) {
            spooledChunkReader.close();
            spooledChunkReader = null;
        }
    }

    protected abstract SpoolingStorage createSpoolingStorage();

    protected abstract SpooledChunkReader createSpooledChunkReader();

    @Test
    public void testHappyPathChunkSpoolMergeDisabled()
    {
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 1, utf8Slice("Spooling")),
                new DataPage(1, 0, utf8Slice("Storage")));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(dataPages)));
        // verify we can read a single chunk multiple times
        for (int i = 0; i < 2; ++i) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                    .containsExactlyElementsOf(dataPages);
        }
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));

        // verify file is actually removed
        assertThatThrownBy(() -> spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))
                .isInstanceOf(DataServerException.class)
                .hasMessage("No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0));
    }

    @Test
    public void testHappyPathChunkSpoolMergeEnabled()
    {
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 1, utf8Slice("Spooling")),
                new DataPage(1, 0, utf8Slice("Storage")));
        ChunkDataLease chunkDataLease = toChunkDataLease(dataPages);
        Map<Long, SpooledChunk> spooledChunkMap = getFutureValue(spoolingStorage.writeMergedChunks(
                BUFFER_NODE_ID,
                EXCHANGE_ID,
                ImmutableMap.of(new Chunk(CHUNK_ID_0), chunkDataLease),
                chunkDataLease.serializedSizeInBytes()));
        // verify we can read a single chunk multiple times
        for (int i = 0; i < 2; ++i) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spooledChunkMap.get(CHUNK_ID_0)))).containsExactlyElementsOf(dataPages);
        }
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
        // verify file is actually removed (we use spoolingStorage.getSpooledChunk with chunkSpoolMergeEnabled = false which will directly look up the file)
        assertThatThrownBy(() -> spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))
                .isInstanceOf(DataServerException.class)
                .hasMessage("No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0));
    }

    @Test
    public void testOverwritingLargeChunksChunkSpoolMergeDisabled()
    {
        String randomLongString1 = getRandomLargeString();
        String randomLongString2 = getRandomLargeString();
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString1)),
                new DataPage(1, 1, utf8Slice(randomLongString2)));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(dataPages)));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                .containsExactlyElementsOf(dataPages);

        // overwriting with order reversed
        List<DataPage> reversedDataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString2)),
                new DataPage(1, 1, utf8Slice(randomLongString1)));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(reversedDataPages)));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                .containsExactlyElementsOf(reversedDataPages);

        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
    }

    @Test
    public void testOverwritingLargeChunksChunkSpoolMergeEnabled()
    {
        String randomLongString1 = getRandomLargeString();
        String randomLongString2 = getRandomLargeString();
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString1)),
                new DataPage(1, 1, utf8Slice(randomLongString2)));
        ChunkDataLease chunkDataLease = toChunkDataLease(dataPages);
        Map<Long, SpooledChunk> spooledChunkMap = getFutureValue(spoolingStorage.writeMergedChunks(
                BUFFER_NODE_ID,
                EXCHANGE_ID,
                ImmutableMap.of(new Chunk(CHUNK_ID_0), chunkDataLease),
                chunkDataLease.serializedSizeInBytes()));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spooledChunkMap.get(CHUNK_ID_0)))).containsExactlyElementsOf(dataPages);

        // overwriting with order reversed
        List<DataPage> reversedDataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString2)),
                new DataPage(1, 1, utf8Slice(randomLongString1)));
        ChunkDataLease reversedChunkDataLease = toChunkDataLease(reversedDataPages);
        spooledChunkMap = getFutureValue(spoolingStorage.writeMergedChunks(
                BUFFER_NODE_ID,
                EXCHANGE_ID,
                ImmutableMap.of(new Chunk(CHUNK_ID_0), reversedChunkDataLease),
                chunkDataLease.serializedSizeInBytes()));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spooledChunkMap.get(CHUNK_ID_0)))).containsExactlyElementsOf(reversedDataPages);

        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
    }

    @Test
    public void testReadWriteManyChunksChunkSpoolMergeDisabled()
    {
        long numChunks = 32L;
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, chunkId,
                    toChunkDataLease(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId)))))));
        }
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, chunkId))))
                    .containsExactlyElementsOf(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId)))));
        }

        assertEquals(32, spoolingStorage.getSpooledChunksCount());
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
        assertEquals(0, spoolingStorage.getSpooledChunksCount());

        // verify spooling files are removed
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            long finalChunkId = chunkId;
            assertThatThrownBy(() -> spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, finalChunkId))
                    .isInstanceOf(DataServerException.class)
                    .hasMessage("No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(BUFFER_NODE_ID, EXCHANGE_ID, chunkId));
        }
    }

    @Test
    public void testReadWriteManyChunksChunkSpoolMergeEnabled()
    {
        long numChunks = 32L;
        ImmutableMap.Builder<Chunk, ChunkDataLease> chunkDataLeaseMapBuilder = ImmutableMap.builder();
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            chunkDataLeaseMapBuilder.put(new Chunk(chunkId), toChunkDataLease(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId))))));
        }
        Map<Chunk, ChunkDataLease> chunkDataLeaseMap = chunkDataLeaseMapBuilder.build();
        long contentLength = chunkDataLeaseMap.values().stream().mapToInt(ChunkDataLease::serializedSizeInBytes).sum();
        Map<Long, SpooledChunk> spooledChunkMap = getFutureValue(spoolingStorage.writeMergedChunks(BUFFER_NODE_ID, EXCHANGE_ID, chunkDataLeaseMap, contentLength));
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spooledChunkMap.get(chunkId))))
                    .containsExactlyElementsOf(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId)))));
        }

        assertEquals(32, spooledChunkMap.size());
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));

        // verify spooling files are removed
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            long finalChunkId = chunkId;
            // (we use spoolingStorage.getSpooledChunk with chunkSpoolMergeEnabled = false which will directly look up the file)
            assertThatThrownBy(() -> spoolingStorage.getSpooledChunk(BUFFER_NODE_ID, EXCHANGE_ID, finalChunkId))
                    .isInstanceOf(DataServerException.class)
                    .hasMessage("No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(BUFFER_NODE_ID, EXCHANGE_ID, chunkId));
        }
    }

    @Test
    public void testWriteReadMetadataFile()
    {
        SpooledChunkMapByExchange spooledChunkMapByExchange = new SpooledChunkMapByExchange();
        Map<Long, SpooledChunk> expectedSpooledChunkMap = new HashMap<>();
        expectedSpooledChunkMap.put(0L, new SpooledChunk("location", 0L, 10));
        expectedSpooledChunkMap.put(1L, new SpooledChunk("location", 10L, 20));
        expectedSpooledChunkMap.put(2L, new SpooledChunk("location", 30L, 30));
        expectedSpooledChunkMap.put(3L, new SpooledChunk("anotherlocation", 0L, 88));
        spooledChunkMapByExchange.update(EXCHANGE_ID, expectedSpooledChunkMap);
        getFutureValue(spoolingStorage.writeMetadataFile(BUFFER_NODE_ID, spooledChunkMapByExchange.encodeMetadataSlice()));

        assertEquals(expectedSpooledChunkMap, decodeMetadataSlice(getFutureValue(spoolingStorage.readMetadataFile(BUFFER_NODE_ID))));
    }

    private static String getRandomLargeString()
    {
        byte[] array = new byte[5_000_000];
        random.nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}
