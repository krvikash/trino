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
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;
import static org.assertj.core.api.Assertions.assertThat;
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
    public void testHappyPath()
    {
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 1, utf8Slice("Spooling")),
                new DataPage(1, 0, utf8Slice("Storage")));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(dataPages)));
        // verify we can read a single chunk multiple times
        for (int i = 0; i < 2; ++i) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                    .containsExactlyElementsOf(dataPages);
        }
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));

        // verify file is actually removed
        assertThrows(RuntimeException.class, () -> spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0));
    }

    @Test
    public void testOverwritingLargeChunks()
    {
        String randomLongString1 = getRandomLargeString();
        String randomLongString2 = getRandomLargeString();
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString1)),
                new DataPage(1, 1, utf8Slice(randomLongString2)));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(dataPages)));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                .containsExactlyElementsOf(dataPages);

        // overwriting with order reversed
        List<DataPage> reversedDataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice(randomLongString2)),
                new DataPage(1, 1, utf8Slice(randomLongString1)));
        getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0, toChunkDataLease(reversedDataPages)));
        assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, CHUNK_ID_0))))
                .containsExactlyElementsOf(reversedDataPages);

        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
    }

    @Test
    public void testReadWriteManyChunks()
    {
        long numChunks = 32L;
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            getFutureValue(spoolingStorage.writeChunk(BUFFER_NODE_ID, EXCHANGE_ID, chunkId,
                    toChunkDataLease(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId)))))));
        }
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            assertThat(getFutureValue(spooledChunkReader.getDataPages(spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, chunkId))))
                    .containsExactlyElementsOf(ImmutableList.of(new DataPage(0, 0, utf8Slice(String.valueOf(chunkId)))));
        }

        assertEquals(32, spoolingStorage.getSpooledChunks());
        getFutureValue(spoolingStorage.removeExchange(BUFFER_NODE_ID, EXCHANGE_ID));
        assertEquals(0, spoolingStorage.getSpooledChunks());

        // verify spooling files are removed
        for (long chunkId = 0; chunkId < numChunks; ++chunkId) {
            long finalChunkId = chunkId;
            assertThatThrownBy(() -> spoolingStorage.getSpoolingFile(BUFFER_NODE_ID, EXCHANGE_ID, finalChunkId))
                    .isInstanceOf(DataServerException.class)
                    .hasMessage("No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(BUFFER_NODE_ID, EXCHANGE_ID, chunkId));
        }
    }

    private static String getRandomLargeString()
    {
        byte[] array = new byte[5_000_000];
        random.nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}
