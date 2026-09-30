/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import io.trino.plugin.base.mapping.MappingConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.testing.TestingConnectorSession;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.IntVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamWriter;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.ArrowType;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.Field;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.FieldType;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.starburstdata.trino.plugin.snowflake.parallel.Chunk.newInlineChunk;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnowflakeArrowPageSource
{
    private static final int RECORD_BATCHES_PER_CHUNK = 10;
    private static final int ROWS_PER_RECORD_BATCH = 100_000;
    // A record batch of 100k INT values (400 KB of data plus validity) is held in one 512 KiB Arrow buffer
    private static final long RECORD_BATCH_BUFFER_SIZE = 512 * 1024;

    private static final JdbcColumnHandle COLUMN = JdbcColumnHandle.builder()
            .setColumnName("C1")
            .setColumnType(INTEGER)
            .setJdbcTypeHandle(JDBC_INTEGER)
            .build();
    private static final SnowflakeSessionParameters SESSION_PARAMETERS = new SnowflakeSessionParameters(
            "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
            "",
            "",
            "",
            "YYYY-MM-DD",
            "HH24:MI:SS",
            "UTC",
            false,
            "HEX",
            // Snowflake default; StarburstSFSession does not support the fallback isJdbcTreatDecimalAsInt()
            true,
            false,
            false);
    // Inline chunks decode themselves from base64 and never touch the stream provider
    private static final StarburstResultStreamProvider UNUSED_STREAM_PROVIDER =
            new StarburstResultStreamProvider(HttpClients.createDefault(), new SnowflakeConfig());

    @Test
    void testReadAllRows()
            throws IOException
    {
        List<Chunk> chunks = ImmutableList.of(inlineChunk(createArrowStream(0).data()), inlineChunk(createArrowStream(1).data()));
        try (SnowflakeArrowPageSource pageSource = createPageSource(chunks)) {
            int expectedValue = 0;
            while (!pageSource.isFinished()) {
                pageSource.isBlocked().join();
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(INTEGER.getInt(block, position)).isEqualTo(expectedValue++);
                }
            }
            assertThat(expectedValue).isEqualTo(2 * RECORD_BATCHES_PER_CHUNK * ROWS_PER_RECORD_BATCH);
        }
    }

    @Test
    void testReadAllRowsFromGzipChunk()
            throws IOException
    {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(createArrowStream(0).data());
        }
        try (SnowflakeArrowPageSource pageSource = createPageSource(ImmutableList.of(inlineChunk(compressed.toByteArray())))) {
            int expectedValue = 0;
            while (!pageSource.isFinished()) {
                pageSource.isBlocked().join();
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(INTEGER.getInt(block, position)).isEqualTo(expectedValue++);
                }
            }
            assertThat(expectedValue).isEqualTo(RECORD_BATCHES_PER_CHUNK * ROWS_PER_RECORD_BATCH);
        }
    }

    @Test
    void testMemoryUsageIncludesChunkBeingRead()
            throws IOException
    {
        byte[] data = createArrowStream(0).data();
        try (SnowflakeArrowPageSource pageSource = createPageSource(ImmutableList.of(inlineChunk(data)))) {
            SourcePage firstPage = null;
            while (firstPage == null) {
                pageSource.isBlocked().join();
                firstPage = pageSource.getNextSourcePage();
            }
            long usageWhileReading = pageSource.getMemoryUsage() - pageSource.getBufferAllocator().getAllocatedMemory();
            readAll(pageSource);
            assertThat(usageWhileReading - pageSource.getMemoryUsage()).isGreaterThanOrEqualTo(data.length);
        }
    }

    @Test
    void testArrowMemoryIsBoundedByRecordBatch()
            throws IOException
    {
        try (SnowflakeArrowPageSource pageSource = createPageSource(ImmutableList.of(inlineChunk(createArrowStream(0).data())))) {
            readAll(pageSource);
            // Loading a record batch allocates its buffer before the previous one is released, so at most two
            // record batches are held at once, instead of all record batches of the chunk
            assertThat(pageSource.getBufferAllocator().getPeakMemoryAllocation())
                    .isPositive()
                    .isLessThanOrEqualTo(2 * RECORD_BATCH_BUFFER_SIZE);
            assertThat(pageSource.getBufferAllocator().getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testCloseReleasesMemoryAfterReadFailure()
            throws IOException
    {
        ArrowStream stream = createArrowStream(0);
        // Cut the stream in the middle of the second record batch, so reading fails after the first one was loaded
        int truncatedLength = (stream.batchEndOffsets().get(0) + stream.batchEndOffsets().get(1)) / 2;
        byte[] truncated = Arrays.copyOf(stream.data(), truncatedLength);

        SnowflakeArrowPageSource pageSource = createPageSource(ImmutableList.of(inlineChunk(truncated)));
        assertThatThrownBy(() -> readAll(pageSource))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Failed reading Arrow stream");
        assertThat(pageSource.getBufferAllocator().getAllocatedMemory()).isPositive();

        // The allocator close would throw "Memory was leaked by query" if the loaded record batch was not released
        pageSource.close();
        assertThat(pageSource.getBufferAllocator().getAllocatedMemory()).isZero();
    }

    @Test
    void testCloseReleasesMemoryAfterInterruptDuringRead()
            throws IOException
    {
        SnowflakeArrowPageSource pageSource = createPageSource(ImmutableList.of(inlineChunk(createArrowStream(0).data())));
        SourcePage firstPage = null;
        while (firstPage == null) {
            pageSource.isBlocked().join();
            firstPage = pageSource.getNextSourcePage();
        }
        assertThat(pageSource.getBufferAllocator().getAllocatedMemory()).isPositive();

        // Task teardown interrupts a driver that is reading a chunk
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(pageSource::getNextSourcePage)
                    .isInstanceOf(TrinoException.class)
                    .hasCauseInstanceOf(ClosedByInterruptException.class);
        }
        finally {
            // Clear the interrupt flag so it does not leak into other tests
            Thread.interrupted();
        }

        pageSource.close();
        assertThat(pageSource.getBufferAllocator().getAllocatedMemory()).isZero();
    }

    private static void readAll(SnowflakeArrowPageSource pageSource)
    {
        while (!pageSource.isFinished()) {
            pageSource.isBlocked().join();
            pageSource.getNextSourcePage();
        }
    }

    private static SnowflakeArrowPageSource createPageSource(List<Chunk> chunks)
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new SnowflakeParallelSessionProperties(new MappingConfig()).getSessionProperties())
                .build();
        SnowflakeArrowSplit split = new SnowflakeArrowSplit(0, chunks, SESSION_PARAMETERS);
        // The JDBC client and table handle are only used for MERGE row ids
        return new SnowflakeArrowPageSource(session, null, null, split, ImmutableList.of(COLUMN), UNUSED_STREAM_PROVIDER);
    }

    private static Chunk inlineChunk(byte[] arrowStream)
    {
        return newInlineChunk(Base64.getEncoder().encodeToString(arrowStream));
    }

    private static ArrowStream createArrowStream(int chunkIndex)
            throws IOException
    {
        Field field = new Field(
                COLUMN.getColumnName(),
                new FieldType(true, new ArrowType.Int(32, true), null, Map.of("logicalType", "FIXED", "scale", "0", "precision", "38")),
                null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int[] batchEndOffsets = new int[RECORD_BATCHES_PER_CHUNK];
        try (BufferAllocator allocator = new RootAllocator();
                IntVector vector = new IntVector(field, allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(vector);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
            writer.start();
            for (int batch = 0; batch < RECORD_BATCHES_PER_CHUNK; batch++) {
                vector.allocateNew(ROWS_PER_RECORD_BATCH);
                int firstValue = (chunkIndex * RECORD_BATCHES_PER_CHUNK + batch) * ROWS_PER_RECORD_BATCH;
                for (int row = 0; row < ROWS_PER_RECORD_BATCH; row++) {
                    vector.set(row, firstValue + row);
                }
                root.setRowCount(ROWS_PER_RECORD_BATCH);
                writer.writeBatch();
                batchEndOffsets[batch] = output.size();
            }
            writer.end();
        }
        return new ArrowStream(output.toByteArray(), Arrays.stream(batchEndOffsets).boxed().toList());
    }

    private record ArrowStream(byte[] data, List<Integer> batchEndOffsets) {}
}
