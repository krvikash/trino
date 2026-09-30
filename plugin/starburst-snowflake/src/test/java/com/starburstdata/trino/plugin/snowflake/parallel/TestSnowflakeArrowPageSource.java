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

import com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeArrowPageSource.CloseableArrowBatch;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.IntVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeArrowPageSource.decodeArrowInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnowflakeArrowPageSource
{
    private static final int RECORD_BATCH_COUNT = 3;
    // Large enough that the midpoint between two batch boundaries falls inside a record batch body, not its metadata
    private static final int ROWS_PER_RECORD_BATCH = 100_000;

    @Test
    void testDecodeWholeChunk()
            throws IOException
    {
        ArrowStream stream = createArrowStream();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (CloseableArrowBatch batch = decodeArrowInputStream(stream.data(), allocator)) {
                assertThat(batch.batch()).hasSize(RECORD_BATCH_COUNT);
                assertThat(batch.batch().getFirst().getFirst().getValueCount()).isEqualTo(ROWS_PER_RECORD_BATCH);
                assertThat(allocator.getAllocatedMemory()).isPositive();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testDecodeFailureReleasesDecodedRecordBatches()
            throws IOException
    {
        ArrowStream stream = createArrowStream();
        // Cut the stream in the middle of the second record batch, so the first one is fully decoded when the read fails
        int truncatedLength = (stream.batchEndOffsets().get(0) + stream.batchEndOffsets().get(1)) / 2;
        byte[] truncated = Arrays.copyOf(stream.data(), truncatedLength);

        // Closing the allocator would throw "Memory was leaked" if any buffer was still outstanding
        try (BufferAllocator allocator = new RootAllocator()) {
            assertThatThrownBy(() -> decodeArrowInputStream(truncated, allocator))
                    .isInstanceOf(IOException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThat(allocator.getPeakMemoryAllocation()).isPositive();
        }
    }

    private static ArrowStream createArrowStream()
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int[] batchEndOffsets = new int[RECORD_BATCH_COUNT];
        try (BufferAllocator allocator = new RootAllocator();
                IntVector vector = new IntVector("C1", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(vector);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
            writer.start();
            for (int batch = 0; batch < RECORD_BATCH_COUNT; batch++) {
                vector.allocateNew(ROWS_PER_RECORD_BATCH);
                for (int row = 0; row < ROWS_PER_RECORD_BATCH; row++) {
                    vector.set(row, batch * ROWS_PER_RECORD_BATCH + row);
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
