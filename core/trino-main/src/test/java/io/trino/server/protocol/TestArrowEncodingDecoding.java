/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.encoding.arrow.ArrowQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrowEncodingDecoding
        extends AbstractTestEncodingDecoding
{
    private static final long MAX_BATCH_SIZE = DataSize.of(32, MEGABYTE).toBytes();

    private BufferAllocator allocator;

    @BeforeAll
    void setUp()
    {
        allocator = new RootAllocator(1024 * 1024);
    }

    @AfterAll
    public void cleanUp()
    {
        allocator.close();
    }

    @Override
    protected QueryDataDecoder createDecoder(List<Column> columns, boolean supportsVariantBinary)
    {
        return new ArrowQueryDataDecoder(columns);
    }

    @Override
    protected QueryDataEncoder createEncoder(Session session, List<OutputColumn> columns)
    {
        return new ArrowQueryDataEncoder(allocator, CompressionCodec.Factory.INSTANCE, CompressionUtil.CodecType.NO_COMPRESSION, columns, MAX_BATCH_SIZE);
    }

    @Test
    void testWidePageIsSplitIntoMultipleBatches()
            throws IOException
    {
        int rowCount = 50_000;
        List<OutputColumn> columns = ImmutableList.of(new OutputColumn(0, "col0", BIGINT));
        Page page = bigintPage(rowCount);

        // A budget far smaller than the whole page forces the encoder to split it into row ranges.
        byte[] encoded = encode(columns, DataSize.of(16, DataSize.Unit.KILOBYTE).toBytes(), page);

        assertThat(batchCount(encoded)).isGreaterThan(1);

        List<List<Object>> decoded = decodeValues(ImmutableList.of(TypedColumn.typed("col0", BIGINT)), false, false, encoded);
        // bigintPage() writes 0..rowCount-1 in order; the split batches must decode back in exactly that order.
        assertThat(decoded).hasSize(rowCount);
        for (int i = 0; i < rowCount; i++) {
            assertThat(decoded.get(i).getFirst()).isEqualTo((long) i);
        }
    }

    @Test
    void testAllocatorIsReleasedWhenEncodingRunsOutOfMemory()
    {
        // The child allocator is capped well below what a single batch of this page needs, so encoding must fail.
        BufferAllocator childAllocator = allocator.newChildAllocator("test-oom", 0, DataSize.of(64, DataSize.Unit.KILOBYTE).toBytes());
        List<OutputColumn> columns = ImmutableList.of(new OutputColumn(0, "col0", BIGINT));
        Page page = bigintPage(100_000);

        // A single batch of this page needs far more than the child allocator allows, so encoding must fail.
        // close() closes the child allocator, which itself fails if any buffer is still outstanding.
        try (QueryDataEncoder encoder = new ArrowQueryDataEncoder(childAllocator, CompressionCodec.Factory.INSTANCE, NO_COMPRESSION, columns, DataSize.of(1, DataSize.Unit.GIGABYTE).toBytes())) {
            assertThatThrownBy(() -> encoder.encodeTo(OutputStream.nullOutputStream(), List.of(page)))
                    .isInstanceOf(OutOfMemoryException.class);
            // Regardless of the failure, the encoder must not leak Arrow buffers.
            assertThat(childAllocator.getAllocatedMemory()).isZero();
        }
    }

    private byte[] encode(List<OutputColumn> columns, long maxBatchSizeInBytes, Page page)
            throws IOException
    {
        try (QueryDataEncoder encoder = new ArrowQueryDataEncoder(
                allocator.newChildAllocator("test-encode", 0, Long.MAX_VALUE),
                CompressionCodec.Factory.INSTANCE,
                NO_COMPRESSION,
                columns,
                maxBatchSizeInBytes)) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            encoder.encodeTo(output, List.of(page));
            return output.toByteArray();
        }
    }

    private int batchCount(byte[] encoded)
            throws IOException
    {
        try (BufferAllocator readAllocator = allocator.newChildAllocator("test-read", 0, Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(encoded), readAllocator)) {
            int batches = 0;
            while (reader.loadNextBatch()) {
                batches++;
            }
            return batches;
        }
    }

    private static Page bigintPage(int rowCount)
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return new Page(blockBuilder.build());
    }

    @Test
    @Override
    public void testVariantSerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantSerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerializationInRows()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerializationInRows)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerializationInMaps()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerializationInMaps)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerializationInRows()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerializationInRows)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerializationInArrays()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerializationInArrays)
                .hasMessage("Unsupported type: variant");
    }
}
