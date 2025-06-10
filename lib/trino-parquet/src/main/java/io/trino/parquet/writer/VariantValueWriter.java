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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.spark.Variant;
import io.trino.parquet.spark.VariantBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class VariantValueWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(VariantValueWriter.class);
    private static final ColumnChunk NULL_COLUMN_CHUNK = new ColumnChunk(new VariableWidthBlockBuilder(null, 1, 1)
            .appendNull()
            .build());
    private final ColumnWriter metadataWriter;
    private final ColumnWriter valueWriter;

    public VariantValueWriter(ColumnWriter metadataWriter, ColumnWriter valueWriter)
    {
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        Block block = columnChunk.getBlock();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            if (block.isNull(i)) {
                metadataWriter.writeBlock(NULL_COLUMN_CHUNK);
                valueWriter.writeBlock(NULL_COLUMN_CHUNK);
            }
            else {
                VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
                int valuePosition = block.getUnderlyingValuePosition(i);
                String json = valueBlock.getSlice(valuePosition).toStringUtf8();
                Variant variant = VariantBuilder.parseJson(json);
                metadataWriter.writeBlock(new ColumnChunk(new VariableWidthBlockBuilder(null, 1, 1)
                        .writeEntry(Slices.wrappedBuffer(variant.getMetadata()))
                        .build()));
                valueWriter.writeBlock(new ColumnChunk(new VariableWidthBlockBuilder(null, 1, 1)
                        .writeEntry(Slices.wrappedBuffer(variant.getValue()))
                        .build()));
            }
        }
    }

    @Override
    public void close()
    {
        metadataWriter.close();
        valueWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        return ImmutableList.<BufferData>builder()
                .addAll(metadataWriter.getBuffer())
                .addAll(valueWriter.getBuffer())
                .build();
    }

    @Override
    public long getBufferedBytes()
    {
        return metadataWriter.getBufferedBytes() + valueWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + metadataWriter.getRetainedBytes() + valueWriter.getRetainedBytes();
    }
}
