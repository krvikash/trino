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
import io.trino.parquet.spark.Variant;
import io.trino.parquet.spark.VariantBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class VariantValueWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(VariantValueWriter.class);
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
        int positionCount = block.getPositionCount();

        // Create builders sized for the entire batch
        VariableWidthBlockBuilder metadataBlockBuilder = new VariableWidthBlockBuilder(null, positionCount, 50);
        VariableWidthBlockBuilder valueBlockBuilder = new VariableWidthBlockBuilder(null, positionCount, 50);
        VariableWidthBlock variantBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();

        for (int i = 0; i < positionCount; ++i) {
            if (block.isNull(i)) {
                metadataBlockBuilder.appendNull();
                valueBlockBuilder.appendNull();
            }
            else {
                int valuePosition = block.getUnderlyingValuePosition(i);
                Variant variant = VariantBuilder.parseJson(variantBlock.getSlice(valuePosition).toStringUtf8());
                metadataBlockBuilder.writeEntry(wrappedBuffer(variant.getMetadata()));
                valueBlockBuilder.writeEntry(wrappedBuffer(variant.getValue()));
            }
        }
        metadataWriter.writeBlock(new ColumnChunk(metadataBlockBuilder.build(), columnChunk.getDefLevelWriterProviders(), columnChunk.getRepLevelWriterProviders()));
        valueWriter.writeBlock(new ColumnChunk(valueBlockBuilder.build(), columnChunk.getDefLevelWriterProviders(), columnChunk.getRepLevelWriterProviders()));
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
