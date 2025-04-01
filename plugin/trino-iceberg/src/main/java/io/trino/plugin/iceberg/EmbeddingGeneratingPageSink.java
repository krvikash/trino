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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.ai.EmbeddingModelClient;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public class EmbeddingGeneratingPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink delegate;
    private final int dataColumnChannel;
    private final int embeddingColumnChannel;
    private final ArrayType embeddingColumnType;
    private final EmbeddingModelClient embeddingModelClient;

    public EmbeddingGeneratingPageSink(ConnectorPageSink delegate, int dataColumnChannel, int embeddingColumnChannel, ArrayType embeddingColumnType, EmbeddingModelClient embeddingModelClient)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.dataColumnChannel = dataColumnChannel;
        this.embeddingColumnChannel = embeddingColumnChannel;
        this.embeddingColumnType = requireNonNull(embeddingColumnType, "embeddingColumnType is null");
        this.embeddingModelClient = requireNonNull(embeddingModelClient, "embeddingModelClient is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return delegate.getValidationCpuNanos();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        Block dataBlock = page.getBlock(dataColumnChannel);
        ImmutableList.Builder<Slice> data = ImmutableList.builder();
        boolean[] isNullOrEmpty = new boolean[page.getPositionCount()];
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (dataBlock.isNull(position)) {
                isNullOrEmpty[position] = true;
                continue;
            }

            Slice row = VarcharType.VARCHAR.getSlice(dataBlock, position);
            if (row.length() == 0) {
                isNullOrEmpty[position] = true;
                continue;
            }

            data.add(row);
        }
        Iterator<List<Float>> embeddings = embeddingModelClient.generateEmbeddings(data.build()).iterator();

        BiConsumer<BlockBuilder, Float> blockWriter = blockWriterForType(embeddingColumnType.getElementType());
        ArrayBlockBuilder embeddingsBlock = embeddingColumnType.createBlockBuilder(null, page.getPositionCount());
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isNullOrEmpty[position]) {
                embeddingsBlock.appendNull();
            }
            else {
                embeddingsBlock.buildEntry(elementBuilder -> {
                    List<Float> embedding = embeddings.next();
                    for (Float datum : embedding) {
                        blockWriter.accept(elementBuilder, datum);
                    }
                });
            }
        }

        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel == embeddingColumnChannel) {
                blocks[channel] = embeddingsBlock.build();
            }
            else {
                blocks[channel] = page.getBlock(channel);
            }
        }

        return delegate.appendPage(new Page(blocks));
    }

    private static BiConsumer<BlockBuilder, Float> blockWriterForType(Type embeddingColumnType)
    {
        TypeSignature embeddingTypeSignature = embeddingColumnType.getTypeSignature();
        BiConsumer<BlockBuilder, Float> blockWriter;
        if (embeddingTypeSignature == DoubleType.DOUBLE.getTypeSignature()) {
            blockWriter = DoubleType.DOUBLE::writeDouble;
        }
        else if (embeddingTypeSignature == RealType.REAL.getTypeSignature()) {
            blockWriter = RealType.REAL::writeFloat;
        }
        else {
            throw new IllegalStateException("generate_embeddings only supports embedding columns with type ARRAY(REAL) or ARRAY(DOUBLE)");
        }
        return blockWriter;
    }

    @Override
    public void closeIdleWriters()
    {
        delegate.closeIdleWriters();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return delegate.finish();
    }

    @Override
    public void abort()
    {
        delegate.abort();
    }
}
