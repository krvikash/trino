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
import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingType;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.ArrayType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class EmbeddingGeneratingPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink delegate;
    private final int dataColumnChannel;
    private final int embeddingColumnChannel;
    private final EmbeddingType embeddingType;
    private final EmbeddingModelClient embeddingModelClient;

    public EmbeddingGeneratingPageSink(
            ConnectorPageSink delegate,
            int dataColumnChannel,
            int embeddingColumnChannel,
            EmbeddingType embeddingType,
            EmbeddingModelClient embeddingModelClient)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.dataColumnChannel = dataColumnChannel;
        this.embeddingColumnChannel = embeddingColumnChannel;
        this.embeddingType = requireNonNull(embeddingType, "embeddingColumnType is null");
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
        Block embeddingsBlock = generateEmbeddings(dataBlock);

        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel == embeddingColumnChannel) {
                blocks[channel] = embeddingsBlock;
            }
            else {
                blocks[channel] = page.getBlock(channel);
            }
        }

        return delegate.appendPage(new Page(blocks));
    }

    private Block generateEmbeddings(Block dataBlock)
    {
        ImmutableList.Builder<Slice> data = ImmutableList.builder();
        boolean[] isNullOrEmpty = new boolean[dataBlock.getPositionCount()];
        for (int position = 0; position < dataBlock.getPositionCount(); position++) {
            if (dataBlock.isNull(position)) {
                isNullOrEmpty[position] = true;
                continue;
            }

            Slice row = VARCHAR.getSlice(dataBlock, position);
            if (row.length() == 0) {
                isNullOrEmpty[position] = true;
                continue;
            }

            data.add(row);
        }

        return switch (embeddingType) {
            case DOUBLE -> generateDoubleEmbeddings(data.build(), isNullOrEmpty, dataBlock.getPositionCount());
            case FLOAT -> generateFloatEmbeddings(data.build(), isNullOrEmpty, dataBlock.getPositionCount());
            case BINARY -> generateBinaryEmbeddings(data.build(), isNullOrEmpty, dataBlock.getPositionCount());
        };
    }

    private Block generateDoubleEmbeddings(List<Slice> data, boolean[] isNullOrEmpty, int positionCount)
    {
        Iterator<List<Float>> embeddings = embeddingModelClient.generateEmbeddings(data).iterator();
        ArrayType embeddingType = new ArrayType(DOUBLE);
        ArrayBlockBuilder embeddingsBlock = embeddingType.createBlockBuilder(null, positionCount);

        for (int position = 0; position < positionCount; position++) {
            if (isNullOrEmpty[position]) {
                embeddingsBlock.appendNull();
            }
            else {
                embeddingsBlock.buildEntry(elementBuilder -> {
                    List<Float> embedding = embeddings.next();
                    for (Float datum : embedding) {
                        DOUBLE.writeDouble(elementBuilder, datum);
                    }
                });
            }
        }

        return embeddingsBlock.build();
    }

    private Block generateFloatEmbeddings(List<Slice> data, boolean[] isNullOrEmpty, int positionCount)
    {
        Iterator<List<Float>> embeddings = embeddingModelClient.generateEmbeddings(data).iterator();

        ArrayType embeddingType = new ArrayType(REAL);
        ArrayBlockBuilder embeddingsBlock = embeddingType.createBlockBuilder(null, positionCount);

        for (int position = 0; position < positionCount; position++) {
            if (isNullOrEmpty[position]) {
                embeddingsBlock.appendNull();
            }
            else {
                embeddingsBlock.buildEntry(elementBuilder -> {
                    List<Float> embedding = embeddings.next();
                    for (Float datum : embedding) {
                        REAL.writeFloat(elementBuilder, datum);
                    }
                });
            }
        }

        return embeddingsBlock.build();
    }

    private Block generateBinaryEmbeddings(List<Slice> data, boolean[] isNullOrEmpty, int positionCount)
    {
        Iterator<Slice> embeddings = embeddingModelClient.generateBinaryEmbeddings(data).iterator();
        BlockBuilder embeddingsBlock = VARBINARY.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (isNullOrEmpty[position]) {
                embeddingsBlock.appendNull();
            }
            else {
                VARBINARY.writeSlice(embeddingsBlock, embeddings.next());
            }
        }

        return embeddingsBlock.build();
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
