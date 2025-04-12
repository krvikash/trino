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
package com.starburstdata.trino.plugin.ai.embedding;

import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingType;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static java.util.Objects.requireNonNull;

public class GenerateEmbeddingsFunctionDataProcessor
        implements TableFunctionDataProcessor
{
    private final EmbeddingModelClient embeddingModelClient;
    private final EmbeddingType embeddingType;

    public GenerateEmbeddingsFunctionDataProcessor(EmbeddingModelClient embeddingModelClient, EmbeddingType embeddingType)
    {
        this.embeddingModelClient = requireNonNull(embeddingModelClient, "embeddingModelClient is null");
        this.embeddingType = requireNonNull(embeddingType, "embeddingType is null");
    }

    @Override
    public TableFunctionProcessorState process(@Nullable List<Optional<Page>> input)
    {
        if (input == null) {
            return FINISHED;
        }
        Optional<Page> sourcePage = getOnlyElement(input);
        if (sourcePage.isEmpty()) {
            throw new IllegalStateException("generate_embeddings table function data processor received empty source page");
        }

        Block sourceStrings = sourcePage.get().getBlock(0);
        List<Slice> content = new ArrayList<>(sourceStrings.getPositionCount());
        boolean[] isNullOrEmpty = new boolean[sourceStrings.getPositionCount()];
        for (int position = 0; position < sourceStrings.getPositionCount(); position++) {
            if (sourceStrings.isNull(position)) {
                isNullOrEmpty[position] = true;
                continue;
            }

            Slice rowValue = VarcharType.createUnboundedVarcharType().getSlice(sourceStrings, position);
            if (rowValue.length() == 0) {
                isNullOrEmpty[position] = true;
                continue;
            }

            content.add(rowValue);
        }

        Block embeddingData = switch (embeddingType) {
            case FLOAT -> generateDoubleEncodedEmbeddings(content, isNullOrEmpty, sourceStrings.getPositionCount());
            case DOUBLE -> generateDoubleEncodedEmbeddings(content, isNullOrEmpty, sourceStrings.getPositionCount());
            case BINARY -> generateBinaryEncodedEmbeddings(content, isNullOrEmpty, sourceStrings.getPositionCount());
        };

        BlockBuilder passThroughBlock = BigintType.BIGINT.createBlockBuilder(null, sourceStrings.getPositionCount());
        for (int position = 0; position < sourceStrings.getPositionCount(); position++) {
            BigintType.BIGINT.writeLong(passThroughBlock, position);
        }

        Block[] page = new Block[2];
        page[0] = embeddingData;
        page[1] = passThroughBlock.build();

        return usedInputAndProduced(new Page(sourceStrings.getPositionCount(), page));
    }

    private Block generateDoubleEncodedEmbeddings(List<Slice> content, boolean[] isNullOrEmpty, int positionCount)
    {
        List<List<Float>> data;
        try {
            data = embeddingModelClient.generateEmbeddings(content);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to generate embedding with remote model", e);
        }

        Type arrayType = new ArrayType(RealType.REAL);
        ArrayBlockBuilder pageEncodingBlockBuilder = (ArrayBlockBuilder) arrayType.createBlockBuilder(null, data.size());

        Iterator<List<Float>> nextEmbedding = data.iterator();

        for (int position = 0; position < positionCount; position++) {
            if (isNullOrEmpty[position]) {
                pageEncodingBlockBuilder.appendNull();
            }
            else {
                List<Float> encoding = nextEmbedding.next();
                pageEncodingBlockBuilder.buildEntry(longArrayBlockBuilder -> {
                    for (Float datum : encoding) {
                        RealType.REAL.writeFloat(longArrayBlockBuilder, datum);
                    }
                });
            }
        }

        return pageEncodingBlockBuilder.build();
    }

    private Block generateBinaryEncodedEmbeddings(List<Slice> content, boolean[] isNullOrEmpty, int positionCount)
    {
        List<Slice> data;
        try {
            data = embeddingModelClient.generateBinaryEmbeddings(content);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to generate embedding with remote model", e);
        }

        BlockBuilder blockBuilder = VarbinaryType.VARBINARY.createBlockBuilder(null, positionCount);
        Iterator<Slice> nextEmbedding = data.iterator();
        for (int position = 0; position < positionCount; position++) {
            if (isNullOrEmpty[position]) {
                blockBuilder.appendNull();
            }
            else {
                VarbinaryType.VARBINARY.writeSlice(blockBuilder, nextEmbedding.next());
            }
        }

        return blockBuilder.build();
    }
}
