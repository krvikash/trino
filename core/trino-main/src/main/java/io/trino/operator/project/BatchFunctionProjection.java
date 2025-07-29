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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.project.BatchProjectionUtils.getRemappedInputChannels;
import static java.util.Objects.requireNonNull;

/**
 * Used to evaluate io.trino.spi.function.FunctionKind#BATCH functions.
 * This uses {@link BatchProjectionUtils#project} to evaluate the inputs to the batch function,
 * and then invokes the function over the blocks of the processed input page.
 * The function is expected to return a single block that represents the result of the batch projection.
 */
public final class BatchFunctionProjection
        implements PageProjection
{
    private final List<PageProjection> batchInputProjections;
    private final boolean isDeterministic;
    private final MethodHandle handle;
    private final InputChannels combinedInputChannels;
    private final List<InputChannels> remappedInputChannels;
    private final Object[] arguments;

    public BatchFunctionProjection(List<PageProjection> batchInputProjections, MethodHandle handle, boolean isDeterministic)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.isDeterministic = isDeterministic;
        List<Integer> combinedChannels = batchInputProjections.stream()
                .map(PageProjection::getInputChannels)
                .map(InputChannels::getInputChannels)
                .flatMap(List::stream)
                .distinct()
                .sorted()
                .collect(toImmutableList());
        this.batchInputProjections = ImmutableList.copyOf(batchInputProjections);
        this.combinedInputChannels = new InputChannels(combinedChannels);
        this.remappedInputChannels = getRemappedInputChannels(batchInputProjections, combinedChannels);
        this.arguments = new Object[(batchInputProjections.size() * 2) + 1];
    }

    @Override
    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return combinedInputChannels;
    }

    @Override
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        Block[] inputBlocks = BatchProjectionUtils.project(batchInputProjections, remappedInputChannels, session, page, selectedPositions);
        int positionsCount = selectedPositions.size();
        int index = 0;
        arguments[index++] = session;
        for (Block block : inputBlocks) {
            switch (block) {
                case ValueBlock valueBlock -> {
                    int[] positions = toPositionsList(positionsCount);
                    arguments[index++] = valueBlock;
                    arguments[index++] = positions;
                }
                case RunLengthEncodedBlock runLengthEncodedBlock -> {
                    int[] positions = new int[positionsCount];
                    Arrays.fill(positions, 0);
                    arguments[index++] = runLengthEncodedBlock.getUnderlyingValueBlock();
                    arguments[index++] = positions;
                }
                case DictionaryBlock dictionaryBlock -> {
                    int[] positions = new int[positionsCount];
                    for (int i = 0; i < positions.length; i++) {
                        positions[i] = dictionaryBlock.getUnderlyingValuePosition(i);
                    }
                    arguments[index++] = dictionaryBlock.getUnderlyingValueBlock();
                    arguments[index++] = positions;
                }
            }
        }

        try {
            return (Block) handle.invokeWithArguments(arguments);
        }
        catch (Throwable throwable) {
            throw propagate(throwable);
        }
    }

    private static int[] toPositionsList(int length)
    {
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = i;
        }
        return positions;
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }
}
