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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.project.BatchProjectionUtils.getRemappedInputChannels;
import static java.util.Objects.requireNonNull;

/**
 * Used to evaluate a scalar projection over the results of batch projections.
 * This is useful for cases where a scalar function needs to be applied to the results of one or more batch functions.
 * The batch projections are evaluated first, and then the scalar projection is applied to the results.
 */
public final class ScalarProjectionOverBatchFunctions
        implements PageProjection
{
    private final List<PageProjection> batchProjections;
    private final PageProjection scalarProjection;
    private final InputChannels combinedInputChannels;
    private final InputChannels scalarRemappedInputChannels;
    private final List<InputChannels> batchRemappedInputChannels;
    private final Block[] scalarInputs;
    private final boolean isDeterministic;

    public ScalarProjectionOverBatchFunctions(List<PageProjection> batchProjections, PageProjection scalarProjection)
    {
        this.scalarProjection = requireNonNull(scalarProjection, "scalarProjection is null");
        List<Integer> combinedChannels = Stream.concat(
                        batchProjections.stream().map(PageProjection::getInputChannels),
                        Stream.of(scalarProjection.getInputChannels()))
                .map(InputChannels::getInputChannels)
                .flatMap(List::stream)
                .distinct()
                .sorted()
                .collect(toImmutableList());
        this.batchProjections = ImmutableList.copyOf(batchProjections);
        this.combinedInputChannels = new InputChannels(combinedChannels);
        this.scalarRemappedInputChannels = new InputChannels(scalarProjection.getInputChannels().getInputChannels().stream()
                .map(combinedChannels::indexOf)
                .collect(toImmutableList()));
        this.batchRemappedInputChannels = getRemappedInputChannels(batchProjections, combinedChannels);
        this.isDeterministic = batchProjections.stream().allMatch(PageProjection::isDeterministic) && scalarProjection.isDeterministic();
        this.scalarInputs = new Block[scalarProjection.getInputChannels().size() + batchProjections.size()];
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
        Block[] batchOutputs = BatchProjectionUtils.project(batchProjections, batchRemappedInputChannels, session, page, selectedPositions);
        SourcePage scalarSourcePage = createScalarSourcePage(batchOutputs, scalarRemappedInputChannels, page, selectedPositions);

        return scalarProjection.project(session, scalarSourcePage, SelectedPositions.positionsRange(0, selectedPositions.size()));
    }

    private SourcePage createScalarSourcePage(Block[] batchOutputs, InputChannels scalarRemappedInputChannels, SourcePage page, SelectedPositions selectedPositions)
    {
        SourcePage scalarSource = scalarRemappedInputChannels.getInputChannels(page);
        int scalarInputCount = scalarSource.getChannelCount();
        // Layout of scalarInputs:
        // [0, scalarInputCount - 1]                                     : scalar input blocks
        // [scalarInputCount, scalarInputCount + batchOutputCount - 1]   : batch output blocks
        for (int channel = 0; channel < scalarInputCount; channel++) {
            scalarInputs[channel] = scalarBlock(scalarSource.getBlock(channel), selectedPositions);
        }
        System.arraycopy(batchOutputs, 0, scalarInputs, scalarInputCount, batchOutputs.length);
        return SourcePage.create(new Page(scalarSource.getPositionCount(), scalarInputs));
    }

    private static Block scalarBlock(Block block, SelectedPositions selectedPositions)
    {
        if (selectedPositions.isList()) {
            return block.copyPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
        }
        if (selectedPositions.size() == block.getPositionCount()) {
            return block;
        }
        return block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
    }
}
