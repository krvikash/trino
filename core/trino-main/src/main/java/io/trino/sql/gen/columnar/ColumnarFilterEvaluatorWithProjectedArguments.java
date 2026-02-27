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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.DictionaryAwarePageProjection;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.util.Objects.requireNonNull;

final class ColumnarFilterEvaluatorWithProjectedArguments
        implements FilterEvaluator
{
    private final DebugContext debugContext;
    private final List<PageProjection> argumentProjections;
    private final FilterEvaluator filter;

    ColumnarFilterEvaluatorWithProjectedArguments(
            DebugContext debugContext,
            List<PageProjection> argumentProjections,
            FilterEvaluator filter)
    {
        this.debugContext = requireNonNull(debugContext, "debugContext is null");
        this.argumentProjections = argumentProjections.stream()
                .map(argumentProjection -> {
                    if (argumentProjection.isDeterministic() && argumentProjection.getInputChannels().size() == 1) {
                        return new DictionaryAwarePageProjection(argumentProjection, _ -> randomDictionaryId());
                    }
                    return argumentProjection;
                })
                .collect(toImmutableList());
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        long start = System.nanoTime();
        Block[] blocks = new Block[argumentProjections.size()];
        for (int i = 0; i < argumentProjections.size(); i++) {
            PageProjection projection = argumentProjections.get(i);
            SourcePage inputPage = projection.getInputChannels().getInputChannels(page);
            blocks[i] = projection.project(session, inputPage, activePositions);
            debugContext.logDebugOutput(i, blocks[i], inputPage, activePositions);
        }
        int positionsCount = activePositions.size();
        SourcePage filterInputPage = new TemporarySourcePage(positionsCount, blocks);
        long projectionTimeNanos = System.nanoTime() - start;
        SelectionResult result = filter.evaluate(session, SelectedPositions.positionsRange(0, positionsCount), filterInputPage);
        SelectedPositions translatedPositions = translateResultPositions(result.selectedPositions(), activePositions);
        debugContext.logDebugFilteredPositions(translatedPositions);
        return new SelectionResult(translatedPositions, projectionTimeNanos + result.filterTimeNanos());
    }

    // Translates positions from the filter input page back to the original page. This is needed because the projection output drops inactive positions.
    // originalPositions - positions in the original page that were used to create the filter input page
    // resultPositions - positions in the filter input page that were selected by the filter
    private static SelectedPositions translateResultPositions(SelectedPositions resultPositions, SelectedPositions originalPositions)
    {
        verify(resultPositions.size() <= originalPositions.size(), "resultPositions (%s) cannot be larger than originalPositions (%s)", resultPositions, originalPositions);
        if (resultPositions.size() == originalPositions.size()) {
            return originalPositions;
        }
        if (originalPositions.isList()) {
            if (resultPositions.isList()) {
                int[] positions = new int[resultPositions.size()];
                int[] resultList = resultPositions.getPositions();
                int[] originalList = originalPositions.getPositions();
                for (int i = 0; i < resultPositions.size(); i++) {
                    int resultPosition = resultList[resultPositions.getOffset() + i];
                    // resultPosition is an index in the originalPositions list
                    positions[i] = originalList[originalPositions.getOffset() + resultPosition];
                }
                return SelectedPositions.positionsList(positions, 0, positions.length);
            }
            else {
                int[] positions = new int[resultPositions.size()];
                int[] originalList = originalPositions.getPositions();
                for (int i = 0; i < resultPositions.size(); i++) {
                    int resultPosition = resultPositions.getOffset() + i;
                    // resultPosition is an index in the originalPositions list
                    positions[i] = originalList[originalPositions.getOffset() + resultPosition];
                }
                return SelectedPositions.positionsList(positions, 0, positions.length);
            }
        }
        else {
            if (resultPositions.isList()) {
                if (originalPositions.getOffset() == 0) {
                    return resultPositions;
                }
                int[] positions = new int[resultPositions.size()];
                int[] resultList = resultPositions.getPositions();
                for (int i = 0; i < resultPositions.size(); i++) {
                    positions[i] = resultList[resultPositions.getOffset() + i] + originalPositions.getOffset();
                }
                return SelectedPositions.positionsList(positions, 0, positions.length);
            }
            else {
                return SelectedPositions.positionsRange(
                        originalPositions.getOffset() + resultPositions.getOffset(),
                        resultPositions.size());
            }
        }
    }

    // A minimal implementation of SourcePage that avoids unnecessary defensive copies
    // SourcePage created in this class is short-lived and will not be retained beyond
    // the scope of the evaluate call
    private static final class TemporarySourcePage
            implements SourcePage
    {
        private final Block[] blocks;
        private final int positionsCount;

        public TemporarySourcePage(int positionsCount, Block[] blocks)
        {
            this.blocks = requireNonNull(blocks, "blocks is null");
            this.positionsCount = positionsCount;
        }

        @Override
        public int getPositionCount()
        {
            return positionsCount;
        }

        @Override
        public long getSizeInBytes()
        {
            return 0;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return 0;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {}

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            return blocks[channel];
        }

        @Override
        public Page getPage()
        {
            return new Page(positionsCount, blocks);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            Block[] blocks = new Block[channels.length];
            for (int i = 0; i < channels.length; i++) {
                blocks[i] = getBlock(channels[i]);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blocks[i].getPositions(positions, offset, size);
            }
        }
    }
}
