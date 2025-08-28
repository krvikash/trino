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

import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * A PageFilter that uses a PageProjection to filter rows.
 * The projection must return a boolean value for each row in the page.
 * Nulls are treated as false.
 */
final class ProjectionPageFilter
        implements PageFilter
{
    private final PageProjection projection;

    public ProjectionPageFilter(PageProjection projection)
    {
        this.projection = requireNonNull(projection, "projection is null");
    }

    @Override
    public boolean isDeterministic()
    {
        return projection.isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        return projection.getInputChannels();
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, SourcePage page)
    {
        Block output = projection.project(session, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        int[] outputPositions = new int[page.getPositionCount()];
        int outputPositionsCount = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            outputPositions[outputPositionsCount] = position;
            outputPositionsCount += !output.isNull(position) && BOOLEAN.getBoolean(output, position) ? 1 : 0;
        }
        if (outputPositionsCount == page.getPositionCount() || outputPositionsCount == 0) {
            return SelectedPositions.positionsRange(0, outputPositionsCount);
        }
        return SelectedPositions.positionsList(outputPositions, 0, outputPositionsCount);
    }
}
