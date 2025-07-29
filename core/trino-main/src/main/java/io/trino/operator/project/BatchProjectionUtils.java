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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class BatchProjectionUtils
{
    private BatchProjectionUtils() {}

    public static Block[] project(
            List<PageProjection> projections,
            List<InputChannels> remappedInputChannels,
            ConnectorSession session,
            SourcePage page,
            SelectedPositions selectedPositions)
    {
        Block[] blocks = new Block[projections.size()];
        for (int i = 0; i < projections.size(); i++) {
            InputChannels inputChannels = remappedInputChannels.get(i);
            blocks[i] = projections.get(i).project(session, inputChannels.getInputChannels(page), selectedPositions);
        }
        return blocks;
    }

    public static List<InputChannels> getRemappedInputChannels(List<PageProjection> projections, List<Integer> combinedChannels)
    {
        return projections.stream()
                .map(projection -> {
                    InputChannels originalInput = projection.getInputChannels();
                    return new InputChannels(originalInput.getInputChannels().stream()
                            .map(combinedChannels::indexOf)
                            .collect(toImmutableList()));
                })
                .collect(toImmutableList());
    }
}
