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
package io.trino.operator.gpu;

import io.trino.operator.gpu.Column.Blocks;
import io.trino.spi.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.Math.addExact;

class BufferPages
        implements GpuSourceOperation
{
    private static final int TARGET_ROW_COUNT = 100_000;

    private final List<Page> bufferedPages = new ArrayList<>();
    private int bufferedPagesPositions;
    private boolean finishing;

    @Override
    public boolean needsInput()
    {
        return !finishing && bufferedPagesPositions < TARGET_ROW_COUNT;
    }

    @Override
    public void addInput(Page page)
    {
        if (page.getPositionCount() != 0) {
            bufferedPages.add(page);
            bufferedPagesPositions = addExact(bufferedPagesPositions, page.getPositionCount());
        }
    }

    @Override
    public void noMoreInput()
    {
        finishing = true;
    }

    @Override
    public Result execute()
    {
        if (finishing && bufferedPages.isEmpty()) {
            return new Finished();
        }
        if (bufferedPagesPositions >= TARGET_ROW_COUNT || finishing) {
            int channelCount = bufferedPages.stream()
                    .map(Page::getChannelCount)
                    .distinct()
                    .collect(onlyElement());
            GpuPage gpuPage = new GpuPage(
                    bufferedPagesPositions,
                    IntStream.range(0, channelCount)
                            .mapToObj(channel ->
                                    new Blocks(bufferedPages.stream()
                                            .map(page -> page.getBlock(channel))
                                            .collect(toImmutableList())))
                            .toArray(Column[]::new));
            bufferedPages.clear();
            bufferedPagesPositions = 0;
            return new Data(gpuPage);
        }
        return new Yielded();
    }

    @Override
    public void close()
    {
        bufferedPages.clear();
    }
}
