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
package io.trino.plugin.warp.storage.read;

import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.spi.block.Block;

import java.util.List;

public interface BlocksAggregator
{
    QueryArgs getQueryArgs(QueryParams queryParams, CustomStatsContext customStatsContext);

    AggregatorArgs open(QueryArgs queryArgs);

    AggregatorPageArgs openPage(RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            ThreadArena pageArena,
            AggregatorArgs aggregatorArgs,
            WarpQueryState queryState,
            List<Integer> blocksToLoad);

    void prepareBlocks(ChunkProperties chunk,
            RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState,
            List<Integer> blocksToLoad);

    Block[] aggregateBlocks(RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState,
            List<ChunkProperties> pageChunksList,
            List<Integer> loadedBlocks,
            Block[] blocks);

    long closePage(QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs);

    void abortPage(QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, Exception e);

    void close(QueryArgs queryArgs);

    List<Integer> getPreLoadedBlocks(QueryArgs queryArgs);
}
