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
import io.trino.spi.block.Block;

public interface BlocksAggregator
{
    QueryArgs getQueryArgs(QueryParams queryParams, CustomStatsContext customStatsContext);

    AggregatorArgs open(QueryArgs queryArgs);

    long getOffHeapMemoryUsage(AggregatorArgs aggregatorArgs);

    AggregatorPageArgs openPage(QueryArgs queryArgs, AggregatorArgs aggregatorArgs, WarpQueryState queryState, int rowsLimit);

    boolean prepareBlocks(QueryArgs queryArgs, AggregatorArgs aggregatorArgs, AggregatorPageArgs aggregatorPageArgs, WarpQueryState queryState);

    Block[] aggregateBlocks(QueryArgs queryArgs, AggregatorArgs aggregatorArgs, WarpQueryState queryState);

    WarpStoragePageSource.RowRanges getRanges(AggregatorPageArgs aggregatorPageArgs);

    long closePage(QueryArgs queryArgs, AggregatorArgs aggregatorArgs, AggregatorPageArgs aggregatorPageArgs, WarpQueryState queryState);

    void abortPage(QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, Exception e);

    void close(QueryArgs queryArgs);
}
