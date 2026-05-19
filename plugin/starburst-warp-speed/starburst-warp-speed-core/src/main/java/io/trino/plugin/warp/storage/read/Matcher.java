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

import java.util.Optional;

public interface Matcher
{
    MatcherArgs open(QueryArgs queryArgs, CustomStatsContext customStatsContext);

    MatcherPageArgs openPage(
            RecordIndexes recordIndexes,
            ThreadArena pageArena,
            QueryArgs queryArgs,
            MatcherArgs matcherArgs,
            AggregatorPageArgs aggregatorPageArgs);

    Optional<ChunkProperties> match(
            int recordsPageLimit,
            QueryArgs queryArgs,
            MatcherArgs matcherArgs,
            MatcherPageArgs matcherPageArgs,
            WarpQueryState queryState);

    void closePage(QueryArgs queryArgs, MatcherArgs matcherArgs, MatcherPageArgs matcherPageArgs);

    void abortPage(QueryArgs queryArgs, MatcherPageArgs matcherPageArgs, Exception e);

    WarpStoragePageSource.RowRanges getRanges(MatcherPageArgs matcherPageArgs);
}
