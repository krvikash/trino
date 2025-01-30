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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface WarpDeleteService
{
    TupleRankResult buildTupleRank(List<TupleFilter> tupleFilters,
            boolean forceDeleteFailedObjects);

    long delete(List<TupleRank> tupleRankList, DemoteContext demoteContext)
            throws ExecutionException, InterruptedException;

    default boolean isDeleteImmediatelyObject(TupleRank tupleRank, Instant currentTime, List<TupleFilter> tupleFilters)
    {
        return CollectionUtils.isNotEmpty(tupleFilters) || // since tupleRanks were already filtered by tupleFilters
                currentTime.isAfter(Instant.ofEpochMilli(tupleRank.warmUpElement().getLastUsedTimestamp())
                        .plus(tupleRank.warmupProperties().ttl(), ChronoUnit.SECONDS));
    }
}
