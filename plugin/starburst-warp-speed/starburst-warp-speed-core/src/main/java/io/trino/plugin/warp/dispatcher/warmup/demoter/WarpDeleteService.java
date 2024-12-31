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

import static io.trino.plugin.warp.dispatcher.warmup.WarmupProperties.NO_EXPIRY;

public interface WarpDeleteService
{
    TupleRankResult buildTupleRank(List<TupleFilter> tupleFilters,
                                   boolean forceDeleteFailedObjects);

    long delete(List<TupleRank> tupleRankList, DemoteContext demoteContext, boolean deleteEmptyRowGroups)
            throws ExecutionException, InterruptedException;

    default boolean isDeleteImmediatelyObject(TupleRank tupleRank, Instant currentTime, List<TupleFilter> tupleFilters)
    {
        return CollectionUtils.isNotEmpty(tupleFilters) || // since tupleRanks were already filtered by tupleFilters
                (tupleRank.warmupProperties().ttl() > NO_EXPIRY &&
                currentTime.isAfter(Instant.ofEpochMilli(tupleRank.warmUpElement().getLastUsedTimestamp())
                                            .plus(tupleRank.warmupProperties().ttl(), ChronoUnit.SECONDS)));
    }
}
