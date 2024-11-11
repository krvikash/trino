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

import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.warmup.model.WarmupRule;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface WarpDeleteService
{
    TupleRankResult buildTupleRank(List<TupleFilter> tupleFilters,
                                   boolean forceDeleteFailedObjects);

    Optional<WarmupRule> findMostRelevantRuleForWarmupElement(RowGroupData rowGroupData,
                                                                     WarmUpElement warmUpElement,
                                                                     List<WarmupRule> rulesForWarmupElement);

    long delete(List<TupleRank> tuppleRankList, DemoteContext demoteContext, boolean deleteEmptyRowGroups)
            throws ExecutionException, InterruptedException;

    void tryAllocateTx();

    void releaseTx();

    void incremenetActiveWarmingTasks();

    void decremenetActiveWarmingTasks();

    int getNumActiveWarmingTasks();
}
