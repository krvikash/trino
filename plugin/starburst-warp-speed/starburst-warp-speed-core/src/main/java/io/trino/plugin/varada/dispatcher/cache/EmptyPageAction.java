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
package io.trino.plugin.varada.dispatcher.cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;

import java.util.Collections;
import java.util.List;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class EmptyPageAction
        implements CacheAction
{
    private final WarmingServiceStats statsWarmingService;
    private final WarmingManager warmingManager;
    private final RowGroupDataService rowGroupDataService;

    @Inject
    public EmptyPageAction(MetricsManager metricsManager,
            WarmingManager warmingManager,
            RowGroupDataService rowGroupDataService)
    {
        this.statsWarmingService = requireNonNull(metricsManager).registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
        this.warmingManager = requireNonNull(warmingManager);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
    }

    @Override
    public CacheWarmState act(List<WarmingCandidate> warmingCandidates, int totalRecords, RowGroupKey permanentRowGroupKey)
    {
        statsWarmingService.incwarm_warp_cache_started();
        // note that on empty page, warmingCandidates is null because it was never initiated
        List<WarmUpElement> warmUpElementList = warmingCandidates.stream().map(x -> x.warmupElementWriteMetadata().warmUpElement()).toList();

        RowGroupData rowGroupData = rowGroupDataService.getIfPresent(permanentRowGroupKey);
        if (rowGroupData == null) {
            warmingManager.saveEmptyRowGroup(permanentRowGroupKey, warmUpElementList, Collections.emptyMap());
        }
        else {
            warmingManager.warmEmptyRowGroup(permanentRowGroupKey, warmUpElementList);
        }
        return CacheWarmState.EMPTY_PAGE;
    }

    @Override
    public boolean close(List<WarmingCandidate> warmingCandidates, RowGroupKey permanentRowGroupKey, long flowId, StorageWriterSplitConfig storageWriterSplitConfig, int txId)
    {
        statsWarmingService.incempty_row_group();
        return true;
    }
}
