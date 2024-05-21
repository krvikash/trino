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
package io.trino.plugin.warp.dispatcher.cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;

import java.util.List;

import static io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class AbortOnInitAction
        implements CacheAction
{
    private final StorageWarmerService storageWarmerService;
    private final WarmingServiceStats warmingServiceStats;

    @Inject
    public AbortOnInitAction(StorageWarmerService storageWarmerService, MetricsManager metricsManager)
    {
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.warmingServiceStats = metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
    }

    @Override
    public CacheWarmState act(List<WarmingCandidate> warmingCandidates, int totalRecords, RowGroupKey permanentRowGroupKey)
    {
        return CacheWarmState.ABORT_ON_INIT_PROCESS;
    }

    @Override
    public boolean close(List<WarmingCandidate> warmingCandidates, RowGroupKey permanentRowGroupKey, long flowId, StorageWriterSplitConfig storageWriterSplitConfig, int txId)
    {
        warmingServiceStats.addwarm_failed(warmingCandidates.size());
        storageWarmerService.finishWarm(
                flowId,
                false,
                true,
                false);
        return false;
    }
}
