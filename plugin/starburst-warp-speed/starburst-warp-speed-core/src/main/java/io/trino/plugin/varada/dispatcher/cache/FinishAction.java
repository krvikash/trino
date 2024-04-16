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
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.varada.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.varada.storage.write.WarpCacheFilesMerger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Singleton
public class FinishAction
        implements CacheAction
{
    private static final Logger logger = Logger.get(FinishAction.class);

    private final RowGroupDataService rowGroupDataService;
    private final StorageWarmerService storageWarmerService;
    private final WarpCacheFilesMerger warpCacheFilesMerger;

    @Inject
    public FinishAction(RowGroupDataService rowGroupDataService,
            StorageWarmerService storageWarmerService,
            WarpCacheFilesMerger warpCacheFilesMerger)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.warpCacheFilesMerger = requireNonNull(warpCacheFilesMerger);
    }

    @Override
    public CacheWarmState act(List<WarmingCandidate> warmingCandidates, int totalRecords, RowGroupKey permanentRowGroupKey)
    {
        CacheWarmState cacheWarmState = CacheWarmState.FINISHING;
        checkArgument(warmingCandidates != null, "warmingCandidates must be set");
        for (WarmingCandidate warmingCandidate : warmingCandidates) {
            try {
                WarmupElementWriteMetadata warmupElementWriteMetadata = warmingCandidate.warmupElementWriteMetadata();
                checkArgument(warmupElementWriteMetadata.warmUpElement().isValid(), "we %s must be valid", warmupElementWriteMetadata);
                RowGroupData tmpRowGroupData = rowGroupDataService.getOrCreateTmpRowGroupData(warmingCandidate.tmpRowGroupKey());
                WarmSinkResult warmSinkResult = storageWarmerService.sinkClose(
                        warmingCandidate.pageSink(),
                        warmupElementWriteMetadata,
                        totalRecords,
                        true,
                        warmingCandidate.fileOffset(),
                        warmingCandidate.fileCookie());
                rowGroupDataService.updateTmpRowGroupData(tmpRowGroupData,
                        warmSinkResult.warmUpElement(),
                        warmSinkResult.offset(),
                        totalRecords);
                if (!warmSinkResult.warmUpElement().isValid()) {
                    cacheWarmState = CacheWarmState.ABORTING;
                }
            }
            catch (Exception e) {
                logger.error(e, "Failed on finish warm. warmingCandidate=%s", warmingCandidate);
                cacheWarmState = CacheWarmState.ABORTING;
            }
        }
        return cacheWarmState;
    }

    @Override
    public boolean close(List<WarmingCandidate> finishedWarmingCandidates, RowGroupKey permanentRowGroupKey, long flowId, StorageWriterSplitConfig storageWriterSplitConfig, int txId)
    {
        List<RowGroupData> tmpRowGroupDataList = new ArrayList<>();
        try {
            for (WarmingCandidate warmingCandidate : finishedWarmingCandidates) {
                try {
                    RowGroupData tmpRowGroupData = rowGroupDataService.getOrCreateTmpRowGroupData(warmingCandidate.tmpRowGroupKey());
                    closeStorageLayer(tmpRowGroupData, warmingCandidate);
                    tmpRowGroupDataList.add(tmpRowGroupData);
                }
                catch (Exception e) {
                    logger.error(e, "failed to cleanStorage for warmingCandidate=%s", warmingCandidate);
                }
            }

            boolean mergeSucceeded;
            try {
                checkArgument(tmpRowGroupDataList.size() == finishedWarmingCandidates.size());
                mergeSucceeded = warpCacheFilesMerger.mergeTmpFiles(tmpRowGroupDataList, permanentRowGroupKey, true);
            }
            catch (Exception e) {
                logger.error(e, "failed on merge %s. key=%s", tmpRowGroupDataList, permanentRowGroupKey);
                mergeSucceeded = false;
            }
            return mergeSucceeded;
        }
        finally {
            storageWarmerService.finishWarm(
                    flowId,
                    true,
                    true,
                    true);
        }
    }

    private void closeStorageLayer(RowGroupData rowGroupData, WarmingCandidate warmingCandidate)
    {
        storageWarmerService.flushRecords(warmingCandidate.fileCookie(), rowGroupData);
        storageWarmerService.fileClose(warmingCandidate.fileCookie(), Optional.of(rowGroupData));
    }
}
