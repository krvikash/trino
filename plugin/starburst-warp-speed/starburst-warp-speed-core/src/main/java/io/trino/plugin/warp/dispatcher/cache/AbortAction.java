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
import io.airlift.log.Logger;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.WarpCacheFilesMerger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AbortAction
        implements CacheAction
{
    private static final Logger logger = Logger.get(FinishAction.class);

    private final RowGroupDataService rowGroupDataService;
    private final StorageWarmerService storageWarmerService;
    private final WarpCacheFilesMerger warpCacheFilesMerger;
    private final DictionaryCacheService dictionaryCacheService;

    @Inject
    public AbortAction(
            RowGroupDataService rowGroupDataService,
            StorageWarmerService storageWarmerService,
            WarpCacheFilesMerger warpCacheFilesMerger,
            DictionaryCacheService dictionaryCacheService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.warpCacheFilesMerger = requireNonNull(warpCacheFilesMerger);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
    }

    @Override
    public CacheWarmState act(List<WarmingCandidate> warmingCandidates, int totalRecords, RowGroupKey permanentRowGroupKey)
    {
        for (WarmingCandidate warmingCandidate : warmingCandidates) {
            RowGroupData tmpRowGroupData = rowGroupDataService.getOrCreateTmpRowGroupData(warmingCandidate.tmpRowGroupKey());
            WarmupElementWriteMetadata warmupElementWriteMetadata = warmingCandidate.warmupElementWriteMetadata();
            try {
                WarmSinkResult warmSinkResult = storageWarmerService.sinkClose(
                        warmingCandidate.pageSink(),
                        warmupElementWriteMetadata,
                        totalRecords,
                        false,
                        warmingCandidate.fileOffset(),
                        warmingCandidate.fileCookie());
                rowGroupDataService.updateTmpRowGroupData(
                        tmpRowGroupData,
                        warmSinkResult.warmUpElement(),
                        warmSinkResult.offset(),
                        totalRecords);
            }
            catch (Exception e) {
                logger.error(e, "failed to close WE %s", warmupElementWriteMetadata);
            }
        }
        return CacheWarmState.ABORTING;
    }

    @Override
    public boolean close(List<WarmingCandidate> failedWarmingCandidates, RowGroupKey permanentRowGroupKey, long flowId, StorageWriterSplitConfig storageWriterSplitConfig)
    {
        List<RowGroupData> tmpRowGroupDataList = new ArrayList<>();
        List<WarmUpElement> actualFailedWarmupElements = new ArrayList<>();
        try {
            for (WarmingCandidate warmingCandidate : failedWarmingCandidates) {
                try {
                    RowGroupKey tmpRowGroupKey = warmingCandidate.tmpRowGroupKey();
                    RowGroupData tmpRowGroupData = rowGroupDataService.getOrCreateTmpRowGroupData(tmpRowGroupKey);
                    closeStorageLayer(tmpRowGroupData, warmingCandidate.fileCookie());
                    tmpRowGroupDataList.add(tmpRowGroupData);
                    if (warmingCandidate.isFailedCandidate()) {
                        for (WarmUpElement we : tmpRowGroupData.getWarmUpElements()) {
                            if (!we.isValid()) {
                                // we only mark failed the candidate who caused the failure.
                                // note that warmup from tmpRowGroupData holds the updated warmup state
                                actualFailedWarmupElements.add(we);
                            }
                        }
                    }
                }
                catch (Exception e) {
                    logger.error(e, "failed to cleanStorage for warmingCandidate=%s", warmingCandidate);
                }
                finally {
                    dictionaryCacheService.releaseActiveDictionary(warmingCandidate.getDictionaryWarmInfos());
                }
            }

            try {
                rowGroupDataService.markAsFailed(permanentRowGroupKey, actualFailedWarmupElements, Collections.emptyMap());
            }
            catch (Exception e) {
                logger.error(e, "failed on merge %s. key=%s", tmpRowGroupDataList, permanentRowGroupKey);
            }
            finally {
                warpCacheFilesMerger.deleteTmpRowGroups(tmpRowGroupDataList);
            }
        }
        finally {
            storageWarmerService.finishWarm(
                    flowId,
                    true,
                    true,
                    true);
        }
        return false;
    }

    private void closeStorageLayer(RowGroupData rowGroupData, long[] fileCookie)
    {
        storageWarmerService.fileClose(fileCookie, Optional.of(rowGroupData));
    }
}
