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
package io.trino.plugin.warp.dispatcher.warmup.warmers;

import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isImportExportEnabled;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmingManager
{
    private static final Logger logger = Logger.get(WarmingManager.class);

    private final DictionaryStats dictionaryStats;
    private final WarpProxiedWarmer warpProxiedWarmer;
    private final EmptyRowGroupWarmer emptyRowGroupWarmer;
    private final GlobalConfig globalConfig;
    private final CloudVendorConfig cloudVendorConfig;
    private final RowGroupDataService rowGroupDataService;
    private final WarmupImportServiceStats warmupImportServiceStats;
    private final DictionaryCacheService dictionaryCacheService;
    private final WeGroupWarmer weGroupWarmer;
    private final StorageWarmerService storageWarmerService;

    @Inject
    public WarmingManager(
            WarpProxiedWarmer warpProxiedWarmer,
            EmptyRowGroupWarmer emptyRowGroupWarmer,
            GlobalConfig globalConfig,
            @ForWarp CloudVendorConfig cloudVendorConfig,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            DictionaryCacheService dictionaryCacheService,
            WeGroupWarmer weGroupWarmer,
            StorageWarmerService storageWarmerService)
    {
        this.warpProxiedWarmer = requireNonNull(warpProxiedWarmer);
        this.emptyRowGroupWarmer = requireNonNull(emptyRowGroupWarmer);
        this.globalConfig = requireNonNull(globalConfig);
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupImportServiceStats = requireNonNull(metricsManager).registerMetric(new WarmupImportServiceStats(WARMUP_IMPORTER_STAT_GROUP));
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.weGroupWarmer = requireNonNull(weGroupWarmer);
        this.storageWarmerService = requireNonNull(storageWarmerService);
    }

    public Optional<RowGroupData> importWeGroup(ConnectorSession session, RowGroupKey rowGroupKey, List<WarmUpElement> warmWarmUpElements)
    {
        if (isImportExportEnabled(globalConfig, cloudVendorConfig, session)) {
            if (warmWarmUpElements.isEmpty()) {
                return weGroupWarmer.importWeGroup(session, rowGroupKey);
            }
            else {
                return weGroupWarmer.importWarmUpElements(session, rowGroupKey, warmWarmUpElements);
            }
        }
        logger.debug("importWeGroup isImportExportEnabled = false");
        return Optional.empty();
    }

    public void warm(RowGroupKey rowGroupKey,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            DispatcherSplit dispatcherSplit,
            List<ColumnHandle> columnsToWarm,
            SetMultimap<WarpColumn, WarmupProperties> requiredWarmUpTypeMap,
            List<WarmUpElement> newWarmupElements,
            Map<WarpColumn, String> partitionKeys,
            boolean skipWait)
            throws InterruptedException
    {
        List<DictionaryWarmInfo> outDictionariesWarmInfos = new ArrayList<>();
        RowGroupData rowGroupData = null;
        boolean locked = false;

        try {
            rowGroupData = rowGroupDataService.getOrCreateRowGroupData(rowGroupKey, partitionKeys);
            storageWarmerService.lockRowGroup(rowGroupData);
            locked = true;
            // get latest row group data in case it was updated
            rowGroupData = rowGroupDataService.getOrCreateRowGroupData(rowGroupKey, partitionKeys);

            try {
                //TODO add logic of rowGroupDataService.verifyUpdatedRowGroupData
                if (!newWarmupElements.isEmpty()) {
                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();
                    rowGroupData = warpProxiedWarmer.warm(connectorPageSourceProvider,
                            transactionHandle,
                            session,
                            dispatcherTableHandle,
                            rowGroupKey,
                            rowGroupData,
                            dispatcherSplit,
                            columnsToWarm,
                            newWarmupElements,
                            requiredWarmUpTypeMap,
                            skipWait,
                            (rowGroupData != null) ? rowGroupData.getNextOffset() : 0,
                            globalConfig.getDebugWarming(),
                            outDictionariesWarmInfos);
                    stopWatch.stop();
                    warmupImportServiceStats.addhiveWarmTime(stopWatch.getNanoTime());
                }
            }
            catch (Exception e) {
                rowGroupDataService.markAsFailed(rowGroupKey, newWarmupElements, partitionKeys);
                throw e;
            }
            finally {
                releaseActiveDictionaries(outDictionariesWarmInfos);
            }
        }
        catch (InterruptedException e) {
            logger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
            throw e;
        }
        finally {
            storageWarmerService.releaseRowGroup(rowGroupData, locked);
        }
    }

    public void warmEmptyRowGroup(RowGroupKey rowGroupKey, List<WarmUpElement> newWarmUpElements)
    {
        emptyRowGroupWarmer.warm(rowGroupKey, newWarmUpElements);
    }

    public void saveEmptyRowGroup(RowGroupKey rowGroupKey, List<WarmUpElement> newWarmUpElements, Map<WarpColumn, String> partitionKeys)
    {
        emptyRowGroupWarmer.saveImportedEmptyRowGroup(newWarmUpElements, rowGroupKey, partitionKeys);
    }

    private void releaseActiveDictionaries(List<DictionaryWarmInfo> dictionariesWarmInfos)
    {
        for (DictionaryWarmInfo dictionaryWarmInfo : dictionariesWarmInfos) {
            if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_REJECTED) {
                dictionaryStats.incdictionary_rejected_elements_count();
            }
            else if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_VALID) {
                dictionaryCacheService.releaseActiveDictionary(dictionaryWarmInfo.dictionaryKey());
                dictionaryStats.incdictionary_success_elements_count();
            }
        }
    }
}
