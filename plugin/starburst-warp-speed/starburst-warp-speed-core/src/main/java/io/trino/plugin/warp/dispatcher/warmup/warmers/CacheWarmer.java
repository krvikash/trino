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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.storage.write.PageSink;
import io.trino.plugin.warp.storage.write.StorageWriterService;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.WarpPageSinkFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WRITE_BUF_ADDR;
import static java.util.Objects.requireNonNull;

@Singleton
public class CacheWarmer
{
    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCreator warmupElementsCreator;
    private final WarpPageSinkFactory warpPageSinkFactory;
    private final StorageWarmerService storageWarmerService;
    private final StorageWriterService storageWriterService;
    private final GlobalConfig globalConfig;
    private final AtomicInteger tmpUniqueKeyMarker;

    @Inject
    public CacheWarmer(RowGroupDataService rowGroupDataService,
            WarmupElementsCreator warmupElementsCreator,
            WarpPageSinkFactory warpPageSinkFactory,
            StorageWarmerService storageWarmerService,
            StorageWriterService storageWriterService,
            GlobalConfig globalConfig)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.warpPageSinkFactory = requireNonNull(warpPageSinkFactory);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.storageWriterService = requireNonNull(storageWriterService);
        this.globalConfig = requireNonNull(globalConfig);
        this.tmpUniqueKeyMarker = new AtomicInteger(0);
    }

    public List<WarmupElementWriteMetadata> getWarmupElementWriteMetadatasToWarm(List<CacheColumnId> columns,
            List<Type> columnsTypes,
            RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        Set<String> permanentFailedWarmupElements = new HashSet<>();
        Map<String, WarmUpElement> temporaryFailedWarmupElements = new HashMap<>();
        if (rowGroupData != null) {
            for (WarmUpElement we : rowGroupData.getWarmUpElements()) {
                WarmUpElementState weState = we.getState();
                if (weState.equals(WarmUpElementState.FAILED_PERMANENTLY)) {
                    permanentFailedWarmupElements.add(we.getWarpColumn().getName());
                }
                else if (weState.state().equals(WarmUpElementState.State.FAILED_TEMPORARILY)) {
                    temporaryFailedWarmupElements.put(we.getWarpColumn().getName(), we);
                }
            }
        }
        UUID storeId = UUID.randomUUID();
        List<WarmupElementWriteMetadata> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String cacheColumnId = columns.get(i).toString().toLowerCase(Locale.ROOT);
            if (permanentFailedWarmupElements.contains(cacheColumnId)) {
                break;
            }
            Optional<WarmupElementWriteMetadata> we = createCacheWarmupElements(rowGroupKey, cacheColumnId, columnsTypes.get(i), i, storeId, temporaryFailedWarmupElements);
            if (we.isEmpty()) {
                break;
            }
            result.add(we.get());
        }
        if (result.size() != columns.size()) {
            result = Collections.emptyList();
        }
        return result;
    }

    private Optional<WarmupElementWriteMetadata> createCacheWarmupElements(RowGroupKey rowGroupKey,
            String cacheColumnId,
            Type type,
            int connectorBlockIndex,
            UUID storeId,
            Map<String, WarmUpElement> temporaryFailedWarmupElements)
    {
        Optional<WarmupElementWriteMetadata> res = Optional.empty();
        Optional<WarmUpElement> warmupElement;
        if (temporaryFailedWarmupElements.containsKey(cacheColumnId)) {
            WarmUpElement temporaryFailedWe = temporaryFailedWarmupElements.get(cacheColumnId);
            warmupElement = Optional.of(WarmUpElement.builder(temporaryFailedWe).storeId(storeId).build());
        }
        else {
            warmupElement = warmupElementsCreator.createWarmupElement(cacheColumnId, type, storeId);
        }
        SchemaTableName schemaTableColumn = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        if (warmupElement.isPresent()) {
            res = Optional.of(WarmupElementWriteMetadata.builder()
                    .warmUpElement(warmupElement.get())
                    .connectorBlockIndex(connectorBlockIndex)
                    .type(type)
                    .schemaTableColumn(new SchemaTableColumn(schemaTableColumn, warmupElement.get().getWarpColumn()))
                    .build());
        }
        return res;
    }

    public RowGroupKey getTempRowGroupKey(WarmupElementWriteMetadata warmupElementWriteMetadata, RowGroupKey permanentRowGroupKey)
    {
        String uniqueKey = permanentRowGroupKey.table();
        uniqueKey = uniqueKey + tmpUniqueKeyMarker.incrementAndGet();
        return new RowGroupKey("TMP_CACHE_MANAGER_" + warmupElementWriteMetadata.warmUpElement().getWarmUpType(),
                uniqueKey,
                "",
                0,
                0,
                0,
                "",
                permanentRowGroupKey.catalogName());
    }

    public WarmingCandidate initCandidate(StorageWriterSplitConfig storageWriterSplitConfig, WarmupElementWriteMetadata warmUpElementToWarm, RowGroupKey tmpRowGroupKey)
            throws IOException
    {
        PageSink pageSink = warpPageSinkFactory.create(storageWriterSplitConfig);
        rowGroupDataService.getOrCreateTmpRowGroupData(tmpRowGroupKey);
        storageWarmerService.createFile(tmpRowGroupKey);
        long[] fileCookieParams = storageWarmerService.fileOpen(tmpRowGroupKey);
        fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = getFileOffset(tmpRowGroupKey);
        fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_ADDR.ordinal()] = storageWriterSplitConfig.writeBuff().address();
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();

        pageSink.open(fileCookieParams, warmUpElementToWarm, outDictionaryWarmInfos);
        return new WarmingCandidate(fileCookieParams,
                pageSink,
                (int) fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()],
                warmUpElementToWarm,
                outDictionaryWarmInfos,
                tmpRowGroupKey);
    }

    public StorageWriterSplitConfig lockAndStartWarming(RowGroupKey permanentRowGroupKey)
            throws InterruptedException
    {
        RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(permanentRowGroupKey, Collections.emptyMap());
        storageWarmerService.lockRowGroup(rowGroupData);
        return storageWriterService.startWarming("WarpCacheManager", permanentRowGroupKey.filePath(), globalConfig.isEnableDictionary());
    }

    public void finishWarmingAndUnlock(StorageWriterSplitConfig storageWriterSplitConfig, RowGroupKey permanentRowGroupKey)
    {
        try {
            if (storageWriterSplitConfig != null) {
                storageWriterService.finishWarming(storageWriterSplitConfig);
            }
        }
        finally {
            RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(permanentRowGroupKey, Collections.emptyMap());
            storageWarmerService.releaseRowGroup(rowGroupData, true);
        }
    }

    private int getFileOffset(RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        return (rowGroupData != null) ? rowGroupData.getNextOffset() : 0;
    }
}
