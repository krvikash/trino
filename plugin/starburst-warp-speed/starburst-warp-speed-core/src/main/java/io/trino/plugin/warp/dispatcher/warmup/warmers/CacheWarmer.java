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
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.storage.write.PageSink;
import io.trino.plugin.warp.storage.write.StorageWriterService;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.VaradaPageSinkFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class CacheWarmer
{
    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCreator warmupElementsCreator;
    private final VaradaPageSinkFactory varadaPageSinkFactory;
    private final StorageWarmerService storageWarmerService;
    private final StorageWriterService storageWriterService;
    private final AtomicInteger tmpUniqueKeyMarker;

    @Inject
    public CacheWarmer(RowGroupDataService rowGroupDataService,
            WarmupElementsCreator warmupElementsCreator,
            VaradaPageSinkFactory varadaPageSinkFactory,
            StorageWarmerService storageWarmerService,
            StorageWriterService storageWriterService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.varadaPageSinkFactory = requireNonNull(varadaPageSinkFactory);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.storageWriterService = requireNonNull(storageWriterService);
        this.tmpUniqueKeyMarker = new AtomicInteger(0);
    }

    public List<WarmupElementWriteMetadata> getWarmupElementWriteMetadatasToWarm(List<CacheColumnId> columns,
            List<Type> columnsTypes,
            RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        Set<String> failedWarmupElements = Collections.emptySet();
        if (rowGroupData != null) {
            //currently, all failed warmup elements are handled the same, do not try to re-warm
            failedWarmupElements = rowGroupData.getWarmUpElements().stream().filter(x -> !x.isValid()).map(x -> x.getVaradaColumn().getName()).collect(Collectors.toSet());
        }
        UUID storeId = UUID.randomUUID();
        List<WarmupElementWriteMetadata> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String cacheColumnId = columns.get(i).toString().toLowerCase(Locale.ROOT);
            if (failedWarmupElements.contains(cacheColumnId)) {
                break;
            }
            Optional<WarmupElementWriteMetadata> we = createCacheWarmupElements(rowGroupKey, cacheColumnId, columnsTypes.get(i), i, storeId);
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

    private Optional<WarmupElementWriteMetadata> createCacheWarmupElements(RowGroupKey rowGroupKey, String cacheColumnId, Type type, int connectorBlockIndex, UUID storeId)
    {
        Optional<WarmupElementWriteMetadata> res = Optional.empty();
        Optional<WarmUpElement> warmupElement = warmupElementsCreator.createWarmupElement(cacheColumnId, type, storeId);
        SchemaTableName schemaTableColumn = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        if (warmupElement.isPresent()) {
            res = Optional.of(WarmupElementWriteMetadata.builder()
                    .warmUpElement(warmupElement.get())
                    .connectorBlockIndex(connectorBlockIndex)
                    .type(type)
                    .schemaTableColumn(new SchemaTableColumn(schemaTableColumn, warmupElement.get().getVaradaColumn()))
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
        PageSink pageSink = varadaPageSinkFactory.create(storageWriterSplitConfig);
        rowGroupDataService.getOrCreateTmpRowGroupData(tmpRowGroupKey);
        storageWarmerService.createFile(tmpRowGroupKey);
        long[] fileCookie = storageWarmerService.fileOpen(tmpRowGroupKey);
        int fileOffset = getFileOffset(tmpRowGroupKey);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();

        pageSink.open(fileCookie, fileOffset, warmUpElementToWarm, outDictionaryWarmInfos);
        return new WarmingCandidate(fileCookie, pageSink, fileOffset, warmUpElementToWarm, tmpRowGroupKey);
    }

    public StorageWriterSplitConfig lockAndStartWarming(RowGroupKey permanentRowGroupKey)
            throws InterruptedException
    {
        RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(permanentRowGroupKey, Collections.emptyMap());
        storageWarmerService.lockRowGroup(rowGroupData);
        return storageWriterService.startWarming("WarpCacheManager", permanentRowGroupKey.filePath(), false);
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
