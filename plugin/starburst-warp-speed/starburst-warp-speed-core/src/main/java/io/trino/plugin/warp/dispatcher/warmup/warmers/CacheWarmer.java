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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.PageSink;
import io.trino.plugin.warp.storage.write.StorageWriterService;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.storage.write.WarpPageSinkFactory;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
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

import static io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator.INVALID_WARM_ID;
import static java.util.Objects.requireNonNull;

@Singleton
public class CacheWarmer
{
    private static final Logger logger = Logger.get(CacheWarmer.class);
    private final ShapingLogger shapingLogger;
    private static final UUID ORDER_DETERMINISTIC_UUID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCreator warmupElementsCreator;
    private final WarpPageSinkFactory warpPageSinkFactory;
    private final StorageWarmerService storageWarmerService;
    private final StorageWriterService storageWriterService;
    private final DictionaryConfig dictionaryConfig;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final AtomicInteger tmpUniqueKeyMarker;

    @Inject
    public CacheWarmer(
            RowGroupDataService rowGroupDataService,
            WarmupElementsCreator warmupElementsCreator,
            WarpPageSinkFactory warpPageSinkFactory,
            StorageWarmerService storageWarmerService,
            StorageWriterService storageWriterService,
            DictionaryConfig dictionaryConfig,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.warpPageSinkFactory = requireNonNull(warpPageSinkFactory);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.storageWriterService = requireNonNull(storageWriterService);
        this.dictionaryConfig = requireNonNull(dictionaryConfig);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.tmpUniqueKeyMarker = new AtomicInteger(0);
        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public List<WarmupElementWriteMetadata> getWarmupElementWriteMetadatasToWarm(
            List<CacheColumnId> columns,
            List<Type> columnsTypes,
            RowGroupKey rowGroupKey,
            boolean isOrderDeterministic,
            boolean isBasicIndexEnabled)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        Set<String> permanentFailedWarmupElements = new HashSet<>();
        Map<String, WarmUpElement> temporaryFailedWarmupElements = new HashMap<>();
        Set<String> existingColumnIds = new HashSet<>();
        if (rowGroupData != null) {
            for (WarmUpElement we : rowGroupData.getWarmUpElements()) {
                WarmUpElementState weState = we.getState();
                if (weState.equals(WarmUpElementState.FAILED_PERMANENTLY)) {
                    permanentFailedWarmupElements.add(we.getWarpColumn().getName());
                }
                else if (weState.state().equals(WarmUpElementState.State.FAILED_TEMPORARILY)) {
                    temporaryFailedWarmupElements.put(we.getWarpColumn().getName(), we);
                }
                else if (weState.state().equals(WarmUpElementState.State.VALID) && isOrderDeterministic && ORDER_DETERMINISTIC_UUID.equals(we.getStoreId())) {
                    // currently we always create both BASIC and DATA, so no need to check each one separately
                    existingColumnIds.add(we.getWarpColumn().getName());
                }
            }
        }

        // when the order is deterministic, there's no need to use different storeIds
        UUID storeId = isOrderDeterministic ? ORDER_DETERMINISTIC_UUID : UUID.randomUUID();

        ImmutableList.Builder<WarmupElementWriteMetadata> result = ImmutableList.builder();
        boolean hasPermanentFailedColumn = false;
        for (int i = 0; i < columns.size(); i++) {
            String cacheColumnId = columns.get(i).toString().toLowerCase(Locale.ROOT);
            if (permanentFailedWarmupElements.contains(cacheColumnId)) {
                hasPermanentFailedColumn = true;
                break;
            }
            if (existingColumnIds.contains(cacheColumnId)) {
                continue;
            }
            Type type = columnsTypes.get(i);
            Optional<WarmupElementWriteMetadata> writeMetadata = createCacheWarmupElements(rowGroupKey, cacheColumnId, type, i, storeId, temporaryFailedWarmupElements);
            if (writeMetadata.isEmpty()) {
                logger.debug("failed to create WarmupElementWriteMetadata for type %s", type);
                hasPermanentFailedColumn = true;
                break;
            }
            result.add(writeMetadata.get());

            if (isOrderDeterministic && isBasicIndexEnabled && TypeUtils.isCacheWarmBasicSupported(type)) {
                // in case of a full scan we warm BASIC in addition to DATA in order to optimize future queries with predicates
                WarmUpElement warmUpElement = writeMetadata.get().warmUpElement();
                int recTypeLength = warmUpElement.getRecTypeLength();
                RecTypeCode recTypeCode = warmUpElement.getRecTypeCode();
                recTypeLength = TypeUtils.getIndexTypeLength(recTypeCode, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
                if (recTypeLength < 0) {
                    shapingLogger.error(
                            "recTypeLength is negative. recTypeLength=%d, recTypeCode=%s, fixedLengthStringLimit=%d, writeMetadata=%s",
                            recTypeLength,
                            recTypeCode,
                            storageEngineConstants.getFixedLengthStringLimit(),
                            writeMetadata);
                    continue;
                }
                int warmUpContextSize = bufferAllocator.getWarmupIndexTxSize();
                WarmUpElement basicElement = WarmUpElement.builder()
                        .creationTime(System.currentTimeMillis())
                        .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                        .recTypeCode(recTypeCode)
                        .recTypeLength(recTypeLength)
                        .warpColumn(warmUpElement.getWarpColumn())
                        .warmId(INVALID_WARM_ID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .warmUpContextSize(warmUpContextSize)
                        .storeId(warmUpElement.getStoreId())
                        .build();
                WarmupElementWriteMetadata basicWriteMetadata = WarmupElementWriteMetadata.builder(writeMetadata.get()).warmUpElement(basicElement).build();
                result.add(basicWriteMetadata);
            }
        }
        if (hasPermanentFailedColumn) {
            return Collections.emptyList();
        }
        return result.build();
    }

    private Optional<WarmupElementWriteMetadata> createCacheWarmupElements(
            RowGroupKey rowGroupKey,
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
                    .fitForDictionary(true)
                    .build());
        }
        return res;
    }

    public RowGroupKey getTempRowGroupKey(WarmupElementWriteMetadata warmupElementWriteMetadata, RowGroupKey permanentRowGroupKey)
    {
        String uniqueKey = permanentRowGroupKey.table();
        uniqueKey = uniqueKey + tmpUniqueKeyMarker.incrementAndGet();
        return new RowGroupKey(
                "TMP_CACHE_MANAGER_" + warmupElementWriteMetadata.warmUpElement().getWarmUpType(),
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
        int startOffset = getFileOffset(tmpRowGroupKey);
        DictionaryWarmInfo dictionaryWarmInfo = pageSink.open(fileCookieParams, startOffset, warmUpElementToWarm);
        return new WarmingCandidate(
                fileCookieParams,
                pageSink,
                startOffset,
                warmUpElementToWarm,
                dictionaryWarmInfo,
                tmpRowGroupKey);
    }

    public Optional<StorageWriterSplitConfig> startWarming(RowGroupKey permanentRowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(permanentRowGroupKey, Collections.emptyMap());
        if (rowGroupData.isEmpty() && !rowGroupData.getWarmUpElements().isEmpty()) {
            shapingLogger.error("Can't add non-empty WarmUpElements to an existing empty RowGroupData. rowGroupData=%s", rowGroupData);
            return Optional.empty();
        }
        return Optional.of(storageWriterService.startWarming(
                "WarpCacheManager",
                permanentRowGroupKey.filePath(),
                dictionaryConfig.getEnableDictionary(),
                false));
    }

    public void finishWarming(StorageWriterSplitConfig storageWriterSplitConfig)
    {
        if (storageWriterSplitConfig != null) {
            storageWriterService.finishWarming(storageWriterSplitConfig);
        }
    }

    private int getFileOffset(RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        return (rowGroupData != null) ? rowGroupData.getNextOffset() : 0;
    }
}
