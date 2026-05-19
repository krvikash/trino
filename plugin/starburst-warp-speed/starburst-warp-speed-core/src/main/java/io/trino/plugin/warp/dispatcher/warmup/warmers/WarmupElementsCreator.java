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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.ExportState;
import io.trino.plugin.warp.dispatcher.model.RecordData;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DateType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupElementsCreator
{
    public static final int INVALID_WARM_ID = -1;
    private final ShapingLogger shapingLogger;

    private final RowGroupDataService rowGroupDataService;
    private final StorageEngineConstants storageEngineConstants;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final WarmingServiceStats statsWarmingService;
    private final BufferAllocator bufferAllocator;
    private final GlobalConfig globalConfig;

    @Inject
    public WarmupElementsCreator(
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            GlobalConfig globalConfig,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.globalConfig = requireNonNull(globalConfig);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.statsWarmingService = (WarmingServiceStats) metricsManager.get(WarmingServiceStats.createKey());
        this.shapingLogger = shapingLoggerFactory.getInstance(WarmupElementsCreator.class);
    }

    public List<WarmUpElement> createWarmupElements(
            RowGroupKey rowGroupKey,
            SetMultimap<WarpColumn, WarmupProperties> requiredWarmUpTypeMap,
            SchemaTableName schemaTableName,
            List<ColumnHandle> columnsToWarm)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);

        List<RecordData> recordDataList = getRecordData(
                schemaTableName,
                columnsToWarm,
                requiredWarmUpTypeMap.keySet());
        List<WarmUpElement> warmUpElements = new ArrayList<>();

        for (RecordData recordData : recordDataList) {
            if (recordData.recTypeCode() == RecTypeCode.REC_TYPE_INVALID) {
                continue;
            }
            WarpColumn warpColumn = recordData.schemaTableColumn().warpColumn();

            for (WarmupProperties warmUpProperty : requiredWarmUpTypeMap.get(warpColumn)) {
                // There might be a failed or demoted warmup element which we need to try to warm again
                Optional<WarmUpElement> existingWarmUpElement = rowGroupData == null ?
                        Optional.empty() :
                        rowGroupData.getWarmUpElements().stream()
                        .filter(we -> we.getWarmUpType().equals(warmUpProperty.warmUpType()) &&
                                      we.getWarpColumn().equals(warpColumn))
                        .findFirst();

                if (existingWarmUpElement.isPresent()) {
                    if (WarmState.WARM.equals(existingWarmUpElement.get().getWarmState())) {
                        warmUpElements.add(WarmUpElement.builder()
                                .warmUpType(existingWarmUpElement.get().getWarmUpType())
                                .recTypeCode(existingWarmUpElement.get().getRecTypeCode())
                                .recTypeLength(existingWarmUpElement.get().getRecTypeLength())
                                .warpColumn(warpColumn)
                                .warmId(INVALID_WARM_ID)
                                .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                                .warmUpContextSize(existingWarmUpElement.get().getWarmUpContextSize())
                                .build());
                    }
                    else {
                        statsWarmingService.incwarm_begin_retry_warmup_element();
                        warmUpElements.add(existingWarmUpElement.get());
                    }
                }
                else {
                    WarmUpType warmUpType = warmUpProperty.warmUpType();
                    TransformFunction transformFunction = warmUpProperty.transformFunction();
                    int recTypeLength;
                    RecTypeCode recTypeCode;

                    if (warmUpType == WarmUpType.WARM_UP_TYPE_BASIC && Objects.equals(transformFunction, TransformFunction.DATE)) {
                        recTypeCode = RecTypeCode.REC_TYPE_DATE;
                        recTypeLength = TypeUtils.getTypeLength(DateType.DATE, storageEngineConstants.getFixedLengthStringLimit());
                    }
                    else {
                        recTypeLength = recordData.recTypeLength();
                        recTypeCode = recordData.recTypeCode();
                    }

                    if (warmUpType != WarmUpType.WARM_UP_TYPE_DATA) {
                        recTypeLength = TypeUtils.getIndexTypeLength(recTypeCode, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
                    }
                    if (recTypeLength < 0) {
                        shapingLogger.error(
                                "recTypeLength is negative. recTypeLength=%d, warmUpType=%s, transformFunction=%s, recordData=%s, recTypeCode=%s, fixedLengthStringLimit=%d",
                                recTypeLength,
                                warmUpType,
                                transformFunction,
                                recordData,
                                recTypeCode,
                                storageEngineConstants.getFixedLengthStringLimit());
                        continue;
                    }

                    int warmUpContextSize = (warmUpType == WarmUpType.WARM_UP_TYPE_DATA) ?
                            bufferAllocator.getWarmupDataTxSize(recTypeCode, recTypeLength) :
                            bufferAllocator.getWarmupIndexTxSize();
                    warmUpElements.add(WarmUpElement.builder()
                            .creationTime(System.currentTimeMillis())
                            .warmUpType(warmUpType)
                            .recTypeCode(recTypeCode)
                            .recTypeLength(recTypeLength)
                            .warpColumn(warpColumn)
                            .warmId(INVALID_WARM_ID)
                            .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                            .warmUpContextSize(warmUpContextSize)
                            .build());
                }
            }
        }
        return warmUpElements;
    }

    List<RecordData> getRecordData(
            SchemaTableName schemaTableName,
            List<ColumnHandle> columnHandles,
            Set<WarpColumn> actualColumnsToWarm)
    {
        Map<String, Type> columnNameToType = columnHandles.stream()
                .collect(Collectors.toMap(
                        columnHandle -> dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle).getName(),
                        dispatcherProxiedConnectorTransformer::getColumnType));
        return actualColumnsToWarm.stream()
                .map(warpColumn -> createColumn(schemaTableName, warpColumn, columnNameToType))
                .toList();
    }

    private RecordData createColumn(SchemaTableName schemaTableName, WarpColumn warpColumn, Map<String, Type> columnNameToType)
    {
        Type type = null;
        int recTypeLength = 0;

        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_INVALID;
        if (warpColumn instanceof RegularColumn) {
            type = columnNameToType.get(warpColumn.getName());
            recTypeLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
            recTypeCode = TypeUtils.convertToRecTypeCode(type, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
        }
        if (recTypeCode == RecTypeCode.REC_TYPE_INVALID) {
            recTypeLength = 0;
        }
        return new RecordData(
                new SchemaTableColumn(schemaTableName, warpColumn),
                type,
                recTypeCode,
                recTypeLength);
    }

    public Optional<WarmUpElement> createWarmupElement(String cacheColumnId, Type columnType, UUID storeId)
    {
        if (globalConfig.getEnableFSCacheMode() || !TypeUtils.isWarmDataSupported(columnType)) {
            statsWarmingService.incwarm_warp_cache_invalid_type();
            return Optional.empty();
        }

        int recTypeLength = TypeUtils.getTypeLength(columnType, storageEngineConstants.getVarcharMaxLen());
        RecTypeCode recTypeCode = TypeUtils.convertToRecTypeCode(columnType, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
        if (recTypeCode == RecTypeCode.REC_TYPE_INVALID) {
            statsWarmingService.incwarm_warp_cache_invalid_type();
            shapingLogger.warn("unexpectedly failed to create warmup element, columnType=%s", columnType);
            return Optional.empty();
        }
        if (recTypeLength < 0) {
            shapingLogger.error(
                    "recTypeLength is negative. recTypeLength=%d, recTypeCode=%s, varcharMaxLen=%d, columnType=%s, cacheColumnId=%s",
                    recTypeLength,
                    recTypeCode,
                    storageEngineConstants.getVarcharMaxLen(),
                    columnType,
                    cacheColumnId);
            return Optional.empty();
        }

        return Optional.of(WarmUpElement.builder()
                .creationTime(System.currentTimeMillis())
                .warpColumn(new RegularColumn(cacheColumnId))
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(recTypeCode)
                .recTypeLength(recTypeLength)
                .warmId(INVALID_WARM_ID)
                .exportState(ExportState.NOT_EXPORTED)
                .storeId(storeId)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                .warmUpContextSize(bufferAllocator.getWarmupDataTxSize(recTypeCode, recTypeLength))
                .build());
    }

    public static byte getCurrentThreadWarmId()
    {
        return (byte) (System.identityHashCode(Thread.currentThread()) & 0xF);
    }
}
