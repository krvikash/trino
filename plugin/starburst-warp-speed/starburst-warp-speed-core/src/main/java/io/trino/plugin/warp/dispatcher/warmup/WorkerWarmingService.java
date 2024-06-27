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
package io.trino.plugin.warp.dispatcher.warmup;

import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.model.WildcardColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateContext;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.warp.dispatcher.query.classifier.WarmedWarmupTypes;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.warmup.WarmupProperties.NA_TTL;
import static io.trino.plugin.warp.type.TypeUtils.isWarmBasicSupported;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerWarmingService
{
    public static final String WARMING_SERVICE_STAT_GROUP = "warming_service";
    private static final int MAX_BATCH_SIZE = 1024;

    public static final Comparator<WarmupRule> warmupRuleComparator =
            Comparator.comparingInt((WarmupRule o) -> o.getWarpColumn().getOrder())
                    .thenComparingInt(o -> o.getPredicates().size());
    private static final Logger logger = Logger.get(WorkerWarmingService.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final WarmingServiceStats statsWarmingService;
    private final WarmExecutionTaskFactory warmExecutionTaskFactory;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final WarmupDemoterService warmupDemoterService;
    private final WorkerWarmupRuleService workerWarmupRuleService;
    private final RowGroupDataService rowGroupDataService;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final GlobalConfig globalConfig;
    private ImmutableMap<WarmUpType, WarmupProperties> defaultRules;
    private Map<WarmUpType, Predicate<Type>> warmupTypeValidators;
    private final StorageWarmerService storageWarmerService;
    private final int batchSize;

    @Inject
    public WorkerWarmingService(MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WorkerTaskExecutorService workerTaskExecutorService,
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WarmupDemoterService warmupDemoterService,
            WorkerWarmupRuleService workerWarmupRuleService,
            RowGroupDataService rowGroupDataService,
            WarmupDemoterConfig warmupDemoterConfig,
            GlobalConfig globalConfig,
            StorageWarmerService storageWarmerService)
    {
        this(metricsManager,
                dispatcherProxiedConnectorTransformer,
                workerTaskExecutorService,
                warmExecutionTaskFactory,
                warmupDemoterService,
                workerWarmupRuleService,
                rowGroupDataService,
                warmupDemoterConfig,
                globalConfig,
                storageWarmerService,
                MAX_BATCH_SIZE);
    }

    @VisibleForTesting
    public WorkerWarmingService(MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WorkerTaskExecutorService workerTaskExecutorService,
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WarmupDemoterService warmupDemoterService,
            WorkerWarmupRuleService workerWarmupRuleService,
            RowGroupDataService rowGroupDataService,
            WarmupDemoterConfig warmupDemoterConfig,
            GlobalConfig globalConfig,
            StorageWarmerService storageWarmerService,
            int batchSize)
    {
        this.warmExecutionTaskFactory = warmExecutionTaskFactory;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.workerWarmupRuleService = requireNonNull(workerWarmupRuleService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.globalConfig = requireNonNull(globalConfig);
        this.storageWarmerService = storageWarmerService;
        this.batchSize = batchSize;
        initDefaultRules();
        initWarmUpTypeValidators();
    }

    public void warm(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            int iterationCount)
    {
        try {
            RowGroupKey rowGroupKey = rowGroupDataService.createRowGroupKey(dispatcherSplit.getSchemaName(),
                    dispatcherSplit.getTableName(),
                    dispatcherSplit.getPath(),
                    dispatcherSplit.getStart(),
                    dispatcherSplit.getLength(),
                    dispatcherSplit.getFileModifiedTime(),
                    dispatcherSplit.getDeletedFilesHash());
            WorkerSubmittableTask prioritizeTask = warmExecutionTaskFactory.createExecutionTask(connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    columns,
                    dynamicFilter,
                    rowGroupKey,
                    this,
                    iterationCount,
                    0,
                    WorkerTaskExecutorService.TaskExecutionType.CLASSIFY);
            WorkerTaskExecutorService.SubmissionResult submissionResult = workerTaskExecutorService.submitTask(prioritizeTask, true);

            if (submissionResult == WorkerTaskExecutorService.SubmissionResult.CONFLICT) {
                statsWarmingService.incwarm_skipped_due_key_conflict();
            }
        }
        catch (Exception e) {
            logger.debug(e, "warm failed");
            statsWarmingService.incwarm_failed();
        }
    }

    protected void warmTaskStarted()
    {
        warmupDemoterService.incremenetActiveWarmingTasks();
    }

    protected void warmTaskFinished()
    {
        warmupDemoterService.decremenetActiveWarmingTasks();
    }

    protected void removeRowGroupFromSubmittedRowGroup(RowGroupKey rowGroupKey)
    {
        workerTaskExecutorService.taskFinished(rowGroupKey);
    }

    WarmData getWarmData(List<ColumnHandle> columns,
            RowGroupKey rowGroupKey,
            DispatcherSplit dispatcherSplit,
            ConnectorSession session,
            QueryContext queryContext,
            boolean isDryRun)
    {
        if (!shouldWarm(columns)) {
            return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
        }
        Map<RegularColumn, ColumnHandle> columnNameHandleMap = columns.stream()
                .collect(Collectors.toMap(dispatcherProxiedConnectorTransformer::getWarpRegularColumn, Function.identity()));

        Map<WarpColumn, Map<WarmUpType, WarmupProperties>> requiredWarmupMap = getMatchingRules(dispatcherSplit, columnNameHandleMap);

        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        WarmDataState warmDataState = getWarmDataState(rowGroupData, requiredWarmupMap);

        if (warmDataState.newRequiredWarmUpTypeMap().isEmpty() && canAddDefaultRules(session, queryContext)) {
            Map<WarpColumn, Set<WarmupProperties>> colNameToDefaultRules = getDefaultPropertiesRules(columns, queryContext, session);

            addDefaultRulesToRequiredColumns(requiredWarmupMap, colNameToDefaultRules);
            warmDataState = getWarmDataState(rowGroupData, requiredWarmupMap);
        }

        if (warmDataState.newRequiredWarmUpTypeMap().isEmpty()) {
            statsWarmingService.incall_elements_warmed_or_skipped();
            logger.debug("nothing to warm, do nothing");
            return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
        }
        if (!isDryRun) {
            if (!storageWarmerService.tryAllocateNativeResourceForWarmup()) {  // validate usage only if this is a 'real' run
                return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
            }
        }
        WarmExecutionState warmExecutionState = rowGroupData != null && rowGroupData.isEmpty() ? WarmExecutionState.EMPTY_ROW_GROUP : WarmExecutionState.WARM;
        SetMultimap<WarpColumn, WarmupProperties> requiredWarmUpTypeMap = warmDataState.newRequiredWarmUpTypeMap();
        if (warmExecutionState == WarmExecutionState.WARM) {
            // No need to warm in batches in case of empty row group
            requiredWarmUpTypeMap = warmInBatches(warmDataState.newRequiredWarmUpTypeMap(), warmDataState.existingWarmupMap());
        }
        List<ColumnHandle> dispatcherColumnsToWarm = new ArrayList<>();
        requiredWarmUpTypeMap.keySet().forEach(warpColumn -> {
            if (warpColumn instanceof TransformedColumn transformedColumn) {
                warpColumn = new RegularColumn(transformedColumn.getName(), transformedColumn.getColumnId());
            }
            if (warpColumn instanceof RegularColumn regularColumn) {
                ColumnHandle columnHandle = columnNameHandleMap.get(regularColumn);
                if (columnHandle != null && !dispatcherColumnsToWarm.contains(columnHandle)) {
                    dispatcherColumnsToWarm.add(columnHandle);
                }
            }
            else {
                logger.warn("warpColumn is not instance of RegularColumn -> %s", warpColumn);
            }
        });

        return new WarmData(dispatcherColumnsToWarm, requiredWarmUpTypeMap, warmExecutionState, true, queryContext, warmDataState.warmWarmUpElements);
    }

    private WarmDataState getWarmDataState(RowGroupData rowGroupData,
            Map<WarpColumn, Map<WarmUpType, WarmupProperties>> requiredlWarmupMap)
    {
        SetMultimap<WarpColumn, WarmupProperties> newRequiredWarmUpTypeMap = HashMultimap.create();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        WarmedWarmupTypes warmedWarmupTypes = createExistingWarmupMap(rowGroupData);

        for (WarpColumn warpColumn : requiredlWarmupMap.keySet()) {
            Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties = filterRequiredWarmupByPriority(requiredlWarmupMap.get(warpColumn));

            if (!requiredWarmUpTypeToProperties.isEmpty()) {
                // Map<WarmUpType, WarmUpElement> existingWarmUpTypeToElement = warmedWarmupTypes.is(warpColumn, Map.of());

                if (warmedWarmupTypes.isNewColumn(warpColumn)) {
                    logger.debug("new column to warm%s. newColumn=%s, warmUpTypes=%s",
                            warmedWarmupTypes.getWarmedColumns().isEmpty() ? "" : " in an existing row group",
                            warpColumn,
                            requiredWarmUpTypeToProperties.keySet());
                    newRequiredWarmUpTypeMap.putAll(warpColumn, requiredWarmUpTypeToProperties.values());
                }
                else {
                    for (WarmUpType warmUpType : requiredWarmUpTypeToProperties.keySet()) {
                        WarmupProperties warmupProperties = requiredWarmUpTypeToProperties.get(warmUpType);
                        Optional<WarmUpElement> warmUpElements = warmedWarmupTypes.getByTypeAndColumn(warmUpType, warpColumn, warmupProperties.transformFunction());

                        if (warmUpElements.isEmpty()) {
                            logger.debug("new type to warm. warpColumn=%s, warmUpType=%s", warpColumn, warmUpType);
                            newRequiredWarmUpTypeMap.put(warpColumn, warmupProperties);
                        }
                        warmUpElements.ifPresent(warmUpElement -> {
                            if (warmUpElement.isValid()) {
                                switch (warmUpElement.getWarmState()) {
                                    case HOT -> logger.debug("column is already warmed locally - nothing to do. columnName=%s, warmUpType=%s", warpColumn, warmUpType);
                                    case WARM -> {
                                        logger.debug("column is warmed on cloud - import. columnName=%s, warmUpType=%s", warpColumn, warmUpType);
                                        newRequiredWarmUpTypeMap.put(warpColumn, warmupProperties);
                                        warmWarmUpElements.add(warmUpElement);
                                    }
                                    default -> logger.warn("unexpected - skip warming. warmUpElement %s", warmUpElement);
                                }
                            }
                            else if (shouldAllowWarm(warmUpElement)) {
                                logger.debug("allow warming for warpColumn=%s, warmUpType=%s", warpColumn, warmUpType);
                                newRequiredWarmUpTypeMap.put(warpColumn, warmupProperties);
                            }
                            else {
                                logger.debug("skip warming for warpColumn=%s, warmUpType=%s", warpColumn, warmUpType);
                            }
                        });
                    }
                }
            }
        }
        return new WarmDataState(newRequiredWarmUpTypeMap, warmWarmUpElements, warmedWarmupTypes);
    }

    // Limit the amount of WarmupProperties to warm in a single time.
    // In addition - retry to warm failed warmup elements one by one and only after warming all new warmup elements.
    private SetMultimap<WarpColumn, WarmupProperties> warmInBatches(SetMultimap<WarpColumn, WarmupProperties> newRequiredWarmUpTypeMap,
            WarmedWarmupTypes existingWarmupMap)
    {
        int originalSize = newRequiredWarmUpTypeMap.size();

        // limit the amount of WarmupProperties to warm in a single time
        if (newRequiredWarmUpTypeMap.size() > batchSize) {
            AtomicInteger elementsToRemove = new AtomicInteger(newRequiredWarmUpTypeMap.size() - batchSize);
            while (elementsToRemove.get() > 0) {
                WarpColumn warpColumn = newRequiredWarmUpTypeMap.keys().stream().findAny().orElseThrow();
                Set<WarmupProperties> warmupProperties = newRequiredWarmUpTypeMap.get(warpColumn);
                warmupProperties.removeIf(x -> elementsToRemove.getAndDecrement() > 0);
            }
        }

        // WarmedWarmupTypes warmedWarmupTypes = null;
        // retry to warm failed warmup elements one by one and only after warming all new warmup elements
        SetMultimap<WarpColumn, WarmupProperties> actualProxiedElementsToWarm = newRequiredWarmUpTypeMap.entries().stream()
                .filter(entry -> !existingWarmupMap.contains(entry.getKey(), entry.getValue().warmUpType(), entry.getValue().transformFunction()))
                .collect(Multimaps.toMultimap(Map.Entry::getKey, Map.Entry::getValue, HashMultimap::create));
        if (actualProxiedElementsToWarm.isEmpty()) {
            actualProxiedElementsToWarm = newRequiredWarmUpTypeMap.entries().stream()
                    .sorted(Comparator.comparingInt(entry ->
                    {
                        Optional<WarmUpElement> warmUpElement = existingWarmupMap.getByTypeAndColumn(entry.getValue().warmUpType(), entry.getKey(), entry.getValue().transformFunction());
                        return warmUpElement.map(element -> element.getState().temporaryFailureCount()).orElse(0);
                    }))
                    .collect(Multimaps.toMultimap(Map.Entry::getKey, Map.Entry::getValue, HashMultimap::create));
        }

        if (logger.isDebugEnabled() && originalSize != actualProxiedElementsToWarm.size()) {
            SetMultimap<WarpColumn, WarmupProperties> finalActualProxiedElementsToWarm = actualProxiedElementsToWarm;
            logger.debug("Splitting warmup into batches. Current batch: %s. Remaining: %s",
                    actualProxiedElementsToWarm.entries().stream()
                            .map(entry -> entry.getKey() + ":" + entry.getValue().warmUpType())
                            .collect(Collectors.joining(", ")),
                    newRequiredWarmUpTypeMap.entries().stream()
                            .filter(entry -> !finalActualProxiedElementsToWarm.containsKey(entry.getKey()) || !finalActualProxiedElementsToWarm.get(entry.getKey()).contains(entry.getValue()))
                            .map(entry -> entry.getKey() + ":" + entry.getValue().warmUpType())
                            .collect(Collectors.joining(", ")));
        }

        return actualProxiedElementsToWarm;
    }

    private WarmedWarmupTypes createExistingWarmupMap(RowGroupData rowGroupData)
    {
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        if (rowGroupData == null) {
            return warmedWarmupTypes.build();
        }
        rowGroupData.getWarmUpElements().forEach(warmedWarmupTypes::add);
        return warmedWarmupTypes.build();
    }

    private boolean shouldAllowWarm(WarmUpElement warmUpElement)
    {
        WarmUpElementState.State state = warmUpElement.getState().state();

        if (WarmUpElementState.State.VALID.equals(state)) {
            return !warmUpElement.isHot();
        }
        if (WarmUpElementState.State.FAILED_PERMANENTLY.equals(state)) {
            logger.debug("won't retry to warm a permanent failed warmUpElement. columnKey=%s, warmUpType=%s", warmUpElement.getWarpColumn().getName(), warmUpElement.getWarmUpType());
            statsWarmingService.incwarm_skip_permanent_failed_warmup_element();
            return false;
        }

        // Backoff is required because otherwise we'll run all the retries one after the other (upon successful warmup, ProxyExecutionTask.warming() re-triggers
        // workerWarmingService.warm() if workerWarmingService.getWarmData() indicates that there are still columns to be warmed)
        int temporaryFailureCount = warmUpElement.getState().temporaryFailureCount();

        if (temporaryFailureCount == 1) {
            // Because at the first try we warm in a batch, we don't know if the failure is related to this specific WarmUpElement and we should try again immediately
            return true;
        }

        double nextAttemptTime = warmUpElement.getState().lastTemporaryFailure() + globalConfig.getWarmRetryBackoffFactorInMillis() * Math.pow(2, temporaryFailureCount - 1);
        boolean backoffElapsed = System.currentTimeMillis() > nextAttemptTime;

        if (backoffElapsed) {
            logger.debug("will retry to warm a failed warmUpElement (failed %d / %d times). columnKey=%s, warmUpType=%s",
                    temporaryFailureCount, globalConfig.getMaxWarmRetries(), warmUpElement.getWarpColumn().getName(), warmUpElement.getWarmUpType());
        }
        else {
            logger.debug("won't retry to warm a temporary failed warmUpElement, backoff is until timestamp %f. columnKey=%s, warmUpType=%s",
                    nextAttemptTime, warmUpElement.getWarpColumn().getName(), warmUpElement.getWarmUpType());
            statsWarmingService.incwarm_skip_temporary_failed_warmup_element();
        }

        return backoffElapsed;
    }

    private boolean shouldWarm(List<ColumnHandle> columns)
    {
        boolean shouldWarm = true;
        if (columns.isEmpty()) {
            statsWarmingService.incempty_column_list();
            shouldWarm = false;
        }
        return shouldWarm;
    }

    private boolean canAddDefaultRules(ConnectorSession session, QueryContext queryContext)
    {
        return isDefaultWarmingEnabled(session, queryContext.getRemainingCollectColumns().size()) &&
                warmupDemoterService.canAllowWarmup(warmupDemoterConfig.getDefaultRulePriority());
    }

    private boolean isDefaultWarmingEnabled(ConnectorSession session, int collectColumnsCount)
    {
        Boolean sessionEnabled = WarpSessionProperties.isDefaultWarmingEnabled(session);
        boolean defaultWarmingEnabled = sessionEnabled != null ? sessionEnabled : globalConfig.isEnableDefaultWarming();
        if (defaultWarmingEnabled) {
            int maxElementsToCollect = globalConfig.getMaxCollectColumnsSkipDefaultWarming();
            return collectColumnsCount <= maxElementsToCollect;
        }
        return false;
    }

    private Map<WarpColumn, Set<WarmupProperties>> getDefaultPropertiesRules(List<ColumnHandle> columns,
            QueryContext queryContext,
            ConnectorSession session)
    {
        Map<WarpColumn, Type> columnNameToColumnType = columns
                .stream()
                .filter(c -> !(dispatcherProxiedConnectorTransformer.getColumnType(c) instanceof MapType))
                .collect(Collectors.toMap(
                        dispatcherProxiedConnectorTransformer::getWarpRegularColumn,
                        dispatcherProxiedConnectorTransformer::getColumnType));
        Map<WarpColumn, Set<WarmupProperties>> result = new HashMap<>();
        Set<WarpColumn> warpColumns = new HashSet<>(queryContext.getPredicateContextData().getRemainingColumns());
        if (WarpSessionProperties.isDefaultWarmingIndex(session)) {
            warpColumns.addAll(columnNameToColumnType.keySet());
        }

        if (!globalConfig.isDataOnlyWarming()) {
            warpColumns.forEach(warpColumn -> {
                Set<WarmupProperties> properties = new HashSet<>();
                PredicateContextData predicateContextData = queryContext.getPredicateContextData();
                Type type = columnNameToColumnType.get(warpColumn);
                if (TypeUtils.isWarmLuceneSupported(type) &&
                        predicateContextData.isLuceneColumn(warpColumn)) {
                    properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_LUCENE));
                }
                else if (warpColumn instanceof TransformedColumn transformedColumn) {
                    // we already validated that TransformedColumn isWarmBasicSupported at Coordinator.
                    WarmupProperties warmingProperty = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC,
                            warmupDemoterConfig.getDefaultRulePriority(),
                            NA_TTL,
                            transformedColumn.getTransformFunction());
                    properties.add(warmingProperty);
                }
                else {
                    if (isWarmBasicSupported(type)) {
                        List<PredicateContext> remainingPredicatesByColumn = queryContext.getPredicateContextData().getRemainingPredicatesByColumn((RegularColumn) warpColumn);
                        if (remainingPredicatesByColumn.isEmpty()) {
                            //in case of default warming + default index, we will warm default column with basic
                            properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_BASIC));
                        }
                        else {
                            for (PredicateContext remainingPredicates : remainingPredicatesByColumn) {
                                WarmupProperties defaultWarmingProperty = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, remainingPredicates.getTransformedColumn());
                                properties.add(defaultWarmingProperty);
                            }
                        }
                    }
                }
                if (!properties.isEmpty()) {
                    result.put(warpColumn, properties);
                }
            });
        }
        queryContext.getRemainingCollectColumns()
                .stream()
                .filter(column -> TypeUtils.isWarmDataSupported(columnNameToColumnType.get(dispatcherProxiedConnectorTransformer.getWarpRegularColumn(column))))
                .forEach(column -> {
                    RegularColumn warpColumn = dispatcherProxiedConnectorTransformer.getWarpRegularColumn(column);
                    Set<WarmupProperties> properties = result.computeIfAbsent(warpColumn, v -> new HashSet<>());
                    properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_DATA));
                });
        return result;
    }

    private Map<WarpColumn, Map<WarmUpType, WarmupProperties>> getMatchingRules(DispatcherSplit dispatcherSplit,
            Map<RegularColumn, ColumnHandle> warpColumnToColumnHandle)
    {
        Map<RegularColumn, String> partitionKeysMap = dispatcherSplit.getPartitionKeys().stream().collect(Collectors.toMap(PartitionKey::regularColumn, PartitionKey::partitionValue));
        List<WarmupRule> schemaAndTableRules = workerWarmupRuleService.getWarmupRules(new SchemaTableName(dispatcherSplit.getSchemaName(), dispatcherSplit.getTableName()));
        Map<WarpColumn, Map<WarmUpType, WarmupProperties>> matchingRules = new HashMap<>();
        Map<String, ColumnHandle> columnNameToColumnHandle = warpColumnToColumnHandle.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));

        for (WarmupRule warmupRule : schemaAndTableRules.stream().sorted(warmupRuleComparator).toList()) {
            if (!(warmupRule.getWarpColumn() instanceof WildcardColumn) &&
                    !columnNameToColumnHandle.containsKey(warmupRule.getWarpColumn().getName())) {
                continue;
            }
            if (!(partitionKeysMap.isEmpty() ||
                    CollectionUtils.isNullOrEmpty(warmupRule.getPredicates()) ||
                    warmupRule.getPredicates().stream().allMatch(warmupPredicateRule -> warmupPredicateRule.test(partitionKeysMap)))) {
                continue;
            }

            List<WarpColumn> warpColumns;
            if (warmupRule.getWarpColumn() instanceof WildcardColumn) {
                warpColumns = columnNameToColumnHandle.values()
                        .stream()
                        .map(dispatcherProxiedConnectorTransformer::getWarpRegularColumn)
                        .collect(Collectors.toList());
            }
            else {
                ColumnHandle columnHandle = columnNameToColumnHandle.get(warmupRule.getWarpColumn().getName());
                RegularColumn regularColumn = dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle);

                if (warmupRule.getWarpColumn() instanceof TransformedColumn warmupRuleColumn) {
                    TransformedColumn transformedColumn =
                            new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), warmupRuleColumn.getTransformFunction());
                    warpColumns = List.of(transformedColumn);
                }
                else {
                    warpColumns = List.of(regularColumn);
                }
            }

            warpColumns.stream()
                    .filter(warpColumn -> {
                        List<ColumnHandle> columnHandles = List.of(columnNameToColumnHandle.get(warpColumn.getName()));
                        return columnHandles.stream()
                                .allMatch(columnHandle -> warmupTypeValidators.getOrDefault(warmupRule.getWarmUpType(), x -> false)
                                        .test(dispatcherProxiedConnectorTransformer.getColumnType(columnHandle)));
                    })
                    .forEach(warpColumn -> {
                        Map<WarmUpType, WarmupProperties> warmUpTypeToProperties = matchingRules.computeIfAbsent(warpColumn, c -> new HashMap<>());
                        TransformFunction transformFunction = (warpColumn instanceof TransformedColumn transformedColumn) ?
                                transformedColumn.getTransformFunction() : TransformFunction.NONE;
                        warmUpTypeToProperties.put(warmupRule.getWarmUpType(),
                                new WarmupProperties(warmupRule.getWarmUpType(), warmupRule.getPriority(), warmupRule.getTtl(), transformFunction));
                    });
        }

        return matchingRules;
    }

    private void addDefaultRulesToRequiredColumns(Map<WarpColumn, Map<WarmUpType, WarmupProperties>> requiredWarmUpTypeMap, Map<WarpColumn, Set<WarmupProperties>> colNameToDefaultRules)
    {
        for (Map.Entry<WarpColumn, Set<WarmupProperties>> defaultRules : colNameToDefaultRules.entrySet()) {
            if (requiredWarmUpTypeMap.containsKey(defaultRules.getKey())) {
                Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties = requiredWarmUpTypeMap.get(defaultRules.getKey());
                defaultRules.getValue().forEach(defaultRuleWarmupProperties -> {
                    if (requiredWarmUpTypeToProperties.containsKey(defaultRuleWarmupProperties.warmUpType())) {
                        if (!Objects.equals(defaultRuleWarmupProperties.transformFunction(), TransformFunction.NONE)) {
                            requiredWarmUpTypeToProperties.put(defaultRuleWarmupProperties.warmUpType(), defaultRuleWarmupProperties);
                        }
                    }
                    else {
                        requiredWarmUpTypeToProperties.put(defaultRuleWarmupProperties.warmUpType(), defaultRuleWarmupProperties);
                    }
                });
            }
            else {
                requiredWarmUpTypeMap.put(defaultRules.getKey(), defaultRules.getValue().stream().collect(Collectors.toMap(WarmupProperties::warmUpType, Function.identity())));
            }
        }
        logger.debug("add default warmup rules: %s, all rules:%s", colNameToDefaultRules, requiredWarmUpTypeMap);
    }

    private Map<WarmUpType, WarmupProperties> filterRequiredWarmupByPriority(Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties)
    {
        return requiredWarmUpTypeToProperties
                .entrySet()
                .stream()
                .filter(entry -> warmupDemoterService.canAllowWarmup(entry.getValue().priority()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void initDefaultRules()
    {
        this.defaultRules = ImmutableMap.<WarmUpType, WarmupProperties>builder()
                .put(WarmUpType.WARM_UP_TYPE_DATA, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .put(WarmUpType.WARM_UP_TYPE_BASIC, new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .put(WarmUpType.WARM_UP_TYPE_LUCENE, new WarmupProperties(WarmUpType.WARM_UP_TYPE_LUCENE, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .buildOrThrow();
    }

    private void initWarmUpTypeValidators()
    {
        warmupTypeValidators = Map.of(
                WarmUpType.WARM_UP_TYPE_DATA, TypeUtils::isWarmDataSupported,
                WarmUpType.WARM_UP_TYPE_BASIC, TypeUtils::isWarmBasicSupported,
                WarmUpType.WARM_UP_TYPE_LUCENE, TypeUtils::isWarmLuceneSupported);
    }

    WarmData updateWarmData(RowGroupData rowGroupData, WarmData warmData)
    {
        SetMultimap<WarpColumn, WarmupProperties> newRequiredWarmUpTypeMap =
                getRequiredWarmUpTypeMap(warmData.requiredWarmUpTypeMap(), rowGroupData.getWarmUpElements());

        return new WarmData(getColumnHandleList(warmData.columnHandleList(), newRequiredWarmUpTypeMap.keySet()),
                newRequiredWarmUpTypeMap,
                warmData.warmExecutionState(),
                warmData.txMemoryReserved(),
                warmData.queryContext(),
                null);
    }

    private SetMultimap<WarpColumn, WarmupProperties> getRequiredWarmUpTypeMap(SetMultimap<WarpColumn, WarmupProperties> requiredWarmUpTypeMap,
            Collection<WarmUpElement> warmUpElements)
    {
        Map<WarpColumn, Map<WarmUpType, WarmupProperties>> requiredWarmupMap = new HashMap<>();
        for (Map.Entry<WarpColumn, WarmupProperties> entry : requiredWarmUpTypeMap.entries()) {
            Map<WarmUpType, WarmupProperties> existingWarmUpTypeToProperties = requiredWarmupMap.computeIfAbsent(entry.getKey(), c -> new HashMap<>());
            existingWarmUpTypeToProperties.put(entry.getValue().warmUpType(), entry.getValue());
        }

        Map<WarpColumn, Map<WarmUpType, WarmUpElement>> existingWarmupMap = new HashMap<>();
        for (WarmUpElement warmUpElement : warmUpElements) {
            Map<WarmUpType, WarmUpElement> existingWarmUpTypeToElement = existingWarmupMap.computeIfAbsent(warmUpElement.getWarpColumn(), c -> new HashMap<>());
            existingWarmUpTypeToElement.put(warmUpElement.getWarmUpType(), warmUpElement);
        }

        SetMultimap<WarpColumn, WarmupProperties> newRequiredWarmUpTypeMap = HashMultimap.create();
        for (Map.Entry<WarpColumn, Map<WarmUpType, WarmupProperties>> requiredlWarmupEntry : requiredWarmupMap.entrySet()) {
            WarpColumn warpColumn = requiredlWarmupEntry.getKey();
            Map<WarmUpType, WarmupProperties> warmupPropertiesMap = requiredlWarmupEntry.getValue();

            Map<WarmUpType, WarmUpElement> warmUpElementMap = existingWarmupMap.get(warpColumn);

            if (warmUpElementMap == null) {
                for (WarmupProperties warmupProperties : warmupPropertiesMap.values()) {
                    newRequiredWarmUpTypeMap.put(warpColumn, warmupProperties);
                }
                continue;
            }

            for (Map.Entry<WarmUpType, WarmupProperties> warmupPropertiesEntry : warmupPropertiesMap.entrySet()) {
                WarmUpElement warmUpElement = warmUpElementMap.get(warmupPropertiesEntry.getKey());

                if ((warmUpElement == null) || shouldAllowWarm(warmUpElement)) {
                    newRequiredWarmUpTypeMap.put(warpColumn, warmupPropertiesEntry.getValue());
                }
            }
        }
        return newRequiredWarmUpTypeMap;
    }

    private List<ColumnHandle> getColumnHandleList(List<ColumnHandle> columnHandleList, Set<WarpColumn> columnSet)
    {
        Map<RegularColumn, ColumnHandle> columnNameHandleMap = columnHandleList.stream()
                .collect(Collectors.toMap(dispatcherProxiedConnectorTransformer::getWarpRegularColumn, Function.identity()));

        List<ColumnHandle> dispatcherColumnsToWarm = new ArrayList<>();

        columnSet.forEach(warpColumn -> {
            ColumnHandle columnHandle = columnNameHandleMap.get(warpColumn);

            if (columnHandle != null) {
                dispatcherColumnsToWarm.add(columnHandle);
            }
        });
        return dispatcherColumnsToWarm;
    }

    private record WarmDataState(
            @SuppressWarnings("unused") SetMultimap<WarpColumn, WarmupProperties> newRequiredWarmUpTypeMap,
            @SuppressWarnings("unused") List<WarmUpElement> warmWarmUpElements,
            @SuppressWarnings("unused") WarmedWarmupTypes existingWarmupMap) {}
}
