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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.model.WildcardColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.warp.dispatcher.warmup.WarmupProperties.NA_TTL;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

public class WarpConnectorDeleteService
        implements WarpDeleteService
{
    private static final Logger logger = Logger.get(WarpConnectorDeleteService.class);
    private final RowGroupDataService rowGroupDataService;
    private final WorkerCapacityManager workerCapacityManager;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WarmupProperties defaultWarmupProperties;
    private final WarmupDemoterStats globalStatsDemoter;
    private final WarmupRuleProvider warmupRuleProvider;
    private final ExecutorService rowGroupExecutorService;

    private final AtomicInteger numActiveWarmingTasks;

    @Inject
    public WarpConnectorDeleteService(RowGroupDataService rowGroupDataService, WorkerCapacityManager workerCapacityManager, WarmupDemoterConfig warmupDemoterConfig, MetricsManager metricsManager, NativeConfig nativeConfig, WarmupRuleProvider warmupRuleProvider)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.defaultWarmupProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE);
        this.warmupDemoterConfig = warmupDemoterConfig;
        this.globalStatsDemoter = metricsManager.registerMetric(WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP));
        this.warmupRuleProvider = requireNonNull(warmupRuleProvider);
        this.numActiveWarmingTasks = new AtomicInteger();
        int rowGroupPoolSize = nativeConfig.getTaskMaxWorkerThreads();
        int rowGroupQueueSize = warmupDemoterConfig.getTasksExecutorQueueSize();
        this.rowGroupExecutorService = new ThreadPoolExecutor(0, rowGroupPoolSize,
                                                              60L, TimeUnit.SECONDS,
                                                              new LinkedBlockingQueue<>(rowGroupQueueSize),
                                                              new ThreadFactoryBuilder().setNameFormat("warp-speed-row-group-%s").setDaemon(true).build());
    }

    public TupleRankResult buildTupleRank(List<TupleFilter> tupleFilters,
                                          boolean forceDeleteFailedObjects)
    {
        List<RowGroupData> rowGroupDataList = rowGroupDataService.getAll();
        List<WarmupRule> warmupRules = warmupRuleProvider.getAll();
        Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap = warmupRules.stream()
                .collect(groupingBy(warmupRule -> new SchemaTableColumn(
                        new SchemaTableName(warmupRule.getSchema(),
                                            warmupRule.getTable()),
                        warmupRule.getWarpColumn())));
        Instant now = Instant.now();
        // all tupleRanks of the same shared rowGroup should be gathered together to reduce the nunmber of saved
        // the key of this map make sure that all rg+WarmupType will be hanndled in a single batch
        Map<String, TupleRank> tupleRanksByKey = new TreeMap<>();

        for (RowGroupData rowGroupData : rowGroupDataList) {
            RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();

            for (WarmUpElement warmUpElement : rowGroupData.getWarmUpElements()) {
                if (WarmState.WARM.equals(warmUpElement.getWarmState())) {
                    continue; // already demoted
                }
                if (CollectionUtils.isNotEmpty(tupleFilters) &&
                        tupleFilters.stream().anyMatch(filter -> !filter.shouldHandle(warmUpElement, rowGroupKey))) {
                    continue;
                }

                List<WarmupRule> warmupRuleList = findExistingWarmupElementRules(rowGroupKey, schemaTableColumnToRulesMap, warmUpElement);
                Stream<WarmupRule> wildcardWarmupRules = schemaTableColumnToRulesMap.getOrDefault(
                                new SchemaTableColumn(
                                        new SchemaTableName(rowGroupKey.schema(),
                                                            rowGroupKey.table()),
                                        new WildcardColumn()),
                                List.of())
                        .stream()
                        .map(warmupRule -> WarmupRule.builder(warmupRule).warpColumn(warmUpElement.getWarpColumn()).build());

                WarmupProperties warmupProperties = findMostRelevantRulePropertiesForWarmupElement(rowGroupData,
                                                                                                   warmUpElement,
                                                                                                   Stream.concat(warmupRuleList.stream(), wildcardWarmupRules).toList());

                String warmupElementKey = rowGroupKey + "_" + warmUpElement.getWarpColumn().getColumnId() + "_" + warmUpElement.getWarmUpType();
                tupleRanksByKey.put(warmupElementKey, new TupleRank(warmupProperties, warmUpElement, rowGroupKey));
            }
        }
        TupleRankResult tupleRankResult = new TupleRankResult();
        for (TupleRank tupleRank : tupleRanksByKey.values()) {
            WarmUpElement warmUpElement = tupleRank.warmUpElement();
            WarmupProperties warmupProperties = tupleRank.warmupProperties();

            if (forceDeleteFailedObjects && !warmUpElement.isValid()) {
                logger.debug("add failed warmupElement to failedObjects: warpColumn = %s, warmupType = %s", warmUpElement.getWarpColumn(), warmupProperties.warmUpType().name());
                tupleRankResult.failedObjects().add(tupleRank);
            }
            else if (isDeleteImmediatelyObject(tupleRank, now, tupleFilters)) {
                logger.debug("add warmupElement to ImmediateObject: warpColumn = %s, warmupType = %s, ttl = %s", warmUpElement.getWarpColumn(), warmupProperties.warmUpType().name(), warmupProperties.ttl());
                tupleRankResult.immediateObjects().add(tupleRank);
            }
            else {
                tupleRankResult.tupleRankList().add(tupleRank);
                logger.debug("add warmupElement to tupleRank: warpColumn = %s, warmupType = %s, priority = %s", warmUpElement.getWarpColumn(), warmupProperties.warmUpType().name(), warmupProperties.priority());
            }
        }
        logger.debug("buildTupleRank allRules.size %d rowGroupDataList.size %d tupleFilters.size %d failedObjects.size %d, immediateObjects.size %d, tupleRankList.size %d",
                     warmupRules.size(), rowGroupDataList.size(), (tupleFilters != null) ? tupleFilters.size() : -1, tupleRankResult.failedObjects().size(), tupleRankResult.immediateObjects().size(), tupleRankResult.tupleRankList().size());
        return tupleRankResult;
    }

    private WarmupProperties findMostRelevantRulePropertiesForWarmupElement(RowGroupData rowGroupData,
                                                                            WarmUpElement warmUpElement,
                                                                            List<WarmupRule> rulesForWarmupElement)
    {
        Optional<WarmupRule> optionalWarmupRule = findMostRelevantRuleForWarmupElement(rowGroupData, warmUpElement, rulesForWarmupElement);
        return optionalWarmupRule.map(warmupRule -> new WarmupProperties(warmupRule.getWarmUpType(), warmupRule.getPriority(), warmupRule.getTtl(), TransformFunction.NONE))
                .orElse(defaultWarmupProperties);
    }

    private boolean isDeleteImmediatelyObject(TupleRank tupleRank, Instant currentTime, List<TupleFilter> tupleFilters)
    {
        return CollectionUtils.isNotEmpty(tupleFilters) || // since tuppleRanks were already filtered by tupleFilters
                ((tupleRank.warmupProperties().ttl() > NA_TTL) &&
                        (tupleRank.warmupProperties().ttl() == 0 ||
                                currentTime.isAfter(Instant.ofEpochMilli(tupleRank.warmUpElement().getLastUsedTimestamp())
                                                            .plus(tupleRank.warmupProperties().ttl(), ChronoUnit.SECONDS))));
    }

    @Override
    public Optional<WarmupRule> findMostRelevantRuleForWarmupElement(RowGroupData rowGroupData,
                                                                     WarmUpElement warmUpElement,
                                                                     List<WarmupRule> rulesForWarmupElement)
    {
        Map<RegularColumn, String> partitionKeys = rowGroupData
                .getPartitionKeys()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> (RegularColumn) entry.getKey(),
                                          Map.Entry::getValue));
        return Objects.nonNull(rulesForWarmupElement) ?
                rulesForWarmupElement.stream()
                        .filter(warmupRule -> warmUpElement.getWarmUpType() == warmupRule.getWarmUpType())
                        .filter(warmupRule -> (CollectionUtils.isEmpty(warmupRule.getPredicates()) ||
                                warmupRule.getPredicates().stream().allMatch(warmupPredicateRule -> warmupPredicateRule.test(partitionKeys)))).max(WorkerWarmingService.warmupRuleComparator)
//                        .max(Comparator.comparing(WarmupRule::getPriority))
                : Optional.empty();
    }

    public synchronized void tryAllocateTx()
    {
        workerCapacityManager.setCurrentUsage();
        workerCapacityManager.setExecutingTx(numActiveWarmingTasks.get());
    }

    public void releaseTx()
    {
        workerCapacityManager.decreaseExecutingTx();
        globalStatsDemoter.addreserved_tx(-1);
    }

    private List<WarmupRule> findExistingWarmupElementRules(RowGroupKey rowGroupKey,
                                                            Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap,
                                                            WarmUpElement warmUpElement)
    {
        WarpColumn warpColumn = warmUpElement.getWarpColumn();
        WarpColumn newWarpColumn;
        if (warpColumn instanceof RegularColumn regularColumn) {
            newWarpColumn = new RegularColumn(regularColumn.getName());
        }
        else {
            newWarpColumn = warpColumn;
        }

        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(
                new SchemaTableName(rowGroupKey.schema(),
                                    rowGroupKey.table()),
                newWarpColumn);
        return schemaTableColumnToRulesMap.getOrDefault(schemaTableColumn, List.of());
    }

    @Override
    public long delete(List<TupleRank> tuppleRankList, DemoteContext demoteContext, boolean deleteEmptyRowGroups)
            throws ExecutionException, InterruptedException
    {
        Map<RowGroupKey, List<TupleRank>> rowGroupDataWarmUpElementMap = tuppleRankList.stream()
                .filter(tr -> !demoteContext.getFailedRowGropDataSet().contains(tr.rowGroupKey()))
                .collect(groupingBy(TupleRank::rowGroupKey, mapping(Function.identity(), Collectors.toList())));
        List<RowGroupKey> rowGroupDataList = List.copyOf(rowGroupDataWarmUpElementMap.keySet());
        logger.debug("going to demote %d rowGroupData", rowGroupDataWarmUpElementMap.size());
        List<ListenableFuture<Long>> rowGroupDeleteFutures = new ArrayList<>();
        long deletedObject = 0;
        try {
            for (int i = 0; i < rowGroupDataList.size(); i++) {
                RowGroupKey rowGroupKey = rowGroupDataList.get(i);
                RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
                try {
                    rowGroupDeleteFutures.add(Futures.submit(() -> deleteRowGroupData(rowGroupData, rowGroupDataWarmUpElementMap.get(rowGroupKey), demoteContext, deleteEmptyRowGroups), rowGroupExecutorService));
                }
                catch (RejectedExecutionException ree) {
                    logger.warn("retry to submit row group data %s", rowGroupKey);
                    try {
                        deletedObject += Futures.allAsList(rowGroupDeleteFutures).get().stream().collect(Collectors.summingLong(Long::longValue));
                        rowGroupDeleteFutures = new ArrayList<>();
                        rowGroupDeleteFutures.add(Futures.submit(() -> deleteRowGroupData(rowGroupData, rowGroupDataWarmUpElementMap.get(rowGroupKey), demoteContext, deleteEmptyRowGroups), rowGroupExecutorService));
                    }
                    catch (Exception e) {
                        logger.error("failed to submit row group data %s", rowGroupKey);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        catch (Exception e) {
            Futures.allAsList(rowGroupDeleteFutures).get();
            throw e;
        }
        deletedObject += Futures.allAsList(rowGroupDeleteFutures).get().stream().collect(Collectors.summingLong(Long::longValue));
        return deletedObject;
    }

    @VisibleForTesting
    public long deleteRowGroupData(RowGroupData rowGroupData, List<TupleRank> tupleRanksToDelete, DemoteContext demoteContext, boolean deleteEmptyRowGroups)
    {
        List<WarmUpElement> elementsToDelete = tupleRanksToDelete.stream().map(TupleRank::warmUpElement).collect(Collectors.toList());
        AtomicBoolean delete = new AtomicBoolean(true);
        if (rowGroupData.isEmpty()) {
            logger.debug("empty rowGropData %s", rowGroupData);
            if (rowGroupData.getWarmUpElements().size() == elementsToDelete.size()) {
                logger.debug("empty rowGropData, delete all row group");
                rowGroupDataService.deleteData(rowGroupData, true);
            }
            else {
                if (deleteEmptyRowGroups) {
                    logger.debug("empty rowGropData %s, delete partial elementsToDelete = %d, left = %d",
                                 rowGroupData.getRowGroupKey(), elementsToDelete.size(), rowGroupData.getWarmUpElements().size());
                    rowGroupDataService.updateEmptyRowGroup(rowGroupData, Collections.emptyList(), elementsToDelete);
                }
                else {
                    delete.set(false);
                }
            }
        }
        else {
            AtomicLong retryFailure = new AtomicLong();
            try {
                RetryPolicy<Object> retryPolicy = RetryPolicy.builder().withDelay(warmupDemoterConfig.getDelayAcquireThread())
                        .withMaxDuration(warmupDemoterConfig.getMaxDurationAcquireThread())
                        .onFailedAttempt(a -> retryFailure.incrementAndGet())
                        .handle(TrinoException.class)
                        .withMaxRetries(warmupDemoterConfig.getMaxRetriesAcquireThread()).handle(RuntimeException.class).build();
                Failsafe.with(retryPolicy).run(() -> {
                    tryAllocateTx();
                    if (!coolRowGroupData(rowGroupData, elementsToDelete, demoteContext)) {
                        delete.set(false);
                    }
                });
            }
            catch (Exception e) {
                logger.error("failed to acquire threads for demotion, number of retries = %d", retryFailure.longValue());
                throw e;
            }
            finally {
                demoteContext.getStatsWarmupDemoter().addnumber_fail_acquire(retryFailure.get());
            }
        }
        return delete.get() ? elementsToDelete.size() : 0;
    }

    private boolean coolRowGroupData(RowGroupData rowGroupData, List<WarmUpElement> elementsToDelete, DemoteContext demoteContext)
    {
        logger.debug("coolRowGroupData rowGroup key %s - going to delete warmupElements size = %d",
                     rowGroupData.getRowGroupKey(), elementsToDelete.size());
        boolean success = true;
        try {
            demote(rowGroupData, elementsToDelete);
        }
        catch (Exception e) {
            logger.error(e, String.format("failed to demote rowGroupData: %s, row grop will be deleted",
                                          rowGroupData.getRowGroupKey()));
            handleFailDeleteRowGroup(rowGroupData, demoteContext);
            success = false;
        }
        finally {
            releaseTx();
        }
        return success;
    }

    private void handleFailDeleteRowGroup(RowGroupData rowGroupData, DemoteContext demoteContext)
    {
        rowGroupDataService.removeElements(rowGroupData);
        demoteContext.addFailedRowGropData(rowGroupData.getRowGroupKey());
        demoteContext.getStatsWarmupDemoter().incfailed_row_group_data();
    }

    private void demote(RowGroupData rowGroupData, List<WarmUpElement> elementsToDelete)
            throws InterruptedException
    {
        boolean locked = false;
        try {
            rowGroupData.getLock().writeLock();
            locked = true;
            try {
                rowGroupDataService.removeElements(rowGroupData, elementsToDelete);
            }
            catch (TrinoException te) {
                logger.error(te, "failed during attach rowGroup %s", rowGroupData.getRowGroupKey());
                throw te;
            }
        }
        catch (InterruptedException e) {
            logger.warn(e, "failed to acquire write lock for row group %s", rowGroupData.getRowGroupKey());
            throw e;
        }
        finally {
            if (rowGroupData != null && locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
    }

    public int getNumActiveWarmingTasks()
    {
        return numActiveWarmingTasks.get();
    }

    public void incremenetActiveWarmingTasks()
    {
        numActiveWarmingTasks.incrementAndGet();
    }

    public void decremenetActiveWarmingTasks()
    {
        numActiveWarmingTasks.decrementAndGet();
    }
}
