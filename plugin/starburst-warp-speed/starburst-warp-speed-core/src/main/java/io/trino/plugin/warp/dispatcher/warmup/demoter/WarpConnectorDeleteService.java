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
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WildcardColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmUtils;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmupDemoterConfigChangedEvent;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isEmptyCollection;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

@Singleton
public class WarpConnectorDeleteService
        implements WarpDeleteService
{
    private static final Logger logger = Logger.get(WarpConnectorDeleteService.class);

    private final RowGroupDataService rowGroupDataService;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WarmupRuleProvider warmupRuleProvider;
    private final ExecutorService rowGroupExecutorService;
    private final WorkerCapacityManager workerCapacityManager;
    private final int pageSizeShift;

    private WarmupProperties defaultWarmupProperties;

    @Inject
    public WarpConnectorDeleteService(
            RowGroupDataService rowGroupDataService,
            WarmupDemoterConfig warmupDemoterConfig,
            NativeConfig nativeConfig,
            WarmupRuleProvider warmupRuleProvider,
            WorkerCapacityManager workerCapacityManager,
            StorageEngineConstants storageEngineConstants,
            EventBus eventBus)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.warmupRuleProvider = requireNonNull(warmupRuleProvider);
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.pageSizeShift = requireNonNull(storageEngineConstants).getPageSizeShift();

        eventBus.register(this);

        initDefaultWarmupProperties();

        rowGroupExecutorService = new ThreadPoolExecutor(
                0,
                nativeConfig.getTaskMaxWorkerThreads(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(warmupDemoterConfig.getTasksExecutorQueueSize()),
                new ThreadFactoryBuilder().setNameFormat("warp-speed-row-group-%s").setDaemon(true).build());
    }

    private void initDefaultWarmupProperties()
    {
        defaultWarmupProperties = new WarmupProperties(
                WarmUpType.WARM_UP_TYPE_DATA,
                warmupDemoterConfig.getDefaultRulePriority(),
                warmupDemoterConfig.getDefaultRuleTtlInSeconds(),
                TransformFunction.NONE);
    }

    public TupleRankResult buildTupleRank(
            List<TupleFilter> tupleFilters,
            boolean forceDeleteFailedObjects)
    {
        Instant now = Instant.now();
        List<RowGroupData> rowGroupDataList = rowGroupDataService.getAll();
        Collection<WarmupRule> warmupRules = warmupRuleProvider.getAll();
        Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap = warmupRules.stream()
                .collect(groupingBy(warmupRule -> new SchemaTableColumn(
                        new SchemaTableName(warmupRule.getSchema(), warmupRule.getTable()),
                        warmupRule.getWarpColumn())));

        List<TupleRank> failedObjects = new ArrayList<>();
        List<TupleRank> immediateObjects = new ArrayList<>();
        List<TupleRank> tupleRankList = new ArrayList<>();

        for (RowGroupData rowGroupData : rowGroupDataList) {
            RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();

            for (WarmUpElement warmUpElement : rowGroupData.getWarmUpElements()) {
                if (WarmState.WARM.equals(warmUpElement.getWarmState())) {
                    continue; // already demoted
                }
                if (!isEmptyCollection(tupleFilters) &&
                        tupleFilters.stream().anyMatch(filter -> !filter.shouldHandle(warmUpElement, rowGroupKey))) {
                    continue;
                }

                List<WarmupRule> warmupRuleList = schemaTableColumnToRulesMap.getOrDefault(
                        new SchemaTableColumn(
                                new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table()),
                                warmUpElement.getWarpColumn()),
                        List.of());

                Stream<WarmupRule> wildcardWarmupRules = schemaTableColumnToRulesMap.getOrDefault(
                                new SchemaTableColumn(
                                        new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table()),
                                        new WildcardColumn()),
                                List.of())
                        .stream()
                        .map(warmupRule -> WarmupRule.builder(warmupRule).warpColumn(warmUpElement.getWarpColumn()).build());

                WarmupProperties warmupProperties = findMostRelevantRulePropertiesForWarmupElement(
                        rowGroupData,
                        warmUpElement,
                        Stream.concat(warmupRuleList.stream(), wildcardWarmupRules).toList());

                TupleRank tupleRank = new TupleRank(warmupProperties, warmUpElement, rowGroupKey);

                if (forceDeleteFailedObjects && !warmUpElement.isValid()) {
                    logger.debug("add failed warmupElement to failedObjects: warpColumn = %s, warmupType = %s",
                            warmUpElement.getWarpColumn(), warmupProperties.warmUpType());
                    failedObjects.add(tupleRank);
                }
                else if (isDeleteImmediatelyObject(tupleRank, now, tupleFilters)) {
                    logger.debug("add warmupElement to ImmediateObject: warpColumn = %s, warmupType = %s, ttl = %s",
                            warmUpElement.getWarpColumn(), warmupProperties.warmUpType(), warmupProperties.ttl());
                    immediateObjects.add(tupleRank);
                }
                else {
                    tupleRankList.add(tupleRank);
                    logger.debug("add warmupElement to tupleRank: warpColumn = %s, warmupType = %s, priority = %s",
                            warmUpElement.getWarpColumn(), warmupProperties.warmUpType(), warmupProperties.priority());
                }
            }
        }

        TupleRankResult tupleRankResult = new TupleRankResult(tupleRankList, immediateObjects, failedObjects);

        logger.debug("buildTupleRank warmupRules.size=%d rowGroupDataList.size=%d tupleFilters.size=%d %s",
                warmupRules.size(),
                rowGroupDataList.size(),
                (tupleFilters != null) ? tupleFilters.size() : -1,
                tupleRankResult.toShortString());

        return tupleRankResult;
    }

    private WarmupProperties findMostRelevantRulePropertiesForWarmupElement(
            RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            List<WarmupRule> rulesForWarmupElement)
    {
        Optional<WarmupRule> optionalWarmupRule = WarmUtils.findMostRelevantRuleForWarmupElement(rowGroupData, warmUpElement, rulesForWarmupElement);
        return optionalWarmupRule.map(warmupRule -> new WarmupProperties(warmupRule.getWarmUpType(), warmupRule.getPriority(), warmupRule.getTtl(), TransformFunction.NONE))
                .orElse(defaultWarmupProperties);
    }

    @Override
    public DeletionStats delete(List<TupleRank> tupleRankList, DemoteContext demoteContext)
            throws ExecutionException, InterruptedException
    {
        Map<RowGroupKey, List<TupleRank>> rowGroupDataWarmUpElementMap = tupleRankList.stream()
                .filter(tr -> !demoteContext.failedRowGropDataSet().contains(tr.rowGroupKey()))
                .collect(groupingBy(TupleRank::rowGroupKey, mapping(Function.identity(), Collectors.toList())));
        List<RowGroupKey> rowGroupDataList = List.copyOf(rowGroupDataWarmUpElementMap.keySet());
        logger.debug("going to demote %d rowGroupData", rowGroupDataWarmUpElementMap.size());
        List<ListenableFuture<DeletionStats>> rowGroupDeleteFutures = new ArrayList<>();
        long deletedObjects = 0;
        long deletedBytes = 0;
        try {
            for (RowGroupKey rowGroupKey : rowGroupDataList) {
                Callable<DeletionStats> callable = () -> {
                    RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
                    return deleteRowGroupData(rowGroupData, rowGroupDataWarmUpElementMap.get(rowGroupKey), demoteContext);
                };
                try {
                    rowGroupDeleteFutures.add(Futures.submit(callable, rowGroupExecutorService));
                }
                catch (RejectedExecutionException ree) {
                    logger.warn("retry to submit row group data %s", rowGroupKey);
                    try {
                        List<DeletionStats> deletionStats = Futures.allAsList(rowGroupDeleteFutures).get();
                        deletedObjects += deletionStats.stream().mapToLong(DeletionStats::objectCount).sum();
                        deletedBytes += deletionStats.stream().mapToLong(DeletionStats::sizeInBytes).sum();
                        rowGroupDeleteFutures = new ArrayList<>();
                        rowGroupDeleteFutures.add(Futures.submit(callable, rowGroupExecutorService));
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
        List<DeletionStats> deletionStats = Futures.allAsList(rowGroupDeleteFutures).get();
        deletedObjects += deletionStats.stream().mapToLong(DeletionStats::objectCount).sum();
        deletedBytes += deletionStats.stream().mapToLong(DeletionStats::sizeInBytes).sum();
        return new DeletionStats(deletedObjects, deletedBytes);
    }

    @VisibleForTesting
    DeletionStats deleteRowGroupData(RowGroupData rowGroupData, List<TupleRank> tupleRanksToDelete, DemoteContext demoteContext)
    {
        List<WarmUpElement> elementsToDelete = tupleRanksToDelete.stream()
                .map(TupleRank::warmUpElement)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        AtomicBoolean delete = new AtomicBoolean(true);
        if (rowGroupData.isEmpty()) {
            logger.debug("empty rowGropData %s", rowGroupData.getRowGroupKey());
            if (rowGroupData.getWarmUpElements().size() == elementsToDelete.size()) {
                logger.debug("empty rowGropData, deleting %s", rowGroupData.getRowGroupKey());
                rowGroupDataService.deleteData(rowGroupData, true);
            }
            else {
                if (demoteContext.isDeleteEmptyRowGroups()) {
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
                        .onFailedAttempt(_ -> retryFailure.incrementAndGet())
                        .handle(TrinoException.class)
                        .withMaxRetries(warmupDemoterConfig.getMaxRetriesAcquireThread()).handle(RuntimeException.class).build();
                Failsafe.with(retryPolicy).run(() -> {
                    workerCapacityManager.tryAllocateResourcesForWarmupTask();
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
                demoteContext.statsWarmupDemoter().addnumber_fail_acquire(retryFailure.get());
            }
        }
        if (delete.get()) {
            return new DeletionStats(
                    elementsToDelete.size(),
                    elementsToDelete.stream().mapToLong(element -> element.getEndOffset() - element.getStartOffset()).sum() << pageSizeShift);
        }
        return DeletionStats.EMPTY;
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
            logger.error(e,
                    "failed to demote rowGroupData: %s, row grop will be deleted",
                    rowGroupData.getRowGroupKey());
            handleFailDeleteRowGroup(rowGroupData, demoteContext);
            success = false;
        }
        finally {
            workerCapacityManager.decrementActiveWarmingTasks();
        }
        return success;
    }

    private void handleFailDeleteRowGroup(RowGroupData rowGroupData, DemoteContext demoteContext)
    {
        rowGroupDataService.removeElements(rowGroupData);
        demoteContext.failedRowGropDataSet().add(rowGroupData.getRowGroupKey());
        demoteContext.statsWarmupDemoter().incfailed_row_group_data();
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

    @Subscribe
    private void handleWarmupDemoterConfigChanged(WarmupDemoterConfigChangedEvent event)
    {
        logger.debug("handleWarmupDemoterConfigChanged=%s", event);
        initDefaultWarmupProperties();
    }
}
