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

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AtomicDouble;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.util.NodeUtils;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmupDemoterServiceTest
{
    private static final String DEFAULT_FILE_PATH = "file_path";

    private final WarmUpType defaultWarmupType = WarmUpType.WARM_UP_TYPE_BASIC;
    private final int defaultPriority = 0;
    private final double defaultMaxThreshold = 95;
    private final double defaultCleanThreshold = 90;
    private final int defaultBatchSize = 2;
    private final long defaultMaxElementsToDemote = 100;

    private WorkerCapacityManager workerCapacityManager;
    private WarmupDemoterService warmupDemoterService;
    private WarmupDemoterConfig warmupDemoterConfig;
    private DemoterSync demoterSync;
    private WarpDeleteService deleteService;
    private MetricsManager metricsManager;
    private long demoteKey;

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed)
    {
        return buildWarmupElement(columnId, lastUsed, WarmUpType.WARM_UP_TYPE_BASIC, true);
    }

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed, WarmUpType warmUpType)
    {
        return buildWarmupElement(columnId, lastUsed, warmUpType, true);
    }

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed, WarmUpType warmUpType, boolean isValid)
    {
        return WarmUpElement.builder()
                .warmUpType(warmUpType)
                .state(isValid ? WarmUpElementState.VALID : WarmUpElementState.FAILED_PERMANENTLY)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .totalRecords(10)
                .colName("c" + columnId)
                .lastUsedTimestamp(lastUsed)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
    }

    @BeforeEach
    public void before()
            throws ExecutionException, InterruptedException
    {
        workerCapacityManager = mock(WorkerCapacityManager.class);
        EventBus eventBus = mock(EventBus.class);
        deleteService = mock(WarpDeleteService.class);
        when(deleteService.delete(anyList(), any())).thenAnswer(invocation -> {
            List<TupleRank> arg = invocation.getArgument(0); // Get the argument passed
            return new WarpDeleteService.DeletionStats(arg.size(), 0);
        });
        metricsManager = TestingTxService.createMetricsManager();

        CatalogName catalogName = new CatalogName("catalog_test");
        NodeManager nodeManager = NodeUtils.mockNodeManager();
        FlowsSequencer flowsSequencer = new FlowsSequencer();

        demoterSync = mock(DemoterSync.class);
        warmupDemoterConfig = new WarmupDemoterConfig();
        demoteKey = 0;

        warmupDemoterService = new WarmupDemoterService(
                workerCapacityManager,
                warmupDemoterConfig,
                metricsManager,
                demoterSync,
                catalogName,
                eventBus,
                deleteService,
                nodeManager,
                flowsSequencer,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
    }

    @Test
    public void testRunWarmupDemoterWithStatusExecuting()
    {
        //initialize demote context as if one is running now
        warmupDemoterService.initDemoteContext(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());

        assertThat(warmupDemoterService.tryDemoteStart()).isFalse();
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WarmupDemoterStats.createKey());
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
        assertThat(warmupDemoterStats.getnot_executed_due_is_already_executing()).isEqualTo(1);
    }

    @Test
    public void testRunFail()
    {
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WarmupDemoterStats.createKey());
        warmupDemoterStats.setcurrentUsage(1000);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, 0, 1, List.of());
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
        assertThat(warmupDemoterStats.getcurrentUsage()).isEqualTo(1000);
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(0);

        assertThat(warmupDemoterService.tryDemoteStart()).isFalse();
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
    }

    @Test
    public void testDeadObject()
    {
        List<TupleRank> immediateObjects = new ArrayList<>();
        IntStream.range(0, 200).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            immediateObjects.add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });

        when(workerCapacityManager.getFractionCurrentUsageFromTotal())
                .thenReturn(0.98, 0.98, 0.9);

        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean()))
                .thenReturn(new TupleRankResult(new ArrayList<>(), immediateObjects, List.of()));
        when(demoterSync.tryStartDemoteProcess(
                demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority()))
                .thenReturn(true);

        assertThat(warmupDemoterService.tryDemoteStart()).isTrue();

        warmupDemoterService.connectorSyncStartDemote(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());

        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(100);
        verify(demoterSync, times(1))
                .finishDemoteProcess(eq(demoteKey),
                        anyDouble(),
                        anyDouble(),
                        eq(DemoteStatus.NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testFailedObjects()
    {
        List<TupleRank> failedObjects = new ArrayList<>();
        IntStream.range(0, 20).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            failedObjects.add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterConfig.setForceDeleteFailedObjects(true);

        when(deleteService.buildTupleRank(anyList(), anyBoolean()))
                .thenReturn(new TupleRankResult(new ArrayList<>(), new ArrayList<>(), failedObjects));
        when(demoterSync.tryStartDemoteProcess(
                eq(demoteKey),
                eq(warmupDemoterConfig.getMaxUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getCleanupUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getBatchSize()),
                eq(warmupDemoterConfig.getMaxElementsToDemoteInIteration()),
                eq(warmupDemoterConfig.getEpsilon()),
                eq(warmupDemoterConfig.isDeleteEmptyRowGroups()),
                eq(warmupDemoterConfig.isForceDeleteFailedObjects()),
                eq(warmupDemoterConfig.isResetHighestPriority())))
                .thenReturn(true);

        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);

        assertThat(warmupDemoterService.tryDemoteStart()).isTrue();

        warmupDemoterService.connectorSyncStartDemote(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());

        assertThat(warmupDemoterService.getCurrentRunStats().getfailed_objects_deleted()).isEqualTo(20);
        verify(demoterSync, times(1))
                .finishDemoteProcess(eq(demoteKey),
                        anyDouble(),
                        anyDouble(),
                        eq(DemoteStatus.NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testDemoteAll()
    {
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);

        List<TupleRank> tupleRankList = new ArrayList<>();
        IntStream.range(0, 20).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankList.add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });
        setConfig(0, 0, 1, 100, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean()))
                .thenReturn(new TupleRankResult(tupleRankList, new ArrayList<>(), List.of()));
        when(demoterSync.tryStartDemoteProcess(
                demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority()))
                .thenReturn(true);

        assertThat(warmupDemoterService.tryDemoteStart()).isTrue();

        warmupDemoterService.connectorSyncStartDemote(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isGreaterThan(1);
        verify(demoterSync, times(1))
                .finishDemoteProcess(eq(demoteKey),
                        eq(Double.MIN_VALUE),
                        eq(0d),
                        eq(DemoteStatus.NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testWarmupDemoterDecreaseMemoryUsageUntilCleanThreshold()
    {
        AtomicDouble usageCapacity = new AtomicDouble(0.92d);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal())
                .thenAnswer(_ -> {
                    usageCapacity.set(usageCapacity.addAndGet(-0.1));
                    return usageCapacity.get();
                });

        List<TupleRank> tupleRankList = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankList.add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });

        setConfig(85, 80, 2, 100, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean()))
                .thenReturn(new TupleRankResult(tupleRankList, new ArrayList<>(), List.of()));
        when(demoterSync.tryStartDemoteProcess(
                demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority()))
                .thenReturn(true);

//        assertThat(warmupDemoterService.tryDemoteStart()).isTrue();

        warmupDemoterService.connectorSyncStartDemote(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, false);

        verify(demoterSync, times(1))
                .finishDemoteProcess(eq(demoteKey),
                        anyDouble(),
                        anyDouble(),
                        eq(DemoteStatus.NOT_COMPLETED));
        verify(demoterSync, times(1))
                .finishDemoteProcess(eq(demoteKey),
                        anyDouble(),
                        anyDouble(),
                        eq(DemoteStatus.REACHED_THRESHOLD));
        assertThat(usageCapacity.get()).isLessThan(0.88);
        assertThat(usageCapacity.get()).isGreaterThan(0.5);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isEqualTo(0);
        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(0);
    }

    @Test
    public void testInitiateSyncDemoteProcessRequestRejected()
    {
        when(demoterSync.tryStartDemoteProcess(
                demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority()))
                .thenReturn(false);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());

        assertThat(warmupDemoterService.initiateDemoteProcess()).isFalse();

        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WarmupDemoterStats.createKey());
        assertThat(warmupDemoterStats.getnot_executed_due_sync_demote_start_rejected()).isEqualTo(1);
    }

    @Test
    public void testInitiateSyncDemoteProcessArgsNotValid()
    {
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        when(demoterSync.tryStartDemoteProcess(
                eq(demoteKey),
                eq(warmupDemoterConfig.getMaxUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getCleanupUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getBatchSize()),
                eq(warmupDemoterConfig.getMaxElementsToDemoteInIteration()),
                eq(warmupDemoterConfig.getEpsilon()),
                eq(warmupDemoterConfig.isDeleteEmptyRowGroups()),
                eq(warmupDemoterConfig.isForceDeleteFailedObjects()),
                eq(warmupDemoterConfig.isResetHighestPriority())))
                .thenReturn(true);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);

        assertThat(warmupDemoterService.initiateDemoteProcess()).isTrue();

        warmupDemoterService.initDemoteContext(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority());
        assertThat(warmupDemoterService.initiateDemoteProcess()).isTrue();
    }

    @Test
    public void testInitiateSyncDemoteProcess()
    {
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(100d);
        when(demoterSync.tryStartDemoteProcess(
                eq(demoteKey),
                eq(warmupDemoterConfig.getMaxUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getCleanupUsageThresholdPercentage()),
                eq(warmupDemoterConfig.getBatchSize()),
                eq(warmupDemoterConfig.getMaxElementsToDemoteInIteration()),
                eq(warmupDemoterConfig.getEpsilon()),
                eq(warmupDemoterConfig.isDeleteEmptyRowGroups()),
                eq(warmupDemoterConfig.isForceDeleteFailedObjects()),
                eq(warmupDemoterConfig.isResetHighestPriority())))
                .thenReturn(true);

        assertThat(warmupDemoterService.initiateDemoteProcess()).isTrue();
    }

    @Test
    public void testTupleRankSort()
    {
        RowGroupKey rowGroupKey = buildRowGroupKey(0);
        TupleRank t1 = new TupleRank(
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 3, 80, TransformFunction.NONE),
                null,
                rowGroupKey);
        TupleRank t2 = new TupleRank(
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 80, TransformFunction.NONE),
                null,
                rowGroupKey);
        TupleRank t3 = new TupleRank(
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 2, 80, TransformFunction.NONE),
                null,
                rowGroupKey);

        List<TupleRank> list = new ArrayList<>(List.of(t1, t2, t3));
        Collections.sort(list);

        assertThat(list).containsExactlyElementsOf(List.of(t2, t3, t1));
    }

    private void setConfig(double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemote,
            List<TupleFilter> tupleFilters)
    {
        warmupDemoterConfig.setBatchSize(batchSize);
        warmupDemoterConfig.setMaxUsageThresholdPercentage(maxUsageThresholdPercentage);
        warmupDemoterConfig.setCleanupUsageThresholdPercentage(cleanupUsageThresholdPercentage);
        warmupDemoterConfig.setEpsilon(1);
        warmupDemoterConfig.setMaxElementsToDemoteInIteration(maxElementsToDemote);
        warmupDemoterConfig.setForceDeleteDeadObjects(false);
        warmupDemoterService.setTupleFilters(tupleFilters);
    }

    private RowGroupKey buildRowGroupKey(int fileIndex)
    {
        return new RowGroupKey("schema1", "table1", DEFAULT_FILE_PATH + "_" + fileIndex, 0, 1L, 0, "", "");
    }
}
