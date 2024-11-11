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
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmupDemoterServiceTest
{
    private static final String DEFAULT_FILE_PATH = "file_path";

    private final WarmUpType defaultWarmupType = WarmUpType.WARM_UP_TYPE_BASIC;
    private final int defaultPriority = 0;
    private final int defaultEpsilon = 1;
    private final double defaultMaxThreshold = 95;
    private final double defaultCleanThreshold = 90;
    private final int defaultBatchSize = 2;
    private final long defaultMaxElementsToDemote = 100;

    private WorkerCapacityManager workerCapacityManager;
    private WarmupDemoterService warmupDemoterService;
    private WarmupDemoterConfig warmupDemoterConfig;
    private WarpDeleteService deleteService;
    private ConnectorSync connectorSync;
    private MetricsManager metricsManager;

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
        when(deleteService.delete(anyList(), any(), anyBoolean())).thenAnswer(invocation -> {
            List<TupleRank> arg = invocation.getArgument(0); // Get the argument passed
            return (long) arg.size();
        });
        metricsManager = TestingTxService.createMetricsManager();

        FlowsSequencer flowsSequencer = spy(new FlowsSequencer(metricsManager));
        connectorSync = mock(ConnectorSync.class);
        warmupDemoterConfig = new WarmupDemoterConfig();
        warmupDemoterConfig.setEnableDemote(true);
        CatalogNameProvider catalogNameProvider = new CatalogNameProvider("catalogTest");
        warmupDemoterService = spy(new WarmupDemoterService(
                workerCapacityManager,
                warmupDemoterConfig,
                metricsManager,
                flowsSequencer,
                connectorSync,
                catalogNameProvider,
                eventBus,
                deleteService));
    }

    @Test
    public void testRunWarmupDemoterWithStatusExecuting()
    {
        assertThat(warmupDemoterService.trySetIsExecutingToTrue()).isTrue();
        warmupDemoterService.tryDemoteStart();
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
        assertThat(warmupDemoterStats.getnot_executed_due_is_already_executing()).isEqualTo(1);
    }

    @Test
    public void testRunFail()
    {
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        warmupDemoterStats.setcurrentUsage(1000);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, 0, 1, List.of());
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
        assertThat(warmupDemoterStats.getcurrentUsage()).isEqualTo(1000);
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(0);
        warmupDemoterService.tryDemoteStart();
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(warmupDemoterStats.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(warmupDemoterStats.getnumber_of_runs()).isEqualTo(0);
    }

    @Test
    public void testDeadObject()
    {
        TupleRankResult tupleRankResult = new TupleRankResult();
        IntStream.range(0, 20).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankResult.immediateObjects().add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });

        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);

        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean())).thenReturn(tupleRankResult);
        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(20);
        verify(connectorSync, times(1)).syncDemoteEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testFailedObjects()
    {
        TupleRankResult tupleRankResult = new TupleRankResult();
        IntStream.range(0, 20).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankResult.failedObjects().add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean())).thenReturn(tupleRankResult);
        warmupDemoterService.setForceDeleteFailedObjects(true);
        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());

        assertThat(warmupDemoterService.getCurrentRunStats().getfailed_objects_deleted()).isEqualTo(20);
        verify(connectorSync, times(1)).syncDemoteEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testDemoteAll()
    {
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);
        TupleRankResult tupleRankResult = new TupleRankResult();
        IntStream.range(0, 20).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankResult.tupleRankList().add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });
        setConfig(0, 0, 1, 100, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean())).thenReturn(tupleRankResult);
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        assertThat(warmupDemoterService.getDemoterHighestPriority().get()).isEqualTo(0);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isGreaterThan(1);
        verify(connectorSync, times(1)).syncDemoteEnd(eq(warmupDemoterService.getCurrentRunSequence()), eq(0D), eq(0D), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testWarmupDemoterDecreaseMemoryUsageUntilCleanThreshold()
    {
        AtomicDouble usageCapacity = new AtomicDouble(0.92D);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal())
                .thenAnswer(_ -> {
                    usageCapacity.set(usageCapacity.addAndGet(-0.1));
                    return usageCapacity.get();
                });

        TupleRankResult tupleRankResult = new TupleRankResult();
        IntStream.range(0, 100).forEach(index -> {
            WarmupProperties warmupProperties = new WarmupProperties(defaultWarmupType, defaultPriority, 1, TransformFunction.NONE);
            RowGroupKey rowGroupKey = buildRowGroupKey(index);
            tupleRankResult.tupleRankList().add(new TupleRank(warmupProperties, buildWarmupElement(index, 0), rowGroupKey));
        });

        setConfig(85, 80, 2, 100, List.of());
        when(deleteService.buildTupleRank(anyList(), anyBoolean())).thenReturn(tupleRankResult);
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        verify(connectorSync, times(1)).syncDemoteEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED));
        verify(connectorSync, times(1)).syncDemoteEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD));
        assertThat(usageCapacity.get()).isLessThan(0.88);
        assertThat(usageCapacity.get()).isGreaterThan(0.5);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isEqualTo(0);
        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(0);
    }

    @Test
    public void testInitiateSyncDemoteProcessRequestRejected()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(-1);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterService.initiateDemoteProcess();
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(warmupDemoterStats.getnot_executed_due_sync_demote_start_rejected()).isEqualTo(1);
    }

    @Test
    public void testInitiateSyncDemoteProcessArgsNotValid()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(1);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        int demoteProcess = warmupDemoterService.initiateDemoteProcess();
        warmupDemoterService.initDemoteContext(demoteProcess);
        warmupDemoterService.initiateDemoteProcess();
        WarmupDemoterStats warmupDemoterStats = (WarmupDemoterStats) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(warmupDemoterStats.getnot_executed_due_is_already_executing()).isEqualTo(1);
    }

    @Test
    public void testInitiateSyncDemoteProcess()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(1);
        setConfig(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterService.initiateDemoteProcess();
    }

    @Test
    public void testTupleRankSort()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "/", 0, 0, 0, "", "");
        WarmupProperties warmupProperties1 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 3, 80, TransformFunction.NONE);
        TupleRank t1 = new TupleRank(warmupProperties1, null, rowGroupKey);
        WarmupProperties warmupProperties2 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 80, TransformFunction.NONE);
        TupleRank t2 = new TupleRank(warmupProperties2, null, rowGroupKey);
        WarmupProperties warmupProperties3 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 2, 80, TransformFunction.NONE);
        TupleRank t3 = new TupleRank(warmupProperties3, null, rowGroupKey);
        List<TupleRank> list = new ArrayList<>(List.of(t1, t2, t3));
        Collections.sort(list);
        assertThat(list.get(0)).isEqualTo(t2);
        assertThat(list.get(1)).isEqualTo(t3);
        assertThat(list.get(2)).isEqualTo(t1);
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
        warmupDemoterService.setTupleFilters(tupleFilters);
        warmupDemoterService.setForceDeleteDeadObjects(false);
    }

    private RowGroupKey buildRowGroupKey(int fileIndex)
    {
        return new RowGroupKey("schema1", "table1", DEFAULT_FILE_PATH + "_" + fileIndex, 0, 1L, 0, "", "");
    }
}
