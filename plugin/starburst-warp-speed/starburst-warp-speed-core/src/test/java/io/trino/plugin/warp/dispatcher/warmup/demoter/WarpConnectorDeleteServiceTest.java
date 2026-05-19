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
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.execution.debugtools.ColumnFilter;
import io.trino.plugin.warp.execution.debugtools.FileFilter;
import io.trino.plugin.warp.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.warp.warmup.model.WarmupPredicateRule;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterServiceTest.buildWarmupElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WarpConnectorDeleteServiceTest
{
    private static final String DEFAULT_FILE_PATH = "file_path";

    private final String defaultSchemaName = "schema1";
    private final String defaultTableName = "table1";
    private final WarmUpType defaultWarmupType = WarmUpType.WARM_UP_TYPE_BASIC;
    private final int defaultPriority = 0;
    private final int notEmptyTTL = 100;

    private WarpConnectorDeleteService warpDeleteService;
    private RowGroupDataService rowGroupDataService;

    private final Set<WarmupPredicateRule> defaultPredicates = Set.of();
    private final Map<RowGroupKey, RowGroupData> rowGroupDataMap = new HashMap<>();
    private final List<WarmupRule> warmupRules = new ArrayList<>();
    private final List<WarmUpElement> warmUpElements = new ArrayList<>();
    private final WarmupRuleProvider warmupRuleProvider = mock(WarmupRuleProvider.class);
    DemoteContext demoteContext;

    @BeforeEach
    public void before()
    {
        rowGroupDataService = mock(RowGroupDataService.class);
        WarmupDemoterConfig warmupDemoterConfig = new WarmupDemoterConfig();
        warmupDemoterConfig.setEnableDemote(true);
        warpDeleteService = spy(new WarpConnectorDeleteService(
                rowGroupDataService,
                warmupDemoterConfig,
                new NativeConfig(),
                warmupRuleProvider,
                mock(WorkerCapacityManager.class),
                new StubsStorageEngineConstants(),
                new EventBus()));
        int defaultBatchSize = 2;
        int defaultEpsilon = 1;
        double defaultMaxThreshold = 95;
        double defaultCleanThreshold = 90;
        demoteContext = new DemoteContext(
                defaultMaxThreshold,
                defaultCleanThreshold,
                defaultBatchSize,
                defaultBatchSize,
                defaultEpsilon,
                true,
                true,
                new TupleRankResult(new ArrayList<>(), new ArrayList<>(), List.of()));

        initDefaultMembers();
    }

    private void initDefaultMembers()
    {
        IntStream.range(0, 20).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, Map.of(), 0, false);
        rowGroupDataMap.put(rowGroupData.getRowGroupKey(), rowGroupData);
    }

    @Test
    public void testBuildTupleRank()
    {
        List<WarmupRule> warmupRules = new ArrayList<>();
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 10).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, index, defaultWarmupType, 10 - index, index, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        warmupRules.add(buildWarmupRule(defaultSchemaName, 10, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
        warmUpElements.add(buildWarmupElement(10, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC, false));
        RowGroupData rowGroupData = buildRowGroupData(
                defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(),
                0,
                false);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(9);
        assertThat(tupleRankResult.failedObjects().size()).isEqualTo(1);
        assertThat(tupleRankResult.immediateObjects().size()).isEqualTo(1);
    }

    @Test
    public void testTwoWarmupsOnSameColumn()
    {
        WarmupProperties propBasic = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 0, TransformFunction.NONE);
        WarmupProperties propData = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 10, 1000, TransformFunction.NONE);

        List<WarmupRule> warmupRules = buildWarmupRule(
                Set.of(propBasic, propData),
                defaultPredicates);

        List<WarmUpElement> warmUpElements = List.of(
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC),
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA));

        List<RowGroupData> rowGroupDataList = List.of(
                buildRowGroupData(
                        defaultSchemaName,
                        defaultTableName,
                        warmUpElements,
                        Map.of(),
                        0,
                        false));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);

        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(1);
        assertThat(tupleRankResult.immediateObjects().size()).isEqualTo(1);
    }

    @Test
    public void testWarmupDemoterFilterByColumn()
    {
        ColumnFilter columnFilter = createColumnFilter(List.of());
        executeFilterTest(List.of(columnFilter), 1);
    }

    @Test
    public void testWarmupDemoterFilterByWarmupElement()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_BASIC));

        executeFilterTest(List.of(columnFilterMatch), 1);
    }

    @Test
    public void testWarmupDemoterFilterByWarmupElementNonMatch()
    {
        ColumnFilter columnFilterNonMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_DATA));
        executeFilterTest(List.of(columnFilterNonMatch), 0);
    }

    @Test
    public void testWarmupDemoterFilterByFilePaths()
    {
        FileFilter fileFilter = new FileFilter(Set.of(DEFAULT_FILE_PATH + "_" + 0));
        executeFilterTest(List.of(fileFilter), 20);
    }

    @Test
    public void testWarmupDemoterFilterByFilePathsNonMatch()
    {
        FileFilter fileFilterNonMatch = new FileFilter(Set.of("non existing file path"));
        executeFilterTest(List.of(fileFilterNonMatch), 0);
    }

    @Test
    public void testWarmupDemoterTwoFilters()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_BASIC));
        FileFilter fileFilter = new FileFilter(Set.of(DEFAULT_FILE_PATH + "_" + 0));
        executeFilterTest(List.of(columnFilterMatch, fileFilter), 1);
    }

    private void executeFilterTest(List<TupleFilter> tupleFilters, int expectedDeletedByTupleFilter)
    {
        double highPriority = 8.5;
        warmupRules.add(buildWarmupRule(defaultSchemaName, 30, WarmUpType.WARM_UP_TYPE_BASIC, highPriority, notEmptyTTL, defaultPredicates));
        warmUpElements.add(buildWarmupElement(30, Instant.now().toEpochMilli()));
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMap.values()));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(tupleFilters, true);

        assertThat(tupleRankResult.immediateObjects().size()).isEqualTo(expectedDeletedByTupleFilter);
        assertThat(tupleRankResult.failedObjects().size()).isEqualTo(0);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(0);
    }

    @Test
    public void testBestMatchOverlappingPredicates()
    {
        WarpColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        int priorityHigh = 10;
        int ttlInTheFuture = 1;
        int weId = 1;

        Map<WarpColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);
        Set<WarmupPredicateRule> predicates = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(defaultSchemaName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates),
                buildWarmupRule(defaultSchemaName, weId, defaultWarmupType, priorityHigh, ttlInTheFuture, Set.of()));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli() + 10000));
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);

        assertThat(tupleRankResult.tupleRankList()).isNotEmpty();
    }

    @Test
    public void testBestMatchPredicatesNotMatch()
    {
        WarpColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        String partitionKey2 = "p2";
        String partitionValue2 = "v2";
        int priorityHigh = 10;
        int ttlInTheFuture = 1;
        int weId = 1;

        Map<WarpColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);

        Set<WarmupPredicateRule> predicates1 = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        Set<WarmupPredicateRule> predicates2 = new HashSet<>(List.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue), new PartitionValueWarmupPredicateRule(partitionKey2, partitionValue2)));
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(defaultSchemaName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates1),
                buildWarmupRule(defaultSchemaName, weId, defaultWarmupType, priorityHigh, ttlInTheFuture, predicates2));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli()));
        List<RowGroupData> rowGroupDataList = List.of(
                buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);

        assertThat(tupleRankResult.tupleRankList()).isNotEmpty();
    }

    @Test
    public void testForceDeleteFailedObjectsWithColumnFilter()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_BASIC));
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(defaultSchemaName, 0, WarmUpType.WARM_UP_TYPE_BASIC, defaultPriority, notEmptyTTL, defaultPredicates),
                buildWarmupRule(defaultSchemaName, 0, WarmUpType.WARM_UP_TYPE_DATA, defaultPriority, notEmptyTTL, defaultPredicates));
        WarmUpElement warmUpElementToDelete = buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC, false);
        WarmUpElement warmUpElementNotToDelete = buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false);
        List<WarmUpElement> warmUpElements = List.of(warmUpElementToDelete, warmUpElementNotToDelete);

        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, Map.of(), 0, false));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(columnFilterMatch), true);
        assertThat(tupleRankResult.immediateObjects().size()).isEqualTo(0);
        assertThat(tupleRankResult.failedObjects().size()).isEqualTo(1);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(0);
    }

    @Test
    public void testTwoWarmupsOnSameRowGroup()
    {
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(
                        defaultSchemaName,
                        0,
                        WarmUpType.WARM_UP_TYPE_BASIC,
                        1,
                        0,
                        defaultPredicates),
                buildWarmupRule(
                        defaultSchemaName,
                        1,
                        WarmUpType.WARM_UP_TYPE_DATA,
                        10,
                        1000,
                        defaultPredicates));

        List<WarmUpElement> warmUpElements = List.of(
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC),
                buildWarmupElement(1, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA));

        RowGroupData rowGroupData = buildRowGroupData(
                defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(),
                0,
                false);

        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(
                List.of(new ColumnFilter(
                        new SchemaTableName(
                                rowGroupData.getRowGroupKey().schema(),
                                rowGroupData.getRowGroupKey().table()),
                        List.of(
                                new WarmupDemoterWarmupElementData(
                                        warmUpElements.get(1).getWarpColumn().getName(),
                                        List.of(warmUpElements.get(1).getWarmUpType()))))),
                true);

        assertThat(tupleRankResult.immediateObjects().size()).isEqualTo(1);
    }

    @Test
    public void testMatchPredicates()
    {
        WarpColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        String partitionValue2 = "v2";
        int priority2 = 5;
        String schemaName2 = "schema2";

        int ttlInTheFuture = 1;
        int weId = 1;

        Map<WarpColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);
        Map<WarpColumn, String> hivePartitionKeys2 = Map.of(partitionKey, partitionValue2);
        Set<WarmupPredicateRule> predicates = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        Set<WarmupPredicateRule> predicates2 = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue2));
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(defaultSchemaName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates),
                buildWarmupRule(schemaName2, weId, defaultWarmupType, priority2, ttlInTheFuture, predicates2));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli()));
        List<RowGroupData> rowGroupDataList = List.of(
                buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false),
                buildRowGroupData(schemaName2, defaultTableName, warmUpElements, hivePartitionKeys2, 1, false));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);

        assertThat(tupleRankResult.tupleRankList()).isNotEmpty();
    }

    @Test
    public void testDeleteRowGroupData()
    {
        IntStream.range(0, 10).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, index, defaultWarmupType, 10 - index, index, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        warmupRules.add(buildWarmupRule(defaultSchemaName, 10, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
        warmUpElements.add(buildWarmupElement(10, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC, false));
        RowGroupData rowGroupData = buildRowGroupData(
                defaultSchemaName,
                defaultTableName,
                List.of(),
                Map.of(),
                0,
                true);
        warpDeleteService.deleteRowGroupData(rowGroupData, List.of(), demoteContext);
        verify(rowGroupDataService, times(1)).deleteData(eq(rowGroupData), eq(true));
    }

    @Test
    public void testDemoteAllEmptyRowGroup()
    {
        List<WarmUpElement> elements = IntStream.range(0, 20)
                .mapToObj(index -> buildWarmupElement(index, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false))
                .collect(Collectors.toList());

        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, elements, Map.of(), 0, true));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(rowGroupDataService.get(eq(rowGroupDataList.getFirst().getRowGroupKey()))).thenReturn(rowGroupDataList.getFirst());
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);
        assertThat(tupleRankResult.failedObjects().size()).isEqualTo(20);

        warpDeleteService.deleteRowGroupData(
                rowGroupDataList.getFirst(),
                tupleRankResult.failedObjects(),
                demoteContext);
        verify(rowGroupDataService, times(1)).deleteData(eq(rowGroupDataList.getFirst()), eq(true));
    }

    @Test
    public void testDeleteAllWarmupElements()
    {
        RowGroupData rowGroupData = rowGroupDataMap.values().iterator().next();
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMap.values()));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(20);

        warpDeleteService.deleteRowGroupData(
                rowGroupData,
                tupleRankResult.tupleRankList(),
                demoteContext);
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData), argThat(list -> list.size() == 20));
    }

    @Test
    public void testDeletePartialWarmupElements()
    {
        RowGroupData rowGroupData = rowGroupDataMap.values().iterator().next();
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMap.values()));
        when(warmupRuleProvider.getAll()).thenReturn(warmupRules);
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(20);

        warpDeleteService.deleteRowGroupData(
                rowGroupData,
                tupleRankResult.tupleRankList().subList(0, 5),
                demoteContext);
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData), argThat(list -> list.size() == 5));
    }

    @Test
    public void testDeleteNativeFailure()
            throws ExecutionException, InterruptedException
    {
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        List<WarmupRule> warmupRuleList = new ArrayList<>();
        IntStream.range(0, 4).forEach(index -> {
            warmUpElementList.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
            warmupRuleList.add(buildWarmupRule(defaultSchemaName, index, defaultWarmupType, 10 - index, index, defaultPredicates));
        });
        RowGroupData rowGroupData1 = buildRowGroupData(defaultSchemaName, "aaa", warmUpElementList, Map.of(), 1, false);
        RowGroupData rowGroupData2 = buildRowGroupData(defaultSchemaName, "bbb", warmUpElementList, Map.of(), 2, false);
        Map<RowGroupKey, RowGroupData> rowGroupDataMapTest = new HashMap<>();
        rowGroupDataMapTest.put(rowGroupData1.getRowGroupKey(), rowGroupData1);
        rowGroupDataMapTest.put(rowGroupData2.getRowGroupKey(), rowGroupData2);

        when(warmupRuleProvider.getAll()).thenReturn(warmupRuleList);
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMapTest.values()));
        when(rowGroupDataService.get(any())).thenAnswer(i -> rowGroupDataMapTest.get(i.getArguments()[0]));
        doThrow(new TrinoException(WarpErrorCode.WARP_NATIVE_ERROR, "test"))
                .doNothing()
                .when(rowGroupDataService).removeElements(eq(rowGroupData1), anyCollection());
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(List.of(), true);
        assertThat(tupleRankResult.tupleRankList().size()).isEqualTo(8);

        DemoteContext demoteContextTmp = new DemoteContext(
                demoteContext.maxUsageThresholdPercentage(),
                demoteContext.cleanupUsageThresholdPercentage(),
                demoteContext.batchSize(),
                demoteContext.batchSize(),
                demoteContext.epsilon(),
                false,
                true,
                new TupleRankResult(new ArrayList<>(), new ArrayList<>(), List.of()));
        warpDeleteService.delete(
                tupleRankResult.tupleRankList(),
                demoteContextTmp);
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData1), anyCollection());
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData1));
    }

    @Test
    public void testIsDeleteImmediatelyObject()
    {
        WarmUpElement warmUpElement = buildWarmupElement(1, Instant.now().toEpochMilli());

        RowGroupData rowGroupData1 = buildRowGroupData(
                defaultSchemaName,
                "aaa",
                List.of(warmUpElement),
                Map.of(),
                1,
                false);

        when(warmupRuleProvider.getAll())
                .thenReturn(List.of(buildWarmupRule(
                        defaultSchemaName,
                        1,
                        defaultWarmupType,
                        10,
                        1,
                        defaultPredicates)));

        TupleRank tupleRank = new TupleRank(
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 0, TransformFunction.NONE),
                warmUpElement,
                rowGroupData1.getRowGroupKey());

        assertThat(warpDeleteService.isDeleteImmediatelyObject(tupleRank, Instant.now().minusSeconds(1), List.of()))
                .isFalse();
        assertThat(warpDeleteService.isDeleteImmediatelyObject(tupleRank, Instant.now(), List.of()))
                .isTrue();
        assertThat(warpDeleteService.isDeleteImmediatelyObject(tupleRank, Instant.now().plusSeconds(1), List.of()))
                .isTrue();
    }

    private WarmupRule buildWarmupRule(
            String schema,
            int weId,
            WarmUpType warmUpType,
            double priority,
            int ttl,
            Set<WarmupPredicateRule> predicates)
    {
        return WarmupRule.builder()
                .schema(schema)
                .table("table1")
                .warpColumn(new RegularColumn("c" + weId))
                .warmUpType(warmUpType)
                .priority(priority)
                .ttl(ttl)
                .predicates(predicates)
                .build();
    }

    private List<WarmupRule> buildWarmupRule(
            Set<WarmupProperties> warmupProperties,
            Set<WarmupPredicateRule> predicates)
    {
        return warmupProperties.stream()
                .map(prop -> WarmupRule.builder()
                        .schema("schema1")
                        .table("table1")
                        .warpColumn(new RegularColumn("c" + 0))
                        .warmUpType(prop.warmUpType())
                        .priority(prop.priority())
                        .ttl(prop.ttl())
                        .predicates(predicates)
                        .build())
                .toList();
    }

    public static RowGroupData buildRowGroupData(String schemaName, String tableName, List<WarmUpElement> warmUpElements, Map<WarpColumn, String> hivePartitionKeys, int fileIndex, boolean isEmpty)
    {
        return RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(schemaName, tableName, DEFAULT_FILE_PATH + "_" + fileIndex, 0, 1L, 0, "", ""))
                .warmUpElements(warmUpElements)
                .partitionKeys(hivePartitionKeys)
                .isEmpty(isEmpty)
                .build();
    }

    private ColumnFilter createColumnFilter(List<WarmUpType> warmupTypes)
    {
        return new ColumnFilter(
                new SchemaTableName(defaultSchemaName, defaultTableName),
                List.of(new WarmupDemoterWarmupElementData("c0", warmupTypes)));
    }
}
