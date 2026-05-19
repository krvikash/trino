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
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterServiceTest.buildWarmupElement;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarpDeleteService.DeletionStats;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarpCacheManagerDeleteServiceTest
{
    @Mock
    private RowGroupDataService rowGroupDataService;
    @Mock
    private CacheMgrWarmupRuleService cacheMgrWarmupRuleService;

    private WarpCacheManagerDeleteService warpCacheManagerDeleteService;
    private DemoteContext demoteContext;

    public static final String SCHEMA = "schema";
    public static final String TABLE_1 = "table1";
    public static final String FILE = "file";

    @BeforeEach
    public void before()
    {
        MockitoAnnotations.openMocks(this);
        warpCacheManagerDeleteService = new WarpCacheManagerDeleteService(
                rowGroupDataService,
                cacheMgrWarmupRuleService,
                new WarmupDemoterConfig(),
                new StubsStorageEngineConstants(),
                new NativeConfig(),
                new EventBus());
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
    }

    @Test
    void testBuildTupleRank_noRules()
    {
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 2).forEach(index -> {
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });

        List<RowGroupData> rowGroupDataList = new ArrayList<>();
        rowGroupDataList.add(createRowGroupData(warmUpElements));

        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(cacheMgrWarmupRuleService.getAll()).thenReturn(new HashMap<>()); // No rules

        TupleRankResult result = warpCacheManagerDeleteService.buildTupleRank(List.of(), true);
        assertThat(result.tupleRankList().size()).isEqualTo(1);
        assertThat(result.failedObjects().size()).isEqualTo(0);
        assertThat(result.immediateObjects().size()).isEqualTo(0);

        WarmupDemoterConfig warmupDemoterConfig = new WarmupDemoterConfig();
        WarmupProperties expectedProperties = new WarmupProperties(
                WarmUpType.WARM_UP_TYPE_DATA,
                10,
                warmupDemoterConfig.getDefaultRuleTtlInSeconds(),
                TransformFunction.NONE); // from defaultWarmupProperties
        TupleRank tupleRank = result.tupleRankList().getFirst();

        assertThat(tupleRank.warmupProperties()).isEqualTo(expectedProperties);
    }

    @Test
    void testBuildTupleRank_withRule()
    {
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        Map<String, CacheManagerRule> rules = new HashMap<>();
        IntStream.range(0, 2).forEach(index -> {
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });

        List<RowGroupData> rowGroupDataList = new ArrayList<>();
        rowGroupDataList.add(createRowGroupData(warmUpElements));

        rules.put(TABLE_1, new CacheManagerRule("signature", 5, Duration.ofSeconds(100)));

        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(cacheMgrWarmupRuleService.getAll()).thenReturn(rules);

        TupleRankResult result = warpCacheManagerDeleteService.buildTupleRank(List.of(), true);
        assertThat(result.tupleRankList().size()).isEqualTo(1);
        assertThat(result.failedObjects().size()).isEqualTo(0);
        assertThat(result.immediateObjects().size()).isEqualTo(0);

        WarmupProperties expectedProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 5, 100, TransformFunction.NONE); // from CacheManagerRule
        TupleRank tupleRank = result.tupleRankList().getFirst();

        assertThat(tupleRank.warmupProperties()).isEqualTo(expectedProperties);
    }

    @Test
    void testBuildTupleRank_forceDeleteFailedObjects()
    {
        long lastUsed = Instant.now().toEpochMilli();
        List<RowGroupData> rowGroupDataList = new ArrayList<>();
        WarmUpElement warmUpElementValid = buildWarmupElement(0, lastUsed, WarmUpType.WARM_UP_TYPE_NUM_OF, true);
        WarmUpElement warmUpElementNotValid = buildWarmupElement(1, lastUsed, WarmUpType.WARM_UP_TYPE_NUM_OF, false);
        rowGroupDataList.add(createRowGroupData(List.of(warmUpElementValid, warmUpElementNotValid)));

        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(cacheMgrWarmupRuleService.getAll()).thenReturn(new HashMap<>()); // No rules

        TupleRankResult result = warpCacheManagerDeleteService.buildTupleRank(List.of(), true);
        assertThat(result.tupleRankList().size()).isEqualTo(0);
        assertThat(result.failedObjects().size()).isEqualTo(1);
        assertThat(result.immediateObjects().size()).isEqualTo(0);
    }

    @Test
    void testBuildTupleRank_deleteImmediately()
    {
        List<RowGroupData> rowGroupDataList = new ArrayList<>();
        List<WarmUpElement> warmupElements = new ArrayList<>();
        IntStream.range(0, 2)
                .forEach(index -> warmupElements.add(buildWarmupElement(index, Instant.now().toEpochMilli() - 10)));

        rowGroupDataList.add(createRowGroupData(warmupElements));

        Map<String, CacheManagerRule> rules = new HashMap<>();
        rules.put(TABLE_1, new CacheManagerRule("signature", 5, Duration.ofSeconds(0)));

        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(cacheMgrWarmupRuleService.getAll()).thenReturn(rules);

        TupleRankResult result = warpCacheManagerDeleteService.buildTupleRank(List.of(), false);
        assertThat(result.tupleRankList().size()).isEqualTo(0);
        assertThat(result.failedObjects().size()).isEqualTo(0);
        assertThat(result.immediateObjects().size()).isEqualTo(1);
    }

    @Test
    void testDelete()
            throws ExecutionException, InterruptedException
    {
        RowGroupKey rowGroupKey = new RowGroupKey(SCHEMA, "table", FILE, 0, 0, 0, "", "");
        List<WarmUpElement> warmupElements = new ArrayList<>();
        IntStream.range(0, 2)
                .forEach(index -> warmupElements.add(buildWarmupElement(index, Instant.now().toEpochMilli())));
        RowGroupData rowGroupData = createRowGroupData(warmupElements);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        WarmupDemoterConfig warmupDemoterConfig = new WarmupDemoterConfig();
        WarmupProperties warmupProperties = new WarmupProperties(
                WarmUpType.WARM_UP_TYPE_DATA,
                10,
                warmupDemoterConfig.getDefaultRuleTtlInSeconds(),
                TransformFunction.NONE); // from defaultWarmupProperties

        TupleRank tupleRank = new TupleRank(warmupProperties, null, rowGroupKey);
        List<TupleRank> tupleRanks = List.of(tupleRank);

        DeletionStats deletionStats = warpCacheManagerDeleteService.delete(tupleRanks, demoteContext);

        verify(rowGroupDataService).get(rowGroupKey);
        verify(rowGroupDataService).deleteData(rowGroupData, true);
//        assertThat(deletedCount).isEqualTo(2); // see comment on `WarpCacheManagerDeleteService.delete` line 134
        assertThat(deletionStats.objectCount()).isEqualTo(1);
    }

    private RowGroupData createRowGroupData(List<WarmUpElement> warmUpElements)
    {
        RowGroupKey rowGroupKey = new RowGroupKey(SCHEMA, TABLE_1, FILE, 0, 1L, 0, "", "");
        return RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .build();
    }
}
