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
package io.trino.plugin.warp.storage.read;

import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.PredicateBufferInfo;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WarpPageSourceTest
{
    private StorageEngineConstants storageEngineConstants;
    private CustomStatsContext customStatsContext;
    private ShapingLoggerFactory shapingLoggerFactory;

    @BeforeEach
    public void before()
    {
        this.storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getChunkSizeShift()).thenReturn(16);
        when(storageEngineConstants.getMaxChunksInRange()).thenReturn(8);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        this.customStatsContext = new CustomStatsContext(metricsManager);
        customStatsContext.getOrRegister(new DispatcherPageSourceStats());
        shapingLoggerFactory = new ShapingLoggerFactory(new CatalogName("catalog-name"), new SharedConfig());
    }

    @Disabled
    @Test
    public void testValidPage()
    {
        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                Integer.MAX_VALUE,
                Optional.empty(),
                mock(QueryParams.class),
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                mock(StorageCollectorService.class),
                mock(MatchService.class),
                mock(WorkerMemoryManager.class));
        SourcePage nextPage = warpPageSource.getNextSourcePage();
        assertThat(nextPage).isNotNull();
        assertThat(warpPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testMatchCollect()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(schemaTableColumn.warpColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        BasicQueryMatchData.Builder queryMatchDataBuilder = new BasicQueryMatchData.Builder();
        queryMatchDataBuilder
                .warpColumn(warmUpElement.getWarpColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.TINY), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

/*        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder()
                .blockIndex(0)
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .warpColumn(warmUpElement.getWarpColumn())
                .matchCollectType(MatchCollectType.ORDINARY);*/

        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                Integer.MAX_VALUE,
                Optional.empty(),
                mock(QueryParams.class),
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                mock(StorageCollectorService.class),
                mock(MatchService.class),
                mock(WorkerMemoryManager.class));
        assertThat(warpPageSource.getNextSourcePage()).isNotNull();
        assertThat(warpPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testMatchOnly()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(schemaTableColumn.warpColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        BasicQueryMatchData.Builder queryMatchDataBuilder = new BasicQueryMatchData.Builder();
        queryMatchDataBuilder
                .warpColumn(warmUpElement.getWarpColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.TINY), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                Integer.MAX_VALUE,
                Optional.empty(),
                mock(QueryParams.class),
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                mock(StorageCollectorService.class),
                mock(MatchService.class),
                mock(WorkerMemoryManager.class));
        assertThat(warpPageSource.getNextSourcePage()).isNotNull();
        assertThat(warpPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testCollect()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(schemaTableColumn.warpColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);

        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder();
        nativeCollectBuilder.blockIndex(0).type(IntegerType.INTEGER).warmUpElement(warmUpElement).warpColumn(warmUpElement.getWarpColumn());

        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                Integer.MAX_VALUE,
                Optional.empty(),
                mock(QueryParams.class),
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                mock(StorageCollectorService.class),
                mock(MatchService.class),
                mock(WorkerMemoryManager.class));
        SourcePage nextPage = warpPageSource.getNextSourcePage();
        assertThat(nextPage).isNotNull();
        assertThat(nextPage.getPositionCount()).isEqualTo(10);
        assertThat(warpPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testCollectWithMatchOnDifferentColumn()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(schemaTableColumn.warpColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
        WarmUpElement warmUpElementMatch = WarmUpElement.builder()
                .warpColumn(new RegularColumn("m_" + schemaTableColumn.warpColumn().getName()))
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);

        BasicQueryMatchData.Builder queryMatchDataBuilder = new BasicQueryMatchData.Builder();
        queryMatchDataBuilder.warpColumn(warmUpElementMatch.getWarpColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElementMatch)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.TINY), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder();
        nativeCollectBuilder.blockIndex(0).type(IntegerType.INTEGER).warmUpElement(warmUpElement).warpColumn(warmUpElement.getWarpColumn());
        NativeQueryCollectData.Builder nativeCollectBuilderMatch = new NativeQueryCollectData.Builder();
        nativeCollectBuilderMatch.blockIndex(1).type(IntegerType.INTEGER).warmUpElement(warmUpElementMatch).warpColumn(warmUpElementMatch.getWarpColumn());

        WarpPageSource warpPageSource = new WarpPageSource(
                storageEngineConstants,
                Integer.MAX_VALUE,
                Optional.empty(),
                mock(QueryParams.class),
                predicatesCacheService,
                customStatsContext,
                shapingLoggerFactory,
                mock(StorageCollectorService.class),
                mock(MatchService.class),
                mock(WorkerMemoryManager.class));
        assertThat(warpPageSource.getNextSourcePage()).isNotNull();
        assertThat(warpPageSource.isFinished()).isFalse();
    }
}
