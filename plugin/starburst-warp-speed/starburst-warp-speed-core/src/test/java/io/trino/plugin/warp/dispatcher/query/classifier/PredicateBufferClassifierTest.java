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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.DomainToMapBlockConvertor;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.MatchCollectType.MAPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class PredicateBufferClassifierTest
        extends ClassifierTest
{
    private PredicatesCacheService predicatesCacheService;
    private PredicateBufferClassifier predicateBufferClassifier;
    private RowGroupData rowGroupData;

    @BeforeEach
    public void before()
    {
        init();
        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.allocPredicateBuffer(anyInt())).thenReturn(null);
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        DomainToMapBlockConvertor domainToMapBlockConvertor = new DomainToMapBlockConvertor(storageEngineConstants);
        MetricsManager metricsManager = mock(MetricsManager.class);
        GlobalConfig globalConfig = new GlobalConfig();
        predicatesCacheService = spy(new PredicatesCacheService(
                bufferAllocator,
                storageEngineConstants,
                metricsManager,
                domainToMapBlockConvertor,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())));
        doReturn(Optional.of(mock(PredicateCacheData.class))).when(predicatesCacheService).predicateDataToBuffer(any(), any());
        predicateBufferClassifier = new PredicateBufferClassifier(
                predicatesCacheService,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));

        rowGroupData = mock(RowGroupData.class);
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        predicateContextFactory = new PredicateContextFactory(globalConfig, dispatcherProxiedConnectorTransformer);
    }

    @Test
    public void testNoBufferAvailableWithPrefilled()
    {
        doReturn(Optional.empty()).when(predicatesCacheService).getOrCreatePredicateBufferId(any(), any());

        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = createCollectColumnsByBlockIndexMap(1, 0);
        WarmedWarmupTypes warmedWarmupTypes = createColumnToWarmUpElementByType(collectColumnsByBlockIndex.values(), WarmUpType.WARM_UP_TYPE_BASIC);
        ColumnHandle columnHandle = collectColumnsByBlockIndex.values().stream().findAny().orElseThrow();
        WarmUpElement warmUpElement = warmedWarmupTypes.basicWarmedElements().values().stream().findAny().orElseThrow();
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 1L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));

        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext baseQueryContext = new QueryContext(predicateContextData, collectColumnsByBlockIndex, "query-id");
        QueryMatchData queryMatchData = BasicQueryMatchData.builder()
                .warpColumn(warmUpElement.getWarpColumn())
                .type(IntegerType.INTEGER)
                .domain(Optional.of(domain))
                .warmUpElement(warmUpElement)
                .tightnessRequired(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain)
                        .collectNulls(domain.isNullAllowed())
                        .build())
                .build();
        PrefilledQueryCollectData prefilledQueryCollectData = PrefilledQueryCollectData.builder()
                .warpColumn(warmUpElement.getWarpColumn())
                .type(IntegerType.INTEGER)
                .build();

        ClassifyArgs classifyArgs = new ClassifyArgs(
                dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(0, columnHandle),
                warmedWarmupTypes,
                false,
                true,
                true,
                false,
                false);
        QueryContext currentQueryContext = baseQueryContext.asBuilder()
                .matchData(Optional.of(queryMatchData))
                .prefilledQueryCollectDataByBlockIndex(Collections.singletonMap(0, prefilledQueryCollectData))
                .build();
        QueryContext queryContext = predicateBufferClassifier.classify(classifyArgs, currentQueryContext);

        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isNotEmpty();
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(baseQueryContext.getRemainingCollectColumnByBlockIndex());
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        assertThat(((QueryMatchData) queryContext.getMatchData().orElseThrow()).getWarmUpElement()).isEqualTo(warmUpElement);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testNoBufferAvailableWithMappedMatchCollect()
    {
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = createCollectColumnsByBlockIndexMap(1, 0);

        WarmedWarmupTypes warmedWarmupTypes = createColumnToWarmUpElementByType(collectColumnsByBlockIndex.values(), WarmUpType.WARM_UP_TYPE_BASIC);
        ColumnHandle columnHandle = collectColumnsByBlockIndex.values().stream().findAny().orElseThrow();
        WarmUpElement warmUpElement = warmedWarmupTypes.basicWarmedElements().values().stream().findAny().orElseThrow();
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 1L);

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of())); // maybe can remove

        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext baseQueryContext = new QueryContext(predicateContextData, collectColumnsByBlockIndex, "query-id");

        QueryMatchData matchForMatchCollect = BasicQueryMatchData.builder()
                .warpColumn(warmUpElement.getWarpColumn())
                .type(IntegerType.INTEGER)
                .domain(Optional.of(domain))
                .warmUpElement(warmUpElement)
                .tightnessRequired(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain)
                        .collectNulls(domain.isNullAllowed())
                        .build())
                .build();
        List<NativeQueryCollectData> collectForMatchCollect = createCollectColumnsForMatchCollect(MAPPED, matchForMatchCollect);
        QueryContext currentQueryContext = baseQueryContext.asBuilder()
                .matchData(Optional.of(matchForMatchCollect))
                .nativeQueryCollectDataList(collectForMatchCollect)
                .remainingCollectColumnByBlockIndex(Map.of())
                .build();

        ClassifyArgs classifyArgs = new ClassifyArgs(
                dispatcherTableHandle,
                rowGroupData,
                predicateContextData,
                collectColumnsByBlockIndex,
                warmedWarmupTypes,
                false,
                true,
                true,
                false,
                true);

        QueryContext queryContext = predicateBufferClassifier.classify(classifyArgs, currentQueryContext);

        // Tests
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isEqualTo(1);
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(baseQueryContext.getRemainingCollectColumnByBlockIndex());
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        assertThat(((QueryMatchData) queryContext.getMatchData().orElseThrow()).getWarmUpElement()).isEqualTo(warmUpElement);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testNoMapForMappedMatchCollect()
    {
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = createCollectColumnsByBlockIndexMap(2, 0);
        WarmedWarmupTypes warmedWarmupTypes = createColumnToWarmUpElementByType(collectColumnsByBlockIndex.values(), WarmUpType.WARM_UP_TYPE_BASIC);

        ColumnHandle columnHandle1 = collectColumnsByBlockIndex.values().stream().toList().getFirst();
        WarmUpElement warmUpElement1 = warmedWarmupTypes.basicWarmedElements().values().stream().toList().getFirst();
        Domain domain1 = Domain.singleValue(IntegerType.INTEGER, 1L);
        PredicateCacheData predicateCacheData1 = mock(PredicateCacheData.class);
        Block dictBlock = mock(DictionaryBlock.class);
        when(predicateCacheData1.getValuesDict()).thenReturn(Optional.of(dictBlock));
        doReturn(Optional.of(predicateCacheData1)).when(predicatesCacheService).getOrCreatePredicateBufferId(any(), eq(domain1));

        ColumnHandle columnHandle2 = collectColumnsByBlockIndex.values().stream().toList().get(1);
        WarmUpElement warmUpElement2 = warmedWarmupTypes.basicWarmedElements().values().stream().toList().get(1);
        Domain domain2 = Domain.singleValue(IntegerType.INTEGER, 2L);
        PredicateCacheData predicateCacheData2 = mock(PredicateCacheData.class);
        when(predicateCacheData2.getValuesDict()).thenReturn(Optional.empty());
        doReturn(Optional.of(predicateCacheData2)).when(predicatesCacheService).getOrCreatePredicateBufferId(any(), eq(domain2));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle1, domain1, columnHandle2, domain2));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext baseQueryContext = new QueryContext(predicateContextData, collectColumnsByBlockIndex, "query-id");

        QueryMatchData matchForMatchCollect1 = BasicQueryMatchData.builder()
                .warpColumn(warmUpElement1.getWarpColumn())
                .type(IntegerType.INTEGER)
                .domain(Optional.of(domain1))
                .warmUpElement(warmUpElement1)
                .tightnessRequired(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain1)
                        .collectNulls(domain1.isNullAllowed())
                        .build())
                .build();
        QueryMatchData matchForMatchCollect2 = BasicQueryMatchData.builder()
                .warpColumn(warmUpElement2.getWarpColumn())
                .type(IntegerType.INTEGER)
                .domain(Optional.of(domain2))
                .warmUpElement(warmUpElement2)
                .tightnessRequired(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain2)
                        .collectNulls(domain2.isNullAllowed())
                        .build())
                .build();
        List<NativeQueryCollectData> collectsForMatchCollect = createCollectColumnsForMatchCollect(MAPPED, matchForMatchCollect1, matchForMatchCollect2);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(matchForMatchCollect1, matchForMatchCollect2));

        ClassifyArgs classifyArgs = new ClassifyArgs(
                dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                collectColumnsByBlockIndex,
                warmedWarmupTypes,
                false,
                true,
                true,
                false,
                false);

        QueryContext currentQueryContext = baseQueryContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(collectsForMatchCollect)
                .remainingCollectColumnByBlockIndex(Map.of())
                .build();
        QueryContext queryContext = predicateBufferClassifier.classify(classifyArgs, currentQueryContext);
        assertThat(queryContext.getRemainingCollectColumns().size()).isEqualTo(1);
        assertThat(queryContext.getNativeQueryCollectDataList().size()).isEqualTo(1);
        assertThat(queryContext.getRemainingCollectColumns()).isEqualTo(List.of(columnHandle2));
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }
}
