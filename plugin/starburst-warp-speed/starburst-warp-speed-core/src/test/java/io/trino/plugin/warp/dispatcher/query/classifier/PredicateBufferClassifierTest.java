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
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.DomainToMapBlockConvertor;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
        DomainToMapBlockConvertor domainToMapBlockConvertor = new DomainToMapBlockConvertor();
        MetricsManager metricsManager = mock(MetricsManager.class);
        predicatesCacheService = spy(new PredicatesCacheService(bufferAllocator,
                storageEngineConstants,
                metricsManager,
                domainToMapBlockConvertor));
        doReturn(Optional.of(mock(PredicateCacheData.class))).when(predicatesCacheService).predicateDataToBuffer(any(), any());
        predicateBufferClassifier = new PredicateBufferClassifier(predicatesCacheService);

        rowGroupData = mock(RowGroupData.class);
        GlobalConfig globalConfig = new GlobalConfig();
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        predicateContextFactory = new PredicateContextFactory(globalConfig, dispatcherProxiedConnectorTransformer);
    }

    @Test
    public void testNoBufferAvailable()
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
        ConnectorSession session = mock(ConnectorSession.class);

        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext baseQueryContext = new QueryContext(predicateContextData, collectColumnsByBlockIndex);
        QueryMatchData queryMatchData = BasicQueryMatchData.builder()
                .varadaColumn(warmUpElement.getVaradaColumn())
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
                .varadaColumn(warmUpElement.getVaradaColumn())
                .type(IntegerType.INTEGER)
                .build();

        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(0, columnHandle),
                warmedWarmupTypes,
                false,
                true,
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
}
