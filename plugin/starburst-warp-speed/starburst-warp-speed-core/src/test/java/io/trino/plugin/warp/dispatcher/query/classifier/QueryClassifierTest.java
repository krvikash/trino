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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.dispatcher.CompletedDynamicFilter;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.SingleValue;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicateBufferInfo;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.warp.WarpSessionProperties.ENABLE_MATCH_COLLECT;
import static io.trino.plugin.warp.WarpSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.createLuceneQueryMatchData;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumnHandle;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryClassifierTest
{
    /**
     * H0,H1 are hive
     * V0,V1,V2 are warp warmed where -
     * V0 [data]
     * V1[data,basic]
     * V2[data,basic,lucene]
     */

    private final SchemaTableName schemaTableName = new SchemaTableName("s1", "t1");
    private final Type intType = IntegerType.INTEGER;
    private final Type varcharType = VarcharType.createVarcharType(10);
    private final Type doubleType = DoubleType.DOUBLE;
    private DispatcherTableHandle dispatcherTableHandle;
    private List<TestingConnectorColumnHandle> testingConnectorColumnHandles;
    private Map<String, TestingConnectorColumnHandle> warpColumnHandles;
    private Map<ColumnHandle, Map<WarmUpType, WarmUpElement>> weHandleToWarmUpElementByType;
    private List<WarmUpElement> warmUpElements;
    private RowGroupData rowGroupData;
    private RowGroupData rowGroupDataWithPartitionKeys;
    private PredicatesCacheService predicatesCacheService;
    private StorageEngineConstants storageEngineConstants;
    private QueryClassifier queryClassifier;
    private BufferAllocator bufferAllocator;
    private MatchCollectIdService matchCollectIdService;
    private GlobalConfig globalConfig;
    private NativeConfig nativeConfig;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private ConnectorSession session;
    private PredicateContextFactory predicateContextFactory;

    private void createWarmUpElements(String name, Type type, WarmUpType... warmUpTypes)
    {
        createWarmUpElements(name, type, WarmUpElementState.VALID, warmUpTypes);
    }

    private void createWarmUpElements(String name, Type type, WarmUpElementState state, WarmUpType... warmUpTypes)
    {
        TestingConnectorColumnHandle columnHandle = mockColumnHandle(name, type, dispatcherProxiedConnectorTransformer);
        when(dispatcherProxiedConnectorTransformer.getWarpRegularColumn(eq(columnHandle))).thenReturn(new RegularColumn(name));
        when(dispatcherProxiedConnectorTransformer.getColumnType(eq(columnHandle))).thenReturn(type);
        warpColumnHandles.put(name, columnHandle);

        for (WarmUpType warmUpType : warmUpTypes) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName(name)
                    .warmUpType(warmUpType)
                    .recTypeCode(TypeUtils.convertToRecTypeCode(type, TypeUtils.getTypeLength(type, 8192), 8))
                    .recTypeLength(TypeUtils.getTypeLength(type, 8192))
                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                    .state(state)
                    .totalRecords(5)
                    .build();
            weHandleToWarmUpElementByType.computeIfAbsent(columnHandle, _ -> new HashMap<>()).put(warmUpType, warmUpElement);
            warmUpElements.add(warmUpElement);
        }
    }

    @BeforeEach
    @SuppressWarnings("MockNotUsedInProduction")
    public void before()
    {
        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getMatchCollectBufferSize()).thenReturn(1024 * 1024);
        when(storageEngineConstants.getMaxChunksInRange()).thenReturn(1);
        when(storageEngineConstants.getMatchCollectNumIds()).thenReturn(100);
        when(storageEngineConstants.getMaxMatchColumns()).thenReturn(128);
        predicatesCacheService = mock(PredicatesCacheService.class);
        PredicateBufferInfo predicateBufferInfo = new PredicateBufferInfo(null, PredicateBufferPoolType.INVALID);
        when(predicatesCacheService.getOrCreatePredicateBufferId(isA(PredicateData.class), isA(Domain.class))).thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        when(predicatesCacheService.predicateDataToBuffer(any(), any())).thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.getQueryNullBufferSize(any())).thenReturn(64 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(4))).thenReturn(256 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(8))).thenReturn(512 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(10))).thenReturn(512 * 1024);
        when(bufferAllocator.getMatchCollectRecordBufferSize(eq(4))).thenReturn(256 * 1024);
        when(bufferAllocator.getMatchCollectRecordBufferSize(eq(8))).thenReturn(512 * 1024);
        matchCollectIdService = mock(MatchCollectIdService.class);
        globalConfig = new GlobalConfig();
        this.dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(any(RowGroupData.class), any(), any())).thenReturn(Optional.empty());
        nativeConfig = mock(NativeConfig.class);
        when(nativeConfig.getCollectTxSize()).thenReturn(8 * 1024 * 1024);
        // when(globalConfig.getEnableMatchCollect()).thenReturn(true);
        // in each test we will set the correct record data (int/varchar) as return value from getType
        ClassifierFactory classifierFactory = new ClassifierFactory(
                storageEngineConstants,
                predicatesCacheService,
                bufferAllocator,
                nativeConfig,
                dispatcherProxiedConnectorTransformer,
                matchCollectIdService,
                globalConfig,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));

        testingConnectorColumnHandles = new ArrayList<>();
        testingConnectorColumnHandles.add(mockColumnHandle("h0", IntegerType.INTEGER, dispatcherProxiedConnectorTransformer));
        testingConnectorColumnHandles.add(mockColumnHandle("h1", IntegerType.INTEGER, dispatcherProxiedConnectorTransformer));

        testingConnectorColumnHandles.forEach(ch -> {
            String name = ch.name();
            Type type = ch.type();
            when(dispatcherProxiedConnectorTransformer.getWarpRegularColumn(eq(ch))).thenReturn(new RegularColumn(name));
            when(dispatcherProxiedConnectorTransformer.getColumnType(eq(ch))).thenReturn(type);
        });
        warpColumnHandles = new HashMap<>();
        weHandleToWarmUpElementByType = new HashMap<>();
        warmUpElements = new ArrayList<>();
        createWarmUpElements("v-int-data", intType, WarmUpType.WARM_UP_TYPE_DATA);
        createWarmUpElements("v-int-data-basic", intType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-all", varcharType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC, WarmUpType.WARM_UP_TYPE_LUCENE);
        createWarmUpElements("v-int-data-basic-2", intType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-data-basic", varcharType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-double-basic", doubleType, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-lucene", varcharType, WarmUpType.WARM_UP_TYPE_LUCENE);
        createWarmUpElements("v-varchar-basic", varcharType, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-data-lucene-failed", varcharType, WarmUpType.WARM_UP_TYPE_DATA);
        createWarmUpElements("v-varchar-data-lucene-failed", varcharType, WarmUpElementState.FAILED_PERMANENTLY, WarmUpType.WARM_UP_TYPE_LUCENE);

        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "path", 0, 1, 0, "", "");
        rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .build();
        rowGroupDataWithPartitionKeys = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .partitionKeys(Map.of(
                        new RegularColumn("v-int-data"), "1",
                        new RegularColumn("v-varchar-data-basic"), "str"))
                .build();
        predicateContextFactory = new PredicateContextFactory(globalConfig, dispatcherProxiedConnectorTransformer);

        session = mock(ConnectorSession.class);
        when(session.getProperty(eq(ENABLE_MATCH_COLLECT), eq(Boolean.class))).thenReturn(true);
        queryClassifier = new QueryClassifier(
                classifierFactory,
                matchCollectIdService,
                predicateContextFactory,
                dispatcherProxiedConnectorTransformer,
                globalConfig);
    }

    /**
     * `select count(H1),count(H2) from T`
     */
    @Test
    public void testProxiedCollect()
    {
        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE);
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.copyOf(testingConnectorColumnHandles), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(
                0, testingConnectorColumnHandles.get(0),
                1, testingConnectorColumnHandles.get(1)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(H1),count(H2) from T where H1 = 1`
     */
    @Test
    public void testProxiedCollectMatch()
    {
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(testingConnectorColumnHandles.getFirst(), Domain.singleValue(testingConnectorColumnHandles.getFirst().type(), 1L)));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.get(0), testingConnectorColumnHandles.get(1)), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(
                0, testingConnectorColumnHandles.get(0),
                1, testingConnectorColumnHandles.get(1)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEqualTo(predicateContextData.getRemainingColumns());
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `SELECT count(v-varchar-lucene)
     * FROM T
     * WHERE v-varchar-lucene like 'str%' AND v-varchar-lucene > 'str1'
     */
    @Test
    public void testWarpMatchOnALuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = warpColumnHandles.get("v-varchar-lucene");

        Range range = Range.greaterThan(matchOnlyLuceneColumn.type(), Slices.utf8Slice("str1"));
        Domain rangeDomain = Domain.create(ValueSet.ofRanges(range), false);

        Slice likePattern = Slices.utf8Slice("str%");
        WarpCall likeWarpExpression = createLikeWarpExpression(matchOnlyLuceneColumn, likePattern);
        String name = matchOnlyLuceneColumn.name();
        RegularColumn regularColumn = new RegularColumn(name);
        WarpExpressionData expressionData = new WarpExpressionData(likeWarpExpression, varcharType, false, Optional.empty(), regularColumn);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, rangeDomain)));
        WarpExpression warpExpression = new WarpExpression(expressionData.getExpression(), List.of(expressionData));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, Set.of(range), Set.of(likePattern), rangeDomain, false);

        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryContext.getMatchData().orElseThrow().getLeavesDFS().forEach(x -> addInnerToMainQueryBuilder(queryBuilder, ((LuceneQueryMatchData) x).getQuery()));
        assertThat(queryBuilder.build()).isEqualTo(expectedLuceneQueryMatchData.getQuery());
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testWarpMatchOnALuceneColumnWithDateFormat()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = warpColumnHandles.get("v-varchar-lucene");

        Range range = Range.greaterThan(matchOnlyLuceneColumn.type(), Slices.utf8Slice("2021-07-06"));
        Domain rangeDomain = Domain.create(ValueSet.ofRanges(range), false);

        Slice likePattern = Slices.utf8Slice("2012-07%");
        WarpCall likeWarpExpression = createLikeWarpExpression(matchOnlyLuceneColumn, likePattern);
        RegularColumn regularColumn = new RegularColumn(matchOnlyLuceneColumn.name());

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, Set.of(range), Set.of(likePattern), rangeDomain, false);

        WarpExpressionData expressionData = new WarpExpressionData(likeWarpExpression, varcharType, false, Optional.empty(), regularColumn);
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(new WarpExpression(expressionData.getExpression(), List.of(expressionData))));

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, rangeDomain)));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(LogicalMatchData.class);
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryContext.getMatchData().orElseThrow().getLeavesDFS().forEach(x -> addInnerToMainQueryBuilder(queryBuilder, ((LuceneQueryMatchData) x).getQuery()));
        assertThat(queryBuilder.build()).isEqualTo(expectedLuceneQueryMatchData.getQuery());
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testWarpMatchNotNullOnLuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = warpColumnHandles.get("v-varchar-lucene");

        Domain notNullDomain = Domain.notNull(matchOnlyLuceneColumn.type());
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(matchOnlyLuceneColumn, notNullDomain));

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        ImmutableSet<Range> ranges = ImmutableSet.of(Range.all(matchOnlyLuceneColumn.type()));
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, ranges, emptySet(), notNullDomain, false);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        assertThat(((LuceneQueryMatchData) queryMatchData)).isEqualTo(expectedLuceneQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(H1) from T where V1 in (1, 2)`
     */
    @Test
    public void testProxiedCollectMatchWarpCollect()
    {
        TestingConnectorColumnHandle dataIntColumn = warpColumnHandles.get("v-int-data");
        Domain domain = Domain.multipleValues(dataIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(dataIntColumn, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        when(dispatcherProxiedConnectorTransformer.proxyHasPushedDownFilter(any())).thenReturn(true);
        QueryContext baseQueryContext = new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.getFirst(), dataIntColumn), true, "query-id");
        QueryContext queryContext = queryClassifier.classify(
                baseQueryContext,
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.getFirst(), 1, dataIntColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isEqualTo(1);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testPrefilledCollectWarpColumn()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = warpColumnHandles.get("v-int-data-basic");
        TestingConnectorColumnHandle matchCollectIntColumn2 = warpColumnHandles.get("v-int-data-basic-2");
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        WarmUpElement matchIntWarmUpElement2 = weHandleToWarmUpElementByType.get(matchCollectIntColumn2).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain domain = Domain.singleValue(matchCollectIntColumn.type(), 1L);
        Domain domain2 = Domain.singleValue(matchCollectIntColumn2.type(), 2L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(
                matchCollectIntColumn, domain, matchCollectIntColumn2, domain2));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchCollectIntColumn, matchCollectIntColumn2), true, "query-id"),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(matchIntWarmUpElement, 0, domain),
                createPrefilledQueryCollectData(matchIntWarmUpElement2, 1, domain2));
        BasicQueryMatchData expectedMatchData = BasicQueryMatchData
                .builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(matchCollectIntColumn.type())
                .domain(Optional.of(domain))
                .nativeExpression(createExpectedNativeExpression(domain))
                .tightnessRequired(true)
                .build();
        BasicQueryMatchData expectedMatchData2 = BasicQueryMatchData
                .builder()
                .warmUpElement(matchIntWarmUpElement2)
                .type(matchCollectIntColumn2.type())
                .domain(Optional.of(domain2))
                .tightnessRequired(true)
                .nativeExpression(createExpectedNativeExpression(domain2))
                .build();
        assertThat(queryContext.getMatchLeavesDFS()).containsExactlyInAnyOrder(expectedMatchData, expectedMatchData2);
    }

    @Test
    public void testPrefilledCollectProxiedColumn()
    {
        TestingConnectorColumnHandle matchOnlyBasicColumn = warpColumnHandles.get("v-double-basic");
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchOnlyBasicColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.singleValue(DoubleType.DOUBLE, 1.5d);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchOnlyBasicColumn, matchDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchOnlyBasicColumn), true, "query-id"),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(matchWarmUpElement, 0, matchDomain));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicQueryMatchData expectedBasicQueryMatchData = BasicQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(doubleType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(true)
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicQueryMatchData);
    }

    /**
     * select count(*) from T where v-varchar-lucene = 'str'
     */
    @Test
    public void testPrefilledCollectProxiedLuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = warpColumnHandles.get("v-varchar-lucene");
        Domain domain = Domain.singleValue(matchOnlyLuceneColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, domain));

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        Set<Range> orderedRanges = new HashSet<>(((SortedRangeSet) domain.getValues()).getOrderedRanges());
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, true, orderedRanges, emptySet(), domain, false);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), true, "query-id"),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().size()).isEqualTo(1);
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        assertThat(queryMatchData).isEqualTo(expectedLuceneQueryMatchData);
    }

    /**
     * select v-varchar-data-basic from T where v-varchar-data-basic in ("str1", "str2")
     */
    @Test
    public void testBasicMatchColumns()
    {
        TestingConnectorColumnHandle columnHandle = warpColumnHandles.get("v-varchar-data-basic");
        Domain matchDomain = Domain.multipleValues(columnHandle.type(), List.of(Slices.utf8Slice("str1"), Slices.utf8Slice("str2")));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(columnHandle, matchDomain));

        String columnName = columnHandle.name();
        WarmUpElement basicWarmupElement = WarmUpElement.builder()
                .colName(columnName)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                .recTypeLength(10)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
        RowGroupData rowGroupData = RowGroupData.builder(this.rowGroupData).warmUpElements(ImmutableList.of(basicWarmupElement)).build();

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(columnHandle), true, "query-id"),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getMatchLeavesDFS().size()).isEqualTo(1);
        QueryMatchData queryMatchData = queryContext.getMatchLeavesDFS().getFirst();

        BasicQueryMatchData expectedBasicQueryMatchData = BasicQueryMatchData.builder()
                .warmUpElement(basicWarmupElement)
                .type(varcharType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain, PredicateType.PREDICATE_TYPE_STRING_VALUES))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * select count(v-double-basic) from T where v-double-basic = 1.5
     * [In addition: v-double-basic is marked as simplified]
     */
    @Test
    public void testNotPrefilledNonTightQueryContext()
    {
        TestingConnectorColumnHandle matchOnlyBasicColumn = warpColumnHandles.get("v-double-basic");
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchOnlyBasicColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.singleValue(DoubleType.DOUBLE, 1.5d);

        ImmutableList<ColumnHandle> remainingCollectColumns = ImmutableList.of(matchOnlyBasicColumn);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchOnlyBasicColumn, matchDomain));

        globalConfig.setPredicateSimplifyThreshold(0);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext baseQueryContext = new QueryContext(predicateContextData, remainingCollectColumns, true, "query-id");
        QueryContext queryContext = queryClassifier.classify(baseQueryContext, rowGroupData, mockDispatcherTableHandle(schemaTableName), Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(matchWarmUpElement, 0, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicQueryMatchData expectedBasicQueryMatchData = BasicQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(doubleType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(false)
                .simplifiedDomain(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .collectNulls(matchDomain.isNullAllowed())
                        .domain(matchDomain)
                        .build())
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicQueryMatchData);
    }

    /**
     * /*
     * v-int-data and v-varchar-data-basic are warmed partition columns
     * select count(v-int-data), count(v-varchar-data-basic) from T
     */
    @Test
    public void testAllPrefilledColumns()
    {
        ColumnHandle intPartitionColumn = warpColumnHandles.get("v-int-data");
        ColumnHandle varcharPartitionColumn = warpColumnHandles.get("v-varchar-data-basic");

        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(intPartitionColumn, varcharPartitionColumn), true, "query-id"),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        // Since we have only prefilled columns, we convert one column to regular collect in order to create a valid tx.
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).hasSize(2);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
    }

    @Test
    public void testPrefilledWhenOnlyPartitionColumnsAndExternal()
    {
        TestingConnectorColumnHandle intPartitionColumn = warpColumnHandles.get("v-int-data");
        WarmUpElement intPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(intPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharPartitionColumn = warpColumnHandles.get("v-varchar-data-basic");
        WarmUpElement varcharPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle column1 = testingConnectorColumnHandles.getFirst();
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(column1, Domain.singleValue(column1.type(), 1L)));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        Optional<SingleValue> expectedSingleValueV1 = Optional.of(SingleValue.create(intPartitionColumn.type(), 1L));
        Optional<SingleValue> expectedSingleValueV2 = Optional.of(SingleValue.create(varcharPartitionColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));

        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        ImmutableList<ColumnHandle> remainingCollectColumns = ImmutableList.of(intPartitionColumn, varcharPartitionColumn, column1);
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, remainingCollectColumns, true, "query-id"),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(2, column1));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().contains(new RegularColumn(column1.name()))).isTrue();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(intPartitionDataWarmUpElement, 0, expectedSingleValueV1.orElseThrow()),
                createPrefilledQueryCollectData(varcharPartitionDataWarmUpElement, 1, expectedSingleValueV2.orElseThrow()));
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
    }

    /**
     * v-int-data and v-varchar-data-basic are partition columns
     * select count(v-int-data), count(v-varchar-data-basic) from T WHERE v-int-data-basic-2 = 1
     */
    @Test
    public void testPrefilledCollectPartitionColumnWithPredicate()
    {
        TestingConnectorColumnHandle intPartitionColumn = warpColumnHandles.get("v-int-data");
        WarmUpElement intPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(intPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharPartitionColumn = warpColumnHandles.get("v-varchar-data-basic");
        WarmUpElement varcharPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle intNonPartitionColumn = warpColumnHandles.get("v-int-data-basic-2");
        WarmUpElement intNonPartitionBasicWarmUpElement = weHandleToWarmUpElementByType.get(intNonPartitionColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);

        // Since we have predicate on a non partition column (V4) - we expect the partition columns to be prefilled
        Domain domain = Domain.singleValue(intNonPartitionColumn.type(), 1L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(intNonPartitionColumn, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);

        Optional<SingleValue> expectedSingleValueV1 = Optional.of(SingleValue.create(intPartitionColumn.type(), 1L));
        Optional<SingleValue> expectedSingleValueV5 = Optional.of(SingleValue.create(varcharPartitionColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        QueryContext queryContext = queryClassifier.classify(
                new QueryContext(predicateContextData, ImmutableList.of(intPartitionColumn, varcharPartitionColumn), true, "query-id"),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(intPartitionDataWarmUpElement, 0, expectedSingleValueV1.orElseThrow()),
                createPrefilledQueryCollectData(varcharPartitionDataWarmUpElement, 1, expectedSingleValueV5.orElseThrow()));
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicQueryMatchData expectedBasicQueryMatchData = BasicQueryMatchData.builder()
                .warmUpElement(intNonPartitionBasicWarmUpElement)
                .type(intType)
                .domain(Optional.of(domain))
                .nativeExpression(createExpectedNativeExpression(domain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicQueryMatchData);
    }

    /**
     * select count(v-int-data), count(v-int-data-basic) from T where [DynamicFilter="v-int-data = 2"]
     * [In addition: predicateThreshold = 1, v-int-data is marked as simplified on the table handle]
     */
    @Test
    public void testGetBasicQueryContext()
    {
        int predicateThreshold = 1;
        when(session.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(predicateThreshold);
        TestingConnectorColumnHandle onlyDataColumnHandle = warpColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle dataBasicIntColumn = warpColumnHandles.get("v-int-data-basic");
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        RegularColumn regularColumn = new RegularColumn(onlyDataColumnHandle.name());
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of(regularColumn)));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 2L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                onlyDataColumnHandle, domain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(tupleDomain);

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(onlyDataColumnHandle, dataBasicIntColumn), dispatcherTableHandle, dynamicFilter, session);

        assertThat(basicQueryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(ImmutableMap.of(
                0, onlyDataColumnHandle,
                1, dataBasicIntColumn));
        assertThat(basicQueryContext.getPredicateContextData().getRemainingColumns().contains(regularColumn)).isTrue();
    }

    /**
     * select count(v-int-data) from T where [DynamicFilter="v-int-data < 3 OR v-int-data > 9"]
     * [In addition: predicateThreshold = 1]
     */
    @Test
    public void testGetBasicQueryContextSimplifiedDynamicFilter()
    {
        int predicateThreshold = 1;
        when(session.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(predicateThreshold);
        TestingConnectorColumnHandle onlyDataColumnHandle = warpColumnHandles.get("v-int-data");
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        Domain domain = Domain.create(SortedRangeSet.copyOf(
                        IntegerType.INTEGER,
                        List.of(Range.lessThan(IntegerType.INTEGER, 3L), Range.greaterThan(IntegerType.INTEGER, 9L))),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(onlyDataColumnHandle, domain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(tupleDomain);

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(onlyDataColumnHandle), dispatcherTableHandle, dynamicFilter, session);

        assertThat(basicQueryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(ImmutableMap.of(0, onlyDataColumnHandle));
        PredicateContextData actualRemainingPredicateContext = basicQueryContext.getPredicateContextData();
        assertThat(actualRemainingPredicateContext.getRemainingColumns().size()).isEqualTo(1);
    }

    /**
     * `select count(*) from T where v-int-data-basic = 1 AND [DynamicFilter="v-int-data < 3 OR v-int-data > 9"]`
     * [In addition: dispatcherTableHandle.isSubsumedPredicates() == true]
     */
    @Test
    public void testSubsumedPredicatesWithDynamicFilter()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = warpColumnHandles.get("v-int-data-basic");
        Domain matchDomain = Domain.singleValue(intType, 1L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        when(dispatcherTableHandle.isSubsumedPredicates()).thenReturn(true);
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);

        TestingConnectorColumnHandle onlyDataColumnHandle = warpColumnHandles.get("v-int-data");
        Domain dynamicFilterDomain = Domain.create(SortedRangeSet.copyOf(
                        intType,
                        List.of(Range.lessThan(intType, 3L), Range.greaterThan(intType, 9L))),
                false);
        TupleDomain<ColumnHandle> dynamicFilterTupleDomain = TupleDomain.withColumnDomains(Map.of(onlyDataColumnHandle, dynamicFilterDomain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(dynamicFilterTupleDomain);
        RegularColumn dynamicFilterColumn = new RegularColumn(onlyDataColumnHandle.name());

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(), dispatcherTableHandle, dynamicFilter, session);

        QueryContext queryContext = queryClassifier.classify(
                basicQueryContext,
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).containsExactly(dynamicFilterColumn);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        BasicQueryMatchData expectedMatchData = BasicQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(true)
                .simplifiedDomain(false)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .collectNulls(matchDomain.isNullAllowed())
                        .domain(matchDomain)
                        .build())
                .build();
        assertThat(queryContext.getMatchData()).isEqualTo(Optional.of(expectedMatchData));
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
        assertThat(queryContext.isCanBeTight()).isFalse();  // because of the dynamic filter
    }

    private DispatcherTableHandle mockDispatcherTableHandle(SchemaTableName schemaTableName)
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSchemaTableName()).thenReturn(schemaTableName);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        return dispatcherTableHandle;
    }

    private Type warmupElementToType(WarmUpElement warmUpElement)
    {
        return switch (warmUpElement.getRecTypeCode()) {
            case REC_TYPE_INTEGER -> intType;
            case REC_TYPE_VARCHAR -> varcharType;
            case REC_TYPE_DOUBLE -> doubleType;
            default -> null;
        };
    }

    private WarpCall createLikeWarpExpression(TestingConnectorColumnHandle columnHandle, Slice likePattern)
    {
        Type variableType = columnHandle.type();

        return new WarpCall(
                LIKE_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(columnHandle, variableType),
                        new WarpSliceConstant(likePattern, VarcharType.VARCHAR)),
                BOOLEAN);
    }

    private NativeQueryCollectData createNativeQueryCollectData(
            WarmUpElement warmUpElement,
            int blockIndex,
            boolean isMatchCollect)
    {
        return createNativeQueryCollectData(warmUpElement, blockIndex, isMatchCollect, warmupElementToType(warmUpElement));
    }

    private NativeQueryCollectData createNativeQueryCollectData(
            WarmUpElement warmUpElement,
            int blockIndex,
            boolean isMatchCollect,
            Type type)
    {
        return NativeQueryCollectData.builder()
                .warmUpElement(warmUpElement)
                .type(type)
                .blockIndex(blockIndex)
                .matchCollectType(isMatchCollect ? MatchCollectType.ORDINARY : MatchCollectType.DISABLED)
                .matchCollectId(isMatchCollect ? 0 : MatchCollectIdService.INVALID_ID)
                .build();
    }

    private PrefilledQueryCollectData createPrefilledQueryCollectData(WarmUpElement warmUpElement, int blockIndex, SingleValue singleValue)
    {
        return PrefilledQueryCollectData.builder()
                .warpColumn(warmUpElement.getWarpColumn())
                .type(warmupElementToType(warmUpElement))
                .blockIndex(blockIndex)
                .singleValue(singleValue)
                .build();
    }

    private PrefilledQueryCollectData createPrefilledQueryCollectData(WarmUpElement warmUpElement, int blockIndex, Domain singleValueDomain)
    {
        return createPrefilledQueryCollectData(
                warmUpElement,
                blockIndex,
                SingleValue.create(warmupElementToType(warmUpElement), singleValueDomain.getSingleValue()));
    }

    private static void addInnerToMainQueryBuilder(BooleanQuery.Builder queryBuilder, BooleanQuery innerQuery)
    {
        for (BooleanClause clause : innerQuery.clauses()) {
            queryBuilder.add(clause);
        }
    }

    private NativeExpression createExpectedNativeExpression(Domain matchDomain)
    {
        return createExpectedNativeExpression(matchDomain, PredicateType.PREDICATE_TYPE_VALUES);
    }

    private NativeExpression createExpectedNativeExpression(Domain matchDomain, PredicateType predicateType)
    {
        return NativeExpression.builder()
                .predicateType(predicateType)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .collectNulls(matchDomain.isNullAllowed())
                .domain(matchDomain)
                .build();
    }
}
