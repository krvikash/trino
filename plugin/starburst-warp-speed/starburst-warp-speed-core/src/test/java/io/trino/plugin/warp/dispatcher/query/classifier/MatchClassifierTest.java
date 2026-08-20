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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.NoneMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumnHandle;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MatchClassifierTest
{
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private DispatcherTableHandle dispatcherTableHandle;
    private Map<String, ColumnHandle> columns;
    private MatchClassifier matchClassifier;

    private PredicateContextFactory predicateContextFactory;

    private ConnectorSession session;

    @BeforeEach
    public void before()
    {
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        dispatcherTableHandle = mock(DispatcherTableHandle.class);
        BasicMatcher basicMatcher = new BasicMatcher();
        matchClassifier = new MatchClassifier(
                List.of(basicMatcher),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
        List<String> columnNames = List.of("a", "b", "c", "d");
        columns = columnNames.stream().map(columnName -> mockColumnHandle(columnName, IntegerType.INTEGER, dispatcherProxiedConnectorTransformer)).collect(Collectors.toMap(TestingConnectorColumnHandle::name, columnHandle -> columnHandle));
        predicateContextFactory = new PredicateContextFactory(
                new GlobalConfig(),
                dispatcherProxiedConnectorTransformer);
        session = mock(ConnectorSession.class);
    }

    static Stream<Arguments> config()
    {
        return Stream.of(
                arguments(List.of("a", "b", "c", "d"), List.of("b", "c", "a", "b", "c", "d", "a")),
                arguments(List.of("d"), List.of()),
                arguments(List.of("c", "d"), List.of("c")),
                arguments(List.of("a"), List.of("a")),
                arguments(List.of("a", "b"), List.of("b", "a")),
                arguments(List.of("b", "c"), List.of("b", "c")),
                arguments(List.of("b"), List.of("b")));
    }

    /**
     * Expression
     * ---                      AND
     * ---       a       AND              OR
     * ---             b   c          a    b   OR
     * ---                                    c   d
     */
    @ParameterizedTest
    @MethodSource("config")
    public void testExpression(List<String> matchColumnNames, List<String> expectedMatchColumns)
    {
        io.trino.plugin.warp.expression.rewrite.WarpExpression warpExpression = createWrapExpression();
        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        when(classifyArgs.getDispatcherTableHandle()).thenReturn(dispatcherTableHandle);
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();

        matchColumnNames
                .forEach(columnName -> {
                    WarmUpElement warmUpElement = mock(WarmUpElement.class);
                    when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);
                    when(warmUpElement.getWarpColumn()).thenReturn(new RegularColumn(columnName));
                    builder.add(warmUpElement);
                });
        WarmedWarmupTypes warmedWarmupTypes = builder.build();
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmedWarmupTypes);
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));

        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Collections.emptySet()));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = new QueryContext(predicateContextData, ImmutableList.of(), false, "query-id");
        QueryContext result = matchClassifier.classify(classifyArgs, queryContext);
        List<String> actualMatchColumns;
        if (result.getMatchData().isEmpty()) {
            actualMatchColumns = Collections.emptyList();
        }
        else {
            actualMatchColumns = result.getMatchData().orElseThrow().getLeavesDFS().stream().map(x -> x.getWarpColumn().getName()).toList();
        }
        assertThat(actualMatchColumns).isEqualTo(expectedMatchColumns);
    }

    // AND with unmergeable predicates on the same column resolves to none when one side is out of range
    @Test
    public void testAndWithUnmergeablePredicatesOnSameColumnResolvesToNone()
    {
        RegularColumn regularColumnA = new RegularColumn("a");
        RegularColumn regularColumnB = new RegularColumn("b");
        WarpVariable warpVariableA = new WarpVariable(columns.get("a"), IntegerType.INTEGER);
        WarpVariable warpVariableB = new WarpVariable(columns.get("b"), IntegerType.INTEGER);

        // function-typed predicate on "a"
        WarpCall functionLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableA, new WarpPrimitiveConstant(3L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression functionNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 3L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_DAY_OF_WEEK)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData functionLeaf = new WarpExpressionData(
                functionLeafExpression, IntegerType.INTEGER, false, Optional.of(functionNativeExpression), regularColumnA);

        // plain domain predicate on "a", outside the warmed element's [1,100] min/max stats
        WarpCall outOfRangeLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableA, new WarpPrimitiveConstant(0L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression outOfRangeNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 0L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData outOfRangeLeaf = new WarpExpressionData(
                outOfRangeLeafExpression, IntegerType.INTEGER, false, Optional.of(outOfRangeNativeExpression), regularColumnA);

        // independently-matchable predicate on column "b"
        WarpCall matchedLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableB, new WarpPrimitiveConstant(1L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression matchedNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 1L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData matchedLeaf = new WarpExpressionData(
                matchedLeafExpression, IntegerType.INTEGER, false, Optional.of(matchedNativeExpression), regularColumnB);

        WarpCall rootExpression = new WarpCall(
                AND_FUNCTION_NAME.getName(),
                List.of(functionLeafExpression, outOfRangeLeafExpression, matchedLeafExpression),
                BOOLEAN);
        io.trino.plugin.warp.expression.rewrite.WarpExpression warpExpression = new io.trino.plugin.warp.expression.rewrite.WarpExpression(
                rootExpression, List.of(functionLeaf, outOfRangeLeaf, matchedLeaf));

        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Collections.emptySet()));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = new QueryContext(predicateContextData, ImmutableList.of(), false, "query-id");

        WarmUpElement rangeWarmUpElement = mock(WarmUpElement.class);
        when(rangeWarmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(rangeWarmUpElement.getWarpColumn()).thenReturn(regularColumnA);
        when(rangeWarmUpElement.getWarmupElementStats()).thenReturn(new WarmupElementStats(0, 1, 100));
        when(rangeWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);

        WarmUpElement basicWarmUpElement = mock(WarmUpElement.class);
        when(basicWarmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(basicWarmUpElement.getWarpColumn()).thenReturn(regularColumnB);
        when(basicWarmUpElement.getWarmupElementStats()).thenReturn(new WarmupElementStats(0, 1, 100));
        when(basicWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);

        WarmedWarmupTypes.Builder warmedWarmupTypesBuilder = new WarmedWarmupTypes.Builder();
        warmedWarmupTypesBuilder.add(rangeWarmUpElement);
        warmedWarmupTypesBuilder.add(basicWarmUpElement);
        WarmedWarmupTypes warmedWarmupTypes = warmedWarmupTypesBuilder.build();

        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        when(classifyArgs.getDispatcherTableHandle()).thenReturn(dispatcherTableHandle);
        when(classifyArgs.isMinMaxFilter()).thenReturn(true);
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmedWarmupTypes);

        MatchClassifier classifierUnderTest = new MatchClassifier(
                List.of(new RangeMatcher(new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())), new BasicMatcher()),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));

        QueryContext result = classifierUnderTest.classify(classifyArgs, queryContext);

        assertThat(result.isNoneOnly()).isTrue();
        assertThat(result.getMatchData().orElseThrow()).isInstanceOf(NoneMatchData.class);
    }

    // OR with an unmergeable AND branch (with one side out of range) resolves to the independently matchable sibling
    @Test
    public void testOrWrappingUnmergeableAndResolvesToMatchableSibling()
    {
        RegularColumn regularColumnA = new RegularColumn("a");
        RegularColumn regularColumnB = new RegularColumn("b");
        RegularColumn regularColumnC = new RegularColumn("c");
        WarpVariable warpVariableA = new WarpVariable(columns.get("a"), IntegerType.INTEGER);
        WarpVariable warpVariableB = new WarpVariable(columns.get("b"), IntegerType.INTEGER);
        WarpVariable warpVariableC = new WarpVariable(columns.get("c"), IntegerType.INTEGER);

        // function-typed predicate on "a"
        WarpCall functionLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableA, new WarpPrimitiveConstant(3L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression functionNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 3L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_DAY_OF_WEEK)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData functionLeaf = new WarpExpressionData(
                functionLeafExpression, IntegerType.INTEGER, false, Optional.of(functionNativeExpression), regularColumnA);

        // plain domain predicate on "a", outside the warmed element's [1,100] min/max stats
        WarpCall outOfRangeLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableA, new WarpPrimitiveConstant(0L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression outOfRangeNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 0L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData outOfRangeLeaf = new WarpExpressionData(
                outOfRangeLeafExpression, IntegerType.INTEGER, false, Optional.of(outOfRangeNativeExpression), regularColumnA);

        // independently-matchable predicate on column "b", inside the AND
        WarpCall matchedBLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableB, new WarpPrimitiveConstant(1L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression matchedBNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 1L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData matchedBLeaf = new WarpExpressionData(
                matchedBLeafExpression, IntegerType.INTEGER, false, Optional.of(matchedBNativeExpression), regularColumnB);

        WarpCall andExpression = new WarpCall(
                AND_FUNCTION_NAME.getName(),
                List.of(functionLeafExpression, outOfRangeLeafExpression, matchedBLeafExpression),
                BOOLEAN);

        // independently-matchable predicate on column "c", sibling of the AND under the OR
        WarpCall matchedCLeafExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(warpVariableC, new WarpPrimitiveConstant(1L, IntegerType.INTEGER)),
                BOOLEAN);
        NativeExpression matchedCNativeExpression = NativeExpression.builder()
                .domain(Domain.singleValue(IntegerType.INTEGER, 1L))
                .collectNulls(false)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        WarpExpressionData matchedCLeaf = new WarpExpressionData(
                matchedCLeafExpression, IntegerType.INTEGER, false, Optional.of(matchedCNativeExpression), regularColumnC);

        WarpCall rootExpression = new WarpCall(
                OR_FUNCTION_NAME.getName(),
                List.of(andExpression, matchedCLeafExpression),
                BOOLEAN);
        io.trino.plugin.warp.expression.rewrite.WarpExpression warpExpression = new io.trino.plugin.warp.expression.rewrite.WarpExpression(
                rootExpression, List.of(functionLeaf, outOfRangeLeaf, matchedBLeaf, matchedCLeaf));

        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Collections.emptySet()));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = new QueryContext(predicateContextData, ImmutableList.of(), false, "query-id");

        WarmUpElement rangeWarmUpElement = mock(WarmUpElement.class);
        when(rangeWarmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(rangeWarmUpElement.getWarpColumn()).thenReturn(regularColumnA);
        when(rangeWarmUpElement.getWarmupElementStats()).thenReturn(new WarmupElementStats(0, 1, 100));
        when(rangeWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);

        WarmUpElement bWarmUpElement = mock(WarmUpElement.class);
        when(bWarmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(bWarmUpElement.getWarpColumn()).thenReturn(regularColumnB);
        when(bWarmUpElement.getWarmupElementStats()).thenReturn(new WarmupElementStats(0, 1, 100));
        when(bWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);

        WarmUpElement cWarmUpElement = mock(WarmUpElement.class);
        when(cWarmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(cWarmUpElement.getWarpColumn()).thenReturn(regularColumnC);
        when(cWarmUpElement.getWarmupElementStats()).thenReturn(new WarmupElementStats(0, 1, 100));
        when(cWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_BASIC);

        WarmedWarmupTypes.Builder warmedWarmupTypesBuilder = new WarmedWarmupTypes.Builder();
        warmedWarmupTypesBuilder.add(rangeWarmUpElement);
        warmedWarmupTypesBuilder.add(bWarmUpElement);
        warmedWarmupTypesBuilder.add(cWarmUpElement);
        WarmedWarmupTypes warmedWarmupTypes = warmedWarmupTypesBuilder.build();

        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        when(classifyArgs.getDispatcherTableHandle()).thenReturn(dispatcherTableHandle);
        when(classifyArgs.isMinMaxFilter()).thenReturn(true);
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmedWarmupTypes);

        MatchClassifier classifierUnderTest = new MatchClassifier(
                List.of(new RangeMatcher(new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())), new BasicMatcher()),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));

        QueryContext result = classifierUnderTest.classify(classifyArgs, queryContext);

        assertThat(result.isNoneOnly()).isFalse();
        assertThat(result.getMatchData().orElseThrow()).isEqualTo(
                BasicQueryMatchData.builder()
                        .warmUpElement(cWarmUpElement)
                        .type(IntegerType.INTEGER)
                        .domain(Optional.of(matchedCNativeExpression.domain()))
                        .simplifiedDomain(false)
                        .nativeExpression(matchedCNativeExpression)
                        .tightnessRequired(false)
                        .build());
    }

    private io.trino.plugin.warp.expression.rewrite.WarpExpression createWrapExpression()
    {
        Map<String, WarpVariable> columnNameToWarpVariable = columns
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> new WarpVariable(x.getValue(), IntegerType.INTEGER)));
        Map<String, WarpExpression> leaves = Map.of(
                "a", createLeafExpression("a", 5L, columnNameToWarpVariable),
                "b", createLeafExpression("b", 6L, columnNameToWarpVariable),
                "c", createLeafExpression("c", 7L, columnNameToWarpVariable),
                "d", createLeafExpression("d", 8L, columnNameToWarpVariable));

        WarpCall rootExpression = new WarpCall(
                AND_FUNCTION_NAME.getName(),
                List.of(leaves.get("a"),
                        new WarpCall(
                                AND_FUNCTION_NAME.getName(),
                                List.of(leaves.get("b"), leaves.get("c")),
                                BOOLEAN),
                        new WarpCall(
                                OR_FUNCTION_NAME.getName(),
                                List.of(leaves.get("a"), leaves.get("b"), new WarpCall(
                                        OR_FUNCTION_NAME.getName(),
                                        List.of(leaves.get("c"), leaves.get("d")),
                                        BOOLEAN)),
                                BOOLEAN)),
                BOOLEAN);
        Domain randomDomain = Domain.singleValue(IntegerType.INTEGER, (long) new Random().nextInt());
        NativeExpression nativeExpression = NativeExpression.builder()
                .domain(randomDomain)
                .collectNulls(randomDomain.isNullAllowed())
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .build();
        List<WarpExpressionData> warpExpressionDataLeaves = columns.keySet().stream().map(columnName -> new WarpExpressionData(
                        leaves.get(columnName),
                        IntegerType.INTEGER,
                        false,
                        Optional.of(nativeExpression),
                        new RegularColumn(columnName)))
                .toList();
        return new io.trino.plugin.warp.expression.rewrite.WarpExpression(rootExpression, warpExpressionDataLeaves);
    }

    private WarpCall createLeafExpression(String columnName, long value, Map<String, WarpVariable> columnNameToWarpVariable)
    {
        return new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(columnNameToWarpVariable.get(columnName), new WarpPrimitiveConstant(value, IntegerType.INTEGER)),
                BOOLEAN);
    }
}
