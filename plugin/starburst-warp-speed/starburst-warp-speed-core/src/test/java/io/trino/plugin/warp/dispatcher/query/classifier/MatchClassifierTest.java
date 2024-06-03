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
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
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
        matchClassifier = new MatchClassifier(List.of(basicMatcher), new GlobalConfig());
        List<String> columnNames = List.of("a", "b", "c", "d");
        columns = columnNames.stream().map(columnName -> mockColumnHandle(columnName, IntegerType.INTEGER, dispatcherProxiedConnectorTransformer)).collect(Collectors.toMap(TestingConnectorColumnHandle::name, columnHandle -> columnHandle));
        predicateContextFactory = new PredicateContextFactory(new GlobalConfig(),
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
        QueryContext queryContext = new QueryContext(predicateContextData, ImmutableList.of(), 0, false);
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

    private io.trino.plugin.warp.expression.rewrite.WarpExpression createWrapExpression()
    {
        Map<String, WarpVariable> columnNameToVaradaVariable = columns
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> new WarpVariable(x.getValue(), IntegerType.INTEGER)));
        Map<String, WarpExpression> leaves = Map.of(
                "a", createLeafExpression("a", 5L, columnNameToVaradaVariable),
                "b", createLeafExpression("b", 6L, columnNameToVaradaVariable),
                "c", createLeafExpression("c", 7L, columnNameToVaradaVariable),
                "d", createLeafExpression("d", 8L, columnNameToVaradaVariable));

        WarpCall rootExpression = new WarpCall(AND_FUNCTION_NAME.getName(),
                List.of(leaves.get("a"),
                        new WarpCall(AND_FUNCTION_NAME.getName(),
                                List.of(leaves.get("b"), leaves.get("c")),
                                BOOLEAN),
                        new WarpCall(OR_FUNCTION_NAME.getName(),
                                List.of(leaves.get("a"), leaves.get("b"), new WarpCall(OR_FUNCTION_NAME.getName(),
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
        List<WarpExpressionData> warpExpressionDataLeaves = columns.keySet().stream().map(columnName -> new WarpExpressionData(leaves.get(columnName),
                        IntegerType.INTEGER,
                        false,
                        Optional.of(nativeExpression),
                        new RegularColumn(columnName)))
                .toList();
        return new io.trino.plugin.warp.expression.rewrite.WarpExpression(rootExpression, warpExpressionDataLeaves);
    }

    private WarpCall createLeafExpression(String columnName, long value, Map<String, WarpVariable> columnNameToVaradaVariable)
    {
        return new WarpCall(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(columnNameToVaradaVariable.get(columnName), new WarpPrimitiveConstant(value, IntegerType.INTEGER)),
                BOOLEAN);
    }
}
