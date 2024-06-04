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
package io.trino.plugin.warp.expression.rewrite;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.ExperimentSupportedFunction;
import io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.NativeExpressionRulesHandler;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.RealOperators;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static io.trino.plugin.warp.WarpSessionProperties.ENABLE_OR_PUSHDOWN;
import static io.trino.plugin.warp.expression.rewrite.ExpressionService.PUSHDOWN_PREDICATES_STAT_GROUP;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.CEIL;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.CONTAINS;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.IS_NAN;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionServiceTest
{
    private static final FunctionName MOD = new FunctionName("mod");
    private final Variable doubleVariable1 = new Variable("double1", DoubleType.DOUBLE);
    private final Variable doubleVariable2 = new Variable("double2", DoubleType.DOUBLE);
    private final Variable longDecimalVariable = new Variable("longDecimal", createDecimalType(30, 2));
    private final VarcharType varcharType = VarcharType.createVarcharType(10);
    private final Variable varcharVariable = new Variable("varchar1", varcharType);
    private final Variable realVariable = new Variable("real1", RealType.REAL);
    private ExpressionService expressionService;
    private GlobalConfig globalConfig;
    private ConnectorSession connectorSession;
    private Map<String, ColumnHandle> assignments;
    private MetricsManager metricsManager;
    private Map<String, Long> customStats;

    static Stream<Arguments> nonDefaultFormatDoubleParams()
    {
        return Stream.of(
                arguments("01111111111111111111E0"),
                // '5' would be casted to 5.0E0, then, we'll gen an exception because 5.0E0 can't be casted back into varchar(1)
                arguments("5"));
    }

    static Stream<Arguments> arrayTypes()
    {
        return Stream.of(
                arguments(new ArrayType(VarcharType.VARCHAR), true),
                arguments(new ArrayType(VarcharType.createVarcharType(10)), true),
                arguments(new ArrayType(VarcharType.VARCHAR), true),
                arguments(new ArrayType(IntegerType.INTEGER), false),
                arguments(new ArrayType(DoubleType.DOUBLE), false),
                arguments(new ArrayType(BigintType.BIGINT), false));
    }

    @BeforeEach
    public void beforeEach()
    {
        this.customStats = new HashMap<>();
        globalConfig = new GlobalConfig();
        metricsManager = TestingTxService.createMetricsManager();
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        NativeExpressionRulesHandler nativeExpressionRulesHandler = new NativeExpressionRulesHandler(storageEngineConstants, metricsManager);
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        expressionService = new ExpressionService(dispatcherProxiedConnectorTransformer,
                new ExperimentSupportedFunction(metricsManager),
                globalConfig,
                new NativeConfig(),
                metricsManager,
                nativeExpressionRulesHandler);
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        assignments = createAssignments(doubleVariable1, doubleVariable2, varcharVariable, realVariable);
    }

    @Test
    public void testEmptyExpression()
    {
        ConnectorExpression expression = new Constant(true, BOOLEAN);
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual.isEmpty()).isTrue();
        assertPushdownStatsSum(0);
    }

    /**
     * is_nan(longDecimal)
     * decimal isn't valid type
     */
    @Test
    public void testInvalidType()
    {
        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(longDecimalVariable));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
    }

    /**
     * is_nan(double1)
     */
    @Test
    public void testWarpFunctionsIs_Nan()
    {
        WarpExpression expectedResult = new WarpCall(IS_NAN.getName(),
                List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);

        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(doubleVariable1));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * double1 > double2
     */
    @Test
    public void testFunctionContains2DistinctColumns_NotSupported()
    {
        ConnectorExpression call = new Call(
                BOOLEAN,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(
                        doubleVariable1,
                        doubleVariable2));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, call, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = false
     */
    @Test
    public void testInverseIsNan()
    {
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.FALSE);
        List<WarpExpressionData> expectedResult = List.of(
                new WarpExpressionData(isNanExpression.getValue(),
                        doubleVariable1.getType(),
                        false,
                        Optional.empty(),
                        new RegularColumn(doubleVariable1.getName())));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, isNanExpression.getKey(), assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = true
     */
    @Test
    public void testValidIsNan()
    {
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        NativeExpression expectedNativeExpression = new NativeExpression(PredicateType.PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_IS_NAN,
                Domain.singleValue(BOOLEAN, true),
                false,
                true,
                Collections.emptyList(), TransformFunction.NONE);
        List<WarpExpressionData> expectedResult = List.of(
                new WarpExpressionData(isNanExpression.getValue(),
                        doubleVariable1.getType(),
                        false,
                        Optional.of(expectedNativeExpression),
                        new RegularColumn(doubleVariable1.getName())));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, isNanExpression.getKey(), assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * ceil(double1) > 5
     */
    @Test
    public void testWarpFunctionsClientCeil()
    {
        Pair<Call, WarpCall> ceilExpression = createCallExpression(CEIL,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, doubleVariable1.getType()));

        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, ceilExpression.getKey(), assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(0).getExpression()).isEqualTo(ceilExpression.getValue());
        assertPushdownStatsSum(0);
    }

    /**
     * where lower(v1) == null
     * translated by trino to:
     * WarpConstant Boolean:null
     */
    @Test
    public void testConstantExpression()
    {
        Constant constant = new Constant(null, BOOLEAN);
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, constant, assignments, customStats);
        assertThat(actual).isEmpty();
    }

    /**
     * where double1 is null or is_nan(double1) = true or ceil(double1) > 5
     */
    @Test
    public void testExpressionWithNull2()
    {
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, WarpCall> right = createCallExpression(CEIL,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));

        Call isNull = new Call(doubleVariable1.getType(),
                IS_NULL_FUNCTION_NAME,
                List.of(new Variable(doubleVariable1.getName(), doubleVariable1.getType())));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(isNull, isNanExpression.getKey(), right.getKey()));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
    }

    /**
     * contains(arr1 , is_nan(col1))
     */
    @Test
    public void testNestedFunctionWith2differentColumns_notSupported()
    {
        String arr1 = "arr1";
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(IntegerType.INTEGER, arr1);
        assignments.put(arr1, columnHandle);
        ConnectorExpression call = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(
                        new Variable(arr1, new ArrayType(BOOLEAN)),
                        new Call(
                                BooleanType.BOOLEAN,
                                IS_NAN,
                                List.of(doubleVariable1))));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, call, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) = 5  or unsupported(double1)=true
     */
    @Test
    public void testFunctionWithOrSameColumnOneFunctionNotSupported()
    {
        Pair<Call, WarpCall> expression = createCallExpression(CEIL,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression connectorExpression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expression.getKey(),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                                List.of(
                                        new Call(
                                                BOOLEAN,
                                                new FunctionName("unsupported"),
                                                List.of(doubleVariable1)),
                                        new Constant(true, BOOLEAN)))));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, connectorExpression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = true  and unsupported(double1) > 5
     */
    @Test
    public void testFunctionWith_And_UnsupportedFunction()
    {
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);

        Optional<NativeExpression> expectedNativeExpression = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_IS_NAN)
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false))
                .collectNulls(false)
                .build());
        List<WarpExpressionData> expectedResult = List.of(new WarpExpressionData(isNanExpression.getValue(),
                doubleVariable1.getType(),
                false,
                expectedNativeExpression,
                new RegularColumn(doubleVariable1.getName())));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                                List.of(
                                        new Call(
                                                BOOLEAN,
                                                new FunctionName("unsupported"),
                                                List.of(doubleVariable1)),
                                        new Constant(5, IntegerType.INTEGER))),
                        isNanExpression.getKey()));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) > 5  or is_nan(double1)=true
     */
    @Test
    public void testFunctionWithOrFunctionSameColumn()
    {
        Pair<Call, WarpCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));

        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(2);
    }

    /**
     * is_nan(double1) = true  or unsupported(double1) > 5
     */
    @Test
    public void testFunctionWith_OR_UnsupportedFunction()
    {
        Call left = new Call(
                BOOLEAN,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(
                        new Call(
                                BOOLEAN,
                                new FunctionName("unsupported"),
                                List.of(doubleVariable1)),
                        new Constant(5D, DoubleType.DOUBLE)));
        Call right = new Call(
                BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(
                        new Call(
                                BOOLEAN,
                                IS_NAN,
                                List.of(doubleVariable1)),
                        new Constant(true, BOOLEAN)));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left, right));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) = 5  and is_nan(double1)=true
     */
    @Test
    public void testFunctionWithAndFunctionSameColumn()
    {
        Pair<Call, WarpCall> left = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(2);
    }

    /**
     * hypothetical case of And of OR's - native does not support this
     * (double1 > 50 AND double2 < 10) OR (varcharVariable = 'bla' AND double2 > 5)
     */
    @Test
    public void testOrOfAnds()
    {
        ConnectorSession orEnabled = mock(ConnectorSession.class);
        when(orEnabled.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        double leftValue = 5D;
        double rightValue = 10D;
        Pair<Call, WarpCall> left = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable1, new Constant(leftValue, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right = createCallExpression(LESS_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(rightValue, DoubleType.DOUBLE));
        ConnectorExpression leftOrExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        left = createCallExpression(EQUAL_OPERATOR_FUNCTION_NAME, varcharVariable, new Constant(Slices.utf8Slice("bla"), VarcharType.VARCHAR));
        right = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(leftValue, DoubleType.DOUBLE));
        ConnectorExpression rightOrExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        ConnectorExpression connectorExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftOrExpression, rightOrExpression));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression = expressionService.convertToWarpExpression(orEnabled, connectorExpression, assignments, customStats);
        assertThat(warpExpression.orElseThrow().warpExpressionDataLeaves().size()).isEqualTo(4);
    }

    /**
     * double1 > 50 OR double2 < 10
     */
    @Test
    public void testWarpExpression()
    {
        ConnectorSession orEnabled = mock(ConnectorSession.class);
        when(orEnabled.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        double leftValue = 5D;
        double rightValue = 10D;
        Pair<Call, WarpCall> left = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable1, new Constant(leftValue, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right = createCallExpression(LESS_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(rightValue, DoubleType.DOUBLE));
        ConnectorExpression connectorExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        WarpExpression warpExpression = new WarpCall(OR_FUNCTION_NAME.getName(), List.of(left.getValue(), right.getValue()), BOOLEAN);

        WarpExpressionData leftWarpExpressionData = new WarpExpressionData(left.getValue(),
                doubleVariable1.getType(),
                false,
                Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                        FunctionType.FUNCTION_TYPE_NONE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(doubleVariable1.getType(), leftValue)), false),
                        false,
                        false,
                        Collections.emptyList(), TransformFunction.NONE)),
                new RegularColumn(doubleVariable1.getName()));
        WarpExpressionData rightWarpExpressionData = new WarpExpressionData(right.getValue(),
                doubleVariable2.getType(),
                false,
                Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                        FunctionType.FUNCTION_TYPE_NONE,
                        Domain.create(ValueSet.ofRanges(Range.lessThan(doubleVariable2.getType(), rightValue)), false),
                        false,
                        false,
                        Collections.emptyList(), TransformFunction.NONE)),
                new RegularColumn(doubleVariable2.getName()));
        io.trino.plugin.warp.expression.rewrite.WarpExpression expectedSiacExpression = new io.trino.plugin.warp.expression.rewrite.WarpExpression(warpExpression, List.of(leftWarpExpressionData, rightWarpExpressionData));
        io.trino.plugin.warp.expression.rewrite.WarpExpression newWarpExpression = expressionService.convertToWarpExpression(orEnabled, connectorExpression, assignments, customStats).orElseThrow();
        assertThat(newWarpExpression).isEqualTo(expectedSiacExpression);
    }

    /**
     * ceil(double1) = 5  or ceil(double1) > 10
     */
    @Test
    public void testAggregateFunctionSameFunctionSameColumn()
    {
        Pair<Call, WarpCall> left = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), right.getKey()));
        List<WarpExpressionData> warpExpressions = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(warpExpressions.size()).isEqualTo(2);
    }

    /**
     * ceil(double1) > 5  and is_nan(double2)=true
     */
    @Test
    public void testFunction2DifferentColumns_Allowed()
    {
        Pair<Call, WarpCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                Constant.TRUE);
        Optional<NativeExpression> expectedNativeExpression1 = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_RANGES)
                .functionType(FunctionType.FUNCTION_TYPE_CEIL)
                .domain(Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 5D)), false))
                .collectNulls(false)
                .build());
        Optional<NativeExpression> expectedNativeExpression2 = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_IS_NAN)
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false))
                .collectNulls(false)
                .build());
        RegularColumn warpColumn1 = new RegularColumn(doubleVariable1.getName());
        RegularColumn warpColumn2 = new RegularColumn(doubleVariable2.getName());
        List<WarpExpressionData> expectedResult = List.of(new WarpExpressionData(left.getValue(), doubleVariable1.getType(), false, expectedNativeExpression1, warpColumn1),
                new WarpExpressionData(isNanExpression.getValue(), doubleVariable2.getType(), false, expectedNativeExpression2, warpColumn2));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * (ceil(double1) > 10 AND (mode(double1, 3) = 1 OR mod(double1, 2) = 0))
     */
    @Test
    public void testComplex1()
    {
        Pair<Call, WarpCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> middle = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(1D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(middle.getKey(), right.getKey()));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), rightSide));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
        assertPushdownStatsSum(2);
    }

    /**
     * (ceil(double2) > 10 AND (mode(double1, 3) = 1 OR mod(double1, 2) = 0))
     */
    @Test
    public void testComplex2()
    {
        Pair<Call, WarpCall> expectedLeft = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> left = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(1D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), right.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(expectedLeft.getKey(), rightSide));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double2 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplex()
    {
        Pair<Call, WarpCall> expectedLeft1 = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, WarpCall> right1 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right2 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double2 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplex2()
    {
        Pair<Call, WarpCall> expectedLeft1 = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, WarpCall> right1 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right2 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    static Stream<Arguments> unsupportedTypes()
    {
        return Stream.of(
                arguments(new MapType(TimeWithTimeZoneType.createTimeWithTimeZoneType(12), VarcharType.VARCHAR, new TypeOperators())),
                arguments(new MapType(VarcharType.VARCHAR, TimeWithTimeZoneType.createTimeWithTimeZoneType(12), new TypeOperators())),
                arguments(TimeWithTimeZoneType.createTimeWithTimeZoneType(12)),
                arguments(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(12)),
                arguments(DecimalType.createDecimalType(30, 10)));
    }

    /**
     * unsupportedType -> nothing
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnType(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression expression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression).isEmpty();
    }

    /**
     * ceil(double1)  or unsupportedType -> nothing
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnTypeWithOr(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression invalidExpression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Pair<Call, WarpCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), invalidExpression));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression).isEmpty();
    }

    /**
     * ceil(double1)  And unsupportedType -> ceil(double1)
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnTypeWithAnd(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression invalidExpression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Pair<Call, WarpCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), invalidExpression));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression.isPresent()).isTrue();
        assertThat(warpExpression.orElseThrow().warpExpressionDataLeaves().size()).isOne();
        assertThat(warpExpression.orElseThrow().rootExpression()).isEqualTo(left.getValue());
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double1 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplexRightSideIsNotSupported()
    {
        Pair<Call, WarpCall> expectedLeft1 = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        Pair<Call, WarpCall> right1 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, WarpCall> right2 = createModWarpCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    /**
     * ( doubleVariable1 = 4  AND  doubleVariable2 <= 6 )  OR  doubleVariable1 = 2
     */
    @Test
    public void testHypotheticalCaseOrROfANDs()
    {
        Call leftSide = new Call(
                BOOLEAN, AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable1, new Constant(4D, DoubleType.DOUBLE))),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)))));

        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(doubleVariable1, new Constant(2D, DoubleType.DOUBLE)));

        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftSide, rightSide));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(result.orElseThrow().warpExpressionDataLeaves().size()).isEqualTo(3);
    }

    /**
     * ( doubleVariable1 = 4  AND  doubleVariable2 <= 6 )  OR  varchar = 'b'
     */
    @Test
    public void testMultiLevelExp()
    {
        Call leftSide = new Call(
                BOOLEAN, AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable1, new Constant(4D, DoubleType.DOUBLE))),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)))));

        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(varcharVariable, new Constant(Slices.utf8Slice("b"), VarcharType.VARCHAR)));

        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftSide, rightSide));

        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(result.orElseThrow().warpExpressionDataLeaves().size()).isEqualTo(3);
    }

    @Test
    public void testUnsupportedTooManyLevels()
    {
        Call leftSide = new Call(
                BOOLEAN,
                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)));
        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(varcharVariable, new Constant(Slices.utf8Slice("b"), VarcharType.VARCHAR)));

        Call l4Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(leftSide, rightSide));
        Call l3Expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(l4Expression, rightSide));
        Call l2Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(l3Expression, rightSide));
        Call l1Expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(l2Expression, rightSide));
        Call l0Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(l1Expression, rightSide));

        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, l1Expression, assignments, customStats);
        assertThat(result).isNotEmpty();
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_expression_depth()).isEqualTo(0);

        result = expressionService.convertToWarpExpression(connectorSession, l0Expression, assignments, customStats);
        assertThat(result).isEmpty();
        assertThat(pushdownPredicatesStats.getunsupported_expression_depth()).isEqualTo(1);
    }

    @Test
    public void testUnsupportAllExpression()
    {
        globalConfig.setUnsupportedFunctions("*");

        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(doubleVariable1));
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
    }

    /**
     * where cast(s_real as varchar(20)) = '5'
     */
    @Test
    public void testCastRealToVarchar()
    {
        Call castCall = new Call(VarcharType.createVarcharType(20), CAST_FUNCTION_NAME, List.of(realVariable));
        Slice slice = RealOperators.castToVarchar(20, 5L);
        Call expression = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(slice, VarcharType.VARCHAR)));
        ColumnHandle realColumn = assignments.get(realVariable.getName());
        WarpCall expectedCastCall = new WarpCall(CAST_FUNCTION_NAME.getName(), List.of(new WarpVariable(realColumn, realVariable.getType())), VarcharType.createVarcharType(20));
        WarpCall expectedResult = new WarpCall(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpSliceConstant(slice, VarcharType.VARCHAR)),
                BOOLEAN);
        List<WarpExpressionData> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(result.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as real) > 10
     */
    @Test
    public void testCastDoubleToReal()
    {
        ColumnHandle realColumn = assignments.get(realVariable.getName());
        Call castCall = new Call(RealType.REAL, CAST_FUNCTION_NAME, List.of(realVariable));
        Call warpExpression = new Call(BooleanType.BOOLEAN,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(10L, RealType.REAL)));
        WarpCall expectedCastCall = new WarpCall(CAST_FUNCTION_NAME.getName(), List.of(new WarpVariable(realColumn, realVariable.getType())), RealType.REAL);
        WarpCall expectedResult = new WarpCall(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(10L, RealType.REAL)),
                BOOLEAN);
        List<WarpExpressionData> result = expressionService.convertToWarpExpression(connectorSession, warpExpression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(result.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as real) > 10 and cast(doubleVariable1 as real) < 100
     */
    @Test
    public void testCastDoubleToRealWithAnd()
    {
        Call castCall = new Call(RealType.REAL, CAST_FUNCTION_NAME, List.of(realVariable));
        Call leftSide = new Call(BooleanType.BOOLEAN,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(10L, RealType.REAL)));
        Call rightSide = new Call(BooleanType.BOOLEAN,
                LESS_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(100L, RealType.REAL)));
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(leftSide, rightSide));

        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(RealType.REAL, 10L)), false);
        List<WarpExpressionData> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        Optional<NativeExpression> expectedResult = Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_REAL.ordinal()), TransformFunction.NONE));
        assertThat(result.get(0).getNativeExpressionOptional()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * The same double can be represented in different formats.
     * For example, 0.01111111111111111111E0 == 1.1111111111111112E-2
     * In Trino: cast(0.01111111111111111111E0 as varchar) = '1.1111111111111112E-2' is True
     * cast(0.01111111111111111111E0 as varchar) = '01111111111111111111E0' is False
     */
    @ParameterizedTest
    @MethodSource("nonDefaultFormatDoubleParams")
    public void testCastDoubleToVarcharNonDefaultFormat(String doubleAsString)
    {
        Call castCall = new Call(
                VarcharType.VARCHAR,
                CAST_FUNCTION_NAME,
                List.of(doubleVariable1));
        Constant nonDefaultFormatConstant = new Constant(
                Slices.utf8Slice(doubleAsString),
                VarcharType.VARCHAR);
        Call warpExpression = new Call(
                BooleanType.BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, nonDefaultFormatConstant));
        NativeExpression expectedResult = NativeExpression.builder()
                .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .domain(Domain.none(DoubleType.DOUBLE))
                .collectNulls(false)
                .build();
        List<WarpExpressionData> result = expressionService.convertToWarpExpression(connectorSession, warpExpression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertPushdownStatsSum(0);
        assertThat(result.get(0).getNativeExpressionOptional().orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * exception during rewrite
     */
    @Test
    public void failedRewriteExpression()
    {
        Call castCall = new Call(VarcharType.createVarcharType(20), CAST_FUNCTION_NAME, List.of(realVariable));
        Slice slice = RealOperators.castToVarchar(20, 5L);
        Call expression = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(slice, VarcharType.VARCHAR)));
        Map<String, ColumnHandle> invalidAssignment = Collections.emptyMap();
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, invalidAssignment, customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getfailed_rewrite_expression()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    public void testContainsOnArrayColumn(ArrayType arrayType, boolean expectedIsValid)
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(arrayType, "arr_column");
        Slice slice = Slices.utf8Slice("test");
        Call expression = new Call(
                BOOLEAN,
                CONTAINS,
                List.of(new Variable(columnHandle.name(), columnHandle.type()),
                        new Constant(slice, arrayType.getElementType())));
        assignments.put(columnHandle.name(), columnHandle);

        List<WarpExpressionData> expectedResult;
        if (expectedIsValid) {
            WarpExpression expectedWarpExpression = new WarpCall(CONTAINS.getName(),
                    List.of(new WarpVariable(columnHandle, columnHandle.type()),
                            new WarpSliceConstant(slice, arrayType.getElementType())),
                    BOOLEAN);
            RegularColumn regularColumn = new RegularColumn(columnHandle.name());
            expectedResult = List.of(new WarpExpressionData(expectedWarpExpression,
                    columnHandle.type(),
                    false,
                    Optional.empty(),
                    regularColumn));
        }
        else {
            expectedResult = List.of();
        }
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        if (expectedResult.isEmpty()) {
            assertThat(actual).isEmpty();
        }
        else {
            assertThat(actual.orElseThrow().warpExpressionDataLeaves()).isEqualTo(expectedResult);
        }
    }

    /**
     * select * from functionsTable where ceil(double1) = 5  or ceil(double1) = 9
     * translated by Trino into:
     * Call[functionName=name='$in',
     * arguments=[Call[functionName=name='ceil', arguments=[double1::double]],
     * Call[functionName=name='$array', arguments=[5.0::double, 9.0::double]]]]
     */
    @Test
    public void testInFunction()
    {
        Call ceilCall = new Call(DoubleType.DOUBLE,
                CEIL,
                List.of(doubleVariable1));

        ArrayType arrayType = new ArrayType(DoubleType.DOUBLE);
        Call arrayCall = new Call(arrayType,
                ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                List.of(new Constant(5D, DoubleType.DOUBLE), new Constant(9D, DoubleType.DOUBLE)));
        Call callExpression = new Call(BooleanType.BOOLEAN,
                IN_PREDICATE_FUNCTION_NAME,
                List.of(ceilCall, arrayCall));

        Type doubleType = doubleVariable1.getType();
        WarpCall expectedCeilCall = new WarpCall(CEIL.getName(),
                List.of(new WarpVariable(assignments.get(doubleVariable1.getName()), doubleType)),
                doubleType);

        WarpCall expectedArrayCall = new WarpCall(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName(),
                List.of(new WarpPrimitiveConstant(5D, DoubleType.DOUBLE), new WarpPrimitiveConstant(9D, DoubleType.DOUBLE)),
                arrayType);
        WarpExpression expectedExpression = new WarpCall(IN_PREDICATE_FUNCTION_NAME.getName(),
                List.of(expectedCeilCall, expectedArrayCall),
                BOOLEAN);
        NativeExpression expectedNativeExpression = NativeExpression
                .builder()
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(doubleType, 5D), Range.equal(doubleType, 9D)), false))
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_CEIL)
                .collectNulls(false)
                .functionParams(Collections.emptyList())
                .build();

        List<WarpExpressionData> expectedResult = List.of(new WarpExpressionData(expectedExpression,
                doubleType,
                false,
                Optional.of(expectedNativeExpression),
                new RegularColumn(doubleVariable1.getName())));

        List<WarpExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, callExpression, assignments, customStats).orElseThrow().warpExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
    }

    private Map<String, ColumnHandle> createAssignments(Variable... variables)
    {
        Map<String, ColumnHandle> assignments = new HashMap<>();
        for (Variable variable : variables) {
            assignments.put(variable.getName(), new TestingConnectorColumnHandle(variable.getType(), variable.getName()));
        }
        return assignments;
    }

    private WarpVariable createExpectedVariable(Variable variable)
    {
        return new WarpVariable(assignments.get(variable.getName()), variable.getType());
    }

    private Pair<Call, WarpCall> createCallExpression(FunctionName functionName,
            FunctionName operator,
            Variable variable,
            Constant operatorValue)
    {
        WarpConstant warpConstant = convertConstantToWarpConstant(operatorValue);
        WarpCall warpCall = new WarpCall(operator.getName(),
                List.of(new WarpCall(functionName.getName(),
                                List.of(createExpectedVariable(variable)), operatorValue.getType()),
                        warpConstant),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(
                        new Call(
                                operatorValue.getType(),
                                functionName,
                                List.of(variable)),
                        operatorValue));
        return Pair.of(call, warpCall);
    }

    private Pair<Call, WarpCall> createCallExpression(FunctionName operator,
            Variable variable,
            Constant operatorValue)
    {
        WarpConstant warpConstant = convertConstantToWarpConstant(operatorValue);
        WarpCall warpCall = new WarpCall(operator.getName(),
                List.of(createExpectedVariable(variable), warpConstant),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(variable, operatorValue));
        return Pair.of(call, warpCall);
    }

    private WarpConstant convertConstantToWarpConstant(Constant operatorValue)
    {
        WarpConstant warpConstant;
        if (operatorValue.getValue() instanceof Slice) {
            warpConstant = new WarpSliceConstant((Slice) operatorValue.getValue(), operatorValue.getType());
        }
        else {
            warpConstant = new WarpPrimitiveConstant(operatorValue.getValue(), operatorValue.getType());
        }
        return warpConstant;
    }

    private Pair<Call, WarpCall> createModWarpCall(FunctionName operator, Variable variable, Constant modeValue, Constant operatorValue)
    {
        assertThat(variable.getType()).isEqualTo(modeValue.getType()).isEqualTo(operatorValue.getType());
        WarpCall warpCall = new WarpCall(operator.getName(),
                List.of(new WarpCall(MOD.getName(),
                                List.of(createExpectedVariable(variable),
                                        new WarpPrimitiveConstant(modeValue.getValue(), modeValue.getType())),
                                modeValue.getType()),
                        new WarpPrimitiveConstant(operatorValue.getValue(), operatorValue.getType())),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(
                        new Call(
                                modeValue.getType(),
                                MOD,
                                List.of(variable, modeValue)),
                        operatorValue));
        return Pair.of(call, warpCall);
    }

    private void assertPushdownStatsSum(int expectedCount)
    {
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getCounters().values().stream().mapToLong(LongAdder::longValue).sum()).isEqualTo(expectedCount);
    }
}
