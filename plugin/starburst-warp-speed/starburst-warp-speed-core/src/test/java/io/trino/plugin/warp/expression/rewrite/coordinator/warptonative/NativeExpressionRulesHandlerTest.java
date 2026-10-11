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
package io.trino.plugin.warp.expression.rewrite.coordinator.warptonative;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.ELEMENT_AT;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_NONE;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_VALUES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_VALUES;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NativeExpressionRulesHandlerTest
{
    public static final FunctionName CEIL = new FunctionName("ceiling");
    public static final FunctionName IS_NAN = new FunctionName("is_nan");

    private final Variable doubleVariable1 = new Variable("double1", DoubleType.DOUBLE);
    private final Variable doubleVariable2 = new Variable("double2", DoubleType.DOUBLE);
    private final Variable realVariable = new Variable("real1", RealType.REAL);
    private final Variable integerVariable = new Variable("integer1", IntegerType.INTEGER);
    private final Variable bigintVariable = new Variable("bigint1", BigintType.BIGINT);
    private final Variable timestampVariable = new Variable("timestamp1", TimestampType.createTimestampType(3));
    private final Variable longTimestampVariable = new Variable("timestamp9", TimestampType.createTimestampType(9));
    private final Variable timestampWithTimeZoneVariable = new Variable("timestamptz1", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3));
    private final VarcharType varcharType = VarcharType.createVarcharType(10);
    private final Variable varcharVariable = new Variable("varchar1", varcharType);

    private Map<String, ColumnHandle> assignments;
    private NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private MetricsManager metricsManager;
    private Map<String, Long> customStats;

    static Stream<Arguments> ceilWithOperators()
    {
        return Stream.of(
                arguments(
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.equal(DoubleType.DOUBLE, 5d),
                        PREDICATE_TYPE_VALUES),
                arguments(
                        LESS_THAN_OPERATOR_FUNCTION_NAME,
                        Range.lessThan(DoubleType.DOUBLE, 5d),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(
                        LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.lessThanOrEqual(DoubleType.DOUBLE, 5d),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(
                        StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                        Range.greaterThan(DoubleType.DOUBLE, 5d),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(
                        GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.greaterThanOrEqual(DoubleType.DOUBLE, 5d),
                        PredicateType.PREDICATE_TYPE_RANGES));
    }

    static Stream<Arguments> rangeOperatorParamsUnsupported()
    {
        return Stream.of(
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME));
    }

    static Stream<Arguments> equalOperatorParamsSupported()
    {
        return Stream.of(
                arguments(VarcharType.VARCHAR),
                arguments(VarcharType.createVarcharType(20)),
                arguments(VarcharType.createVarcharType(10)));
    }

    static Stream<Arguments> castDoubleToReal()
    {
        Type type = RealType.REAL;
        long value = 5L;
        return Stream.of(
                arguments(EQUAL_OPERATOR_FUNCTION_NAME, Domain.singleValue(type, value)),
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false)),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false)),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false)),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false)));
    }

    static Stream<Arguments> castIntegerToDouble()
    {
        Type type = DoubleType.DOUBLE;
        double value = 5d;
        return Stream.of(
                arguments(EQUAL_OPERATOR_FUNCTION_NAME, Domain.singleValue(type, value)),
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false)),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false)),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false)),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false)));
    }

    static Stream<Arguments> castDoubleToInteger()
    {
        Type type = IntegerType.INTEGER;
        long value = 5L;
        return Stream.of(
                arguments(EQUAL_OPERATOR_FUNCTION_NAME, Domain.singleValue(type, value)),
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false)),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false)),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false)),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false)));
    }

    public static Map<String, ColumnHandle> createAssignments(Variable... variables)
    {
        Map<String, ColumnHandle> assignments = new HashMap<>();
        for (Variable variable : variables) {
            TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(variable.getType(), variable.getName());
            assignments.put(variable.getName(), columnHandle);
        }
        return assignments;
    }

    @BeforeEach
    public void beforeEach()
    {
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        metricsManager = TestingTxService.createMetricsManager();
        nativeExpressionRulesHandler = new NativeExpressionRulesHandler(storageEngineConstants, metricsManager);
        assignments = createAssignments(
                doubleVariable1,
                doubleVariable2,
                varcharVariable,
                realVariable,
                integerVariable,
                bigintVariable,
                timestampVariable,
                longTimestampVariable,
                timestampWithTimeZoneVariable);
        customStats = new HashMap<>();
    }

    @Test
    public void failedRewriteExpression()
    {
        WarpExpression invalidWarpExpression = new WarpCall(CEIL.getName(), List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(invalidWarpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.empty());
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getfailed_rewrite_to_native_expression()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = false
     */
    @Test
    public void testWarpExpressionIsNanWithEqual()
    {
        WarpExpression warpExpression = new WarpCall(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new WarpCall(
                                IS_NAN.getName(),
                                List.of(createExpectedVariable(doubleVariable1)),
                                BOOLEAN),
                        WarpPrimitiveConstant.FALSE),
                BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.empty());
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * is_nan(double1)
     */
    @Test
    public void testWarpExpressionIsNan()
    {
        WarpExpression warpExpression = new WarpCall(IS_NAN.getName(), List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);
        Range range = Range.equal(BOOLEAN, true);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);
        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_IS_NAN,
                domain,
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * element_at(mapColumn, 'key1') = 'val1'
     */
    @Test
    public void testMapElementAtExpression()
    {
        MapType mapType = new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators());
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(mapType, "mapColumn");
        WarpVariable warpVariable = new WarpVariable(columnHandle, mapType);
        WarpSliceConstant sliceConstant = new WarpSliceConstant(Slices.utf8Slice("val1"), VarcharType.VARCHAR);
        WarpExpression warpExpression = new WarpCall(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new WarpCall(
                                ELEMENT_AT.getName(),
                                List.of(warpVariable, new WarpSliceConstant(Slices.utf8Slice("key1"), mapType)),
                                BOOLEAN),
                        sliceConstant),
                BOOLEAN);
        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_STRING_VALUES,
                FunctionType.FUNCTION_TYPE_TRANSFORMED,
                Domain.create(ValueSet.ofRanges(Range.equal(sliceConstant.getType(), sliceConstant.getValue())), false),
                false,
                true,
                Collections.emptyList(),
                new TransformFunction(
                        TransformFunction.TransformType.ELEMENT_AT,
                        List.of(new WarpSliceConstant(Slices.utf8Slice("key1"), mapType))));
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, mapType, Collections.emptySet(), customStats);
        assertThat(result.orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * json_extract_scalar(varcharCol, '$.number') = '12.345678'
     */
    @Test
    void testJsonExtractScalarExpression()
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "varcharCol");
        WarpVariable warpVariable = new WarpVariable(columnHandle, VarcharType.VARCHAR);
        WarpSliceConstant sliceConstant = new WarpSliceConstant(Slices.utf8Slice("12.345678"), VarcharType.VARCHAR);
        WarpExpression warpExpression = new WarpCall(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new WarpCall(
                                SupportedFunctions.JSON_EXTRACT_SCALAR.getName(),
                                List.of(warpVariable, new WarpSliceConstant(Slices.utf8Slice("$.number"), VarcharType.VARCHAR)),
                                BOOLEAN),
                        sliceConstant),
                BOOLEAN);
        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_STRING_VALUES,
                FunctionType.FUNCTION_TYPE_TRANSFORMED,
                Domain.create(ValueSet.ofRanges(Range.equal(sliceConstant.getType(), sliceConstant.getValue())), false),
                false,
                true,
                Collections.emptyList(),
                new TransformFunction(
                        TransformFunction.TransformType.JSON_EXTRACT_SCALAR,
                        List.of(new WarpPrimitiveConstant("$.number", VarcharType.VARCHAR))));
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, VarcharType.VARCHAR, Collections.emptySet(), customStats);
        assertThat(result.orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * unsupported(double1) = false
     */
    @Test
    public void testUnsupportedExpression()
    {
        WarpExpression warpExpression = new WarpCall(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new WarpCall(
                                "unsupported",
                                List.of(createExpectedVariable(doubleVariable1)),
                                BOOLEAN),
                        WarpPrimitiveConstant.FALSE),
                BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("ceilWithOperators")
    public void testCeilWithOperators(
            FunctionName functionName,
            Range expectedRange,
            PredicateType expectedPredicateType)
    {
        WarpExpression warpExpression = new WarpCall(
                functionName.getName(),
                List.of(new WarpCall(
                                CEIL.getName(),
                                List.of(createExpectedVariable(doubleVariable1)),
                                BOOLEAN),
                        new WarpPrimitiveConstant(5d, DoubleType.DOUBLE)),
                BOOLEAN);
        ValueSet valueSet = ValueSet.ofRanges(expectedRange);
        Domain domain = Domain.create(valueSet, false);
        NativeExpression expectedResult = new NativeExpression(
                expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CEIL,
                domain,
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * (ceil(double1) > 5) = false
     */
    @Test
    public void testDomainAlreadySet()
    {
        WarpExpression warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new WarpCall(
                                GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
                                List.of(new WarpCall(
                                                CEIL.getName(),
                                                List.of(createExpectedVariable(doubleVariable1)),
                                                BOOLEAN),
                                        new WarpPrimitiveConstant(5L, IntegerType.INTEGER)),
                                BOOLEAN),
                        new WarpPrimitiveConstant(false, BOOLEAN)),
                BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertPushdownStatsSum(1);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(s_real as varchar) rangesOperator '5'
     */
    @ParameterizedTest
    @MethodSource("rangeOperatorParamsUnsupported")
    public void testCastRealToVarchar_unsupported(FunctionName operator)
    {
        String stringValue = "5";
        Slice slice = Slices.utf8Slice(stringValue);
        Variable columnType = realVariable;
        ColumnHandle realColumn = assignments.get(columnType.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(realColumn, columnType.getType())),
                VarcharType.VARCHAR);
        WarpCall warpExpression = new WarpCall(
                operator.getName(),
                List.of(expectedCastCall, new WarpSliceConstant(slice, VarcharType.VARCHAR)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, columnType.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(s_real as varchar) = '2.624819E2' matches the run of reals that Trino casts to that string
     */
    @ParameterizedTest
    @MethodSource("equalOperatorParamsSupported")
    public void testCastRealToVarcharSupported(VarcharType varcharType)
    {
        float value = 2.624819e2f;
        int varcharLength = varcharType.getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        Slice slice = RealOperators.castToVarchar(varcharLength, floatToIntBits(value));
        Range expectedRange = realsCastingTo(slice, value, varcharLength);
        assertThat(expectedRange.isSingleValue()).isFalse();
        WarpCall castCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(assignments.get(realVariable.getName()), realVariable.getType())),
                varcharType);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(castCall, new WarpSliceConstant(slice, varcharType)),
                BOOLEAN);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_RANGES,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.create(ValueSet.ofRanges(expectedRange), false),
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, realVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(s_real as varchar) = '1.4E-45' matches the one real that Trino casts to that string
     */
    @Test
    public void testCastRealToVarcharUniqueForm()
    {
        float value = Float.MIN_VALUE;
        Slice slice = RealOperators.castToVarchar(VarcharType.UNBOUNDED_LENGTH, floatToIntBits(value));
        Range expectedRange = realsCastingTo(slice, value, VarcharType.UNBOUNDED_LENGTH);
        assertThat(expectedRange.isSingleValue()).isTrue();
        WarpCall warpExpression = castToVarcharEquals(realVariable, slice);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.singleValue(RealType.REAL, (long) floatToIntBits(value)),
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, realVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    // Walks outward from the seed over every real that Trino's own cast prints as the slice.
    private static Range realsCastingTo(Slice slice, float seed, int varcharLength)
    {
        float lowValue = seed;
        while (RealOperators.castToVarchar(varcharLength, floatToIntBits(Math.nextDown(lowValue))).equals(slice)) {
            lowValue = Math.nextDown(lowValue);
        }
        float highValue = seed;
        while (RealOperators.castToVarchar(varcharLength, floatToIntBits(Math.nextUp(highValue))).equals(slice)) {
            highValue = Math.nextUp(highValue);
        }
        return Range.range(RealType.REAL, (long) floatToIntBits(lowValue), true, (long) floatToIntBits(highValue), true);
    }

    /**
     * where cast(s_double as varchar) = value stays with Trino for signed zero and NaN
     */
    @ParameterizedTest
    @ValueSource(strings = {"0E0", "-0E0", "NaN"})
    public void testCastDoubleZeroOrNaNToVarcharNotPushed(String value)
    {
        assertCastToVarcharNotPushed(doubleVariable1, value);
    }

    /**
     * where cast(s_real as varchar) = value stays with Trino for signed zero and NaN
     */
    @ParameterizedTest
    @ValueSource(strings = {"0E0", "-0E0", "NaN"})
    public void testCastRealZeroOrNaNToVarcharNotPushed(String value)
    {
        assertCastToVarcharNotPushed(realVariable, value);
    }

    /**
     * where cast(s_real as varchar) in ('2.624819E2', '1.4E-45') matches the run of reals and the single real
     */
    @Test
    public void testCastRealToVarcharIn()
    {
        float runValue = 2.624819e2f;
        Slice runSlice = RealOperators.castToVarchar(VarcharType.UNBOUNDED_LENGTH, floatToIntBits(runValue));
        Range runRange = realsCastingTo(runSlice, runValue, VarcharType.UNBOUNDED_LENGTH);
        float singleValue = Float.MIN_VALUE;
        Slice singleSlice = RealOperators.castToVarchar(VarcharType.UNBOUNDED_LENGTH, floatToIntBits(singleValue));
        WarpCall warpExpression = castToVarcharIn(realVariable, List.of(runSlice, singleSlice));

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_RANGES,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.create(ValueSet.ofRanges(Range.equal(RealType.REAL, (long) floatToIntBits(singleValue)), runRange), false),
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, realVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(s_real as varchar) in ('1.4E-45', '-0E0') stays with Trino
     */
    @Test
    public void testCastRealToVarcharInWithZeroNotPushed()
    {
        Slice singleSlice = RealOperators.castToVarchar(VarcharType.UNBOUNDED_LENGTH, floatToIntBits(Float.MIN_VALUE));
        assertNotPushed(castToVarcharIn(realVariable, List.of(singleSlice, Slices.utf8Slice("-0E0"))), realVariable);
    }

    /**
     * where cast(s_double as varchar) in ('5', '7') matches no row
     */
    @Test
    public void testCastDoubleToVarcharInNoneDomain()
    {
        WarpCall warpExpression = castToVarcharIn(doubleVariable1, List.of(Slices.utf8Slice("5"), Slices.utf8Slice("7")));

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_NONE,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.none(DoubleType.DOUBLE),
                false,
                true,
                Collections.emptyList(),
                TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    private void assertCastToVarcharNotPushed(Variable column, String stringValue)
    {
        assertNotPushed(castToVarcharEquals(column, Slices.utf8Slice(stringValue)), column);
    }

    private void assertNotPushed(WarpCall warpExpression, Variable column)
    {
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, column.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    private WarpCall castToVarcharEquals(Variable column, Slice slice)
    {
        return new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(castToVarchar(column), new WarpSliceConstant(slice, VarcharType.VARCHAR)),
                BOOLEAN);
    }

    private WarpCall castToVarcharIn(Variable column, List<Slice> slices)
    {
        WarpCall arrayCall = new WarpCall(
                ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName(),
                slices.stream()
                        .map(slice -> (WarpExpression) new WarpSliceConstant(slice, VarcharType.VARCHAR))
                        .collect(toImmutableList()),
                new ArrayType(VarcharType.VARCHAR));
        return new WarpCall(
                IN_PREDICATE_FUNCTION_NAME.getName(),
                List.of(castToVarchar(column), arrayCall),
                BOOLEAN);
    }

    private WarpCall castToVarchar(Variable column)
    {
        return new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(createExpectedVariable(column)),
                VarcharType.VARCHAR);
    }

    /**
     * * where cast(doubleVariable1 as real) operator 5
     */
    @ParameterizedTest
    @MethodSource("castDoubleToReal")
    public void testCastDoubleToReal(FunctionName functionName, Domain domain)
    {
        ColumnHandle doubleColumn = assignments.get(doubleVariable1.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(doubleColumn, doubleVariable1.getType())),
                RealType.REAL);
        WarpCall warpExpression = new WarpCall(
                functionName.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(5L, RealType.REAL)),
                BOOLEAN);

        PredicateType expectedPredicateType;
        if (domain.isSingleValue()) {
            expectedPredicateType = PREDICATE_TYPE_VALUES;
        }
        else {
            expectedPredicateType = PREDICATE_TYPE_RANGES;
        }
        NativeExpression expectedResult = new NativeExpression(
                expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_REAL.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(integerVariable as double) operator 5.0 - a non-real numeric CAST target on an
     * integer source. Native's int-source filler only handles a real target, so this must be
     * verified as a supported numeric pair rather than reaching that filler.
     */
    @ParameterizedTest
    @MethodSource("castIntegerToDouble")
    public void testCastIntegerToDouble(FunctionName functionName, Domain domain)
    {
        ColumnHandle integerColumn = assignments.get(integerVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(integerColumn, integerVariable.getType())),
                DoubleType.DOUBLE);
        WarpCall warpExpression = new WarpCall(
                functionName.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(5d, DoubleType.DOUBLE)),
                BOOLEAN);

        PredicateType expectedPredicateType = domain.isSingleValue() ? PREDICATE_TYPE_VALUES : PREDICATE_TYPE_RANGES;
        NativeExpression expectedResult = new NativeExpression(
                expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_DOUBLE.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, integerVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as integer) operator 5 - a non-real numeric CAST target on a
     * double source. Native's double-source cast family only handles a real target, so this must
     * be verified as a supported numeric pair rather than reaching that filler.
     */
    @ParameterizedTest
    @MethodSource("castDoubleToInteger")
    public void testCastDoubleToInteger(FunctionName functionName, Domain domain)
    {
        ColumnHandle doubleColumn = assignments.get(doubleVariable1.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(doubleColumn, doubleVariable1.getType())),
                IntegerType.INTEGER);
        WarpCall warpExpression = new WarpCall(
                functionName.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(5L, IntegerType.INTEGER)),
                BOOLEAN);

        PredicateType expectedPredicateType = domain.isSingleValue() ? PREDICATE_TYPE_VALUES : PREDICATE_TYPE_RANGES;
        NativeExpression expectedResult = new NativeExpression(
                expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_INTEGER.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as boolean) = true - outside the numeric CAST family entirely, no
     * native filler exists for this (source, target) pair, so pushdown must be rejected rather than
     * reaching a native filler that would misread the buffer.
     */
    @Test
    public void testCastUnsupportedTargetFallsBackToTrino()
    {
        ColumnHandle doubleColumn = assignments.get(doubleVariable1.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(doubleColumn, doubleVariable1.getType())),
                BooleanType.BOOLEAN);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(true, BooleanType.BOOLEAN)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(timestampVariable as date) = DATE '2024-01-01' - a short TIMESTAMP source, the one
     * (source, target) pair native's FUNCTION_TYPE_CAST dispatch actually supports for timestamp.
     * Must still be pushed down - non-regression check for the isTimestampType() narrowing fix.
     */
    @Test
    public void testCastShortTimestampToDateStillPushedDown()
    {
        ColumnHandle timestampColumn = assignments.get(timestampVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(timestampColumn, timestampVariable.getType())),
                DateType.DATE);
        long dateValue = 19723L;
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(dateValue, DateType.DATE)),
                BOOLEAN);

        Domain domain = Domain.singleValue(DateType.DATE, dateValue);
        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_DATE.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, timestampVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(timestampWithTimeZoneVariable as date) = DATE '2024-01-01' - isTimestampType() also
     * matches TIMESTAMP WITH TIME ZONE, but there is no native filler for it: a short
     * timestamp-with-time-zone value is a packed (millis, tzkey) pair, not epoch micros, and the
     * Java-side buffer sizing writes no precision byte for it while native unconditionally consumes
     * one for any REC_TYPE_TIMESTAMP rec_type. Must fall back to Trino instead of being pushed down.
     */
    @Test
    public void testCastTimestampWithTimeZoneToDateFallsBackToTrino()
    {
        ColumnHandle timestampTzColumn = assignments.get(timestampWithTimeZoneVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(timestampTzColumn, timestampWithTimeZoneVariable.getType())),
                DateType.DATE);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(19723L, DateType.DATE)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, timestampWithTimeZoneVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(longTimestampVariable as date) = DATE '2024-01-01' - TIMESTAMP(9), a long timestamp.
     * Native's predicate_short_timestamp() rejects any precision > 6, so this must fall back to
     * Trino rather than reach a native precision check that throws.
     */
    @Test
    public void testCastLongTimestampToDateFallsBackToTrino()
    {
        ColumnHandle longTimestampColumn = assignments.get(longTimestampVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(longTimestampColumn, longTimestampVariable.getType())),
                DateType.DATE);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(19723L, DateType.DATE)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, longTimestampVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(integerVariable as bigint) = 5 - the coercion Trino emits for
     * {@code integer_col = <bigint literal>}. An integer source can never exceed 2^31, so native's
     * double-space comparison stays exact and this must be pushed down.
     */
    @Test
    public void testCastIntegerToBigintPushedDown()
    {
        ColumnHandle integerColumn = assignments.get(integerVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(integerColumn, integerVariable.getType())),
                BigintType.BIGINT);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(5L, BigintType.BIGINT)),
                BOOLEAN);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_CAST,
                Domain.singleValue(BigintType.BIGINT, 5L),
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_BIGINT.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, integerVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as bigint) > 9007199254740995 - native compares in double space, so
     * a bigint literal past 2^53 and the rounded record value can collapse onto the same double and
     * a strict bound would then drop records Trino keeps. Must fall back to Trino.
     */
    @Test
    public void testCastDoubleToBigintFallsBackToTrino()
    {
        ColumnHandle doubleColumn = assignments.get(doubleVariable1.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(doubleColumn, doubleVariable1.getType())),
                BigintType.BIGINT);
        WarpCall warpExpression = new WarpCall(
                GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(9007199254740995L, BigintType.BIGINT)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(bigintVariable as real) = 5.0 - a bigint source with a real target. Native rounds
     * once (int64 -&gt; float, see buf2value_signed_int_as_cast_source in predicate_match_internal.h)
     * rather than going through a double, so it agrees with Trino on every value and must be pushed
     * down.
     */
    @Test
    public void testCastBigintToRealPushedDown()
    {
        ColumnHandle bigintColumn = assignments.get(bigintVariable.getName());
        WarpCall expectedCastCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(bigintColumn, bigintVariable.getType())),
                RealType.REAL);
        long realValue = floatToIntBits(5f);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new WarpPrimitiveConstant(realValue, RealType.REAL)),
                BOOLEAN);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_CAST,
                Domain.singleValue(RealType.REAL, realValue),
                false,
                true,
                List.of(RecTypeCode.REC_TYPE_REAL.ordinal()),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, bigintVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * where day(cast(timestampVariable as date)) = 15 - FunctionsWithCastRewriter sets
     * FUNCTION_TYPE_DAY and then delegates the CAST to VariableRewriter.cast(). Native has no filler
     * that applies day() on top of a CAST, and overwriting the function type with FUNCTION_TYPE_CAST
     * would match this as cast(timestampVariable as date) = 15, comparing epoch days against 15.
     * Must fall back to Trino.
     */
    @Test
    public void testDayOfCastTimestampToDateFallsBackToTrino()
    {
        ColumnHandle timestampColumn = assignments.get(timestampVariable.getName());
        WarpCall castCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(timestampColumn, timestampVariable.getType())),
                DateType.DATE);
        WarpCall dayCall = new WarpCall(SupportedFunctions.DAY.getName(), List.of(castCall), IntegerType.INTEGER);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(dayCall, new WarpPrimitiveConstant(15L, IntegerType.INTEGER)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, timestampVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where day(cast(varcharVariable as date)) = 15 - the one CAST shape that legitimately carries an
     * enclosing date function: the varchar-to-date branch transforms the column instead of the
     * predicate, and native does support day() over a DATE-transformed column. Non-regression check
     * that the enclosing FUNCTION_TYPE_DAY survives.
     */
    @Test
    public void testDayOfCastVarcharToDateStillPushedDown()
    {
        ColumnHandle varcharColumn = assignments.get(varcharVariable.getName());
        WarpCall castCall = new WarpCall(
                CAST_FUNCTION_NAME.getName(),
                List.of(new WarpVariable(varcharColumn, varcharType)),
                DateType.DATE);
        WarpCall dayCall = new WarpCall(SupportedFunctions.DAY.getName(), List.of(castCall), IntegerType.INTEGER);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(dayCall, new WarpPrimitiveConstant(15L, IntegerType.INTEGER)),
                BOOLEAN);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_DAY,
                Domain.singleValue(IntegerType.INTEGER, 15L),
                false,
                true,
                List.of(),
                TransformFunction.DATE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, varcharType, Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * hour(timestamp_col) = 13 pushes down as FUNCTION_TYPE_HOUR over the raw timestamp column.
     */
    @Test
    public void testHourOfTimestampPushedDown()
    {
        WarpCall hourCall = new WarpCall(SupportedFunctions.HOUR.getName(), List.of(createExpectedVariable(timestampVariable)), BigintType.BIGINT);
        WarpCall warpExpression = new WarpCall(
                EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(hourCall, new WarpPrimitiveConstant(13L, BigintType.BIGINT)),
                BOOLEAN);

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_HOUR,
                Domain.singleValue(BigintType.BIGINT, 13L),
                false,
                true,
                List.of(),
                TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, timestampVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * cast(real1 as varchar) = 'Infinity' matches the one infinite real
     */
    @Test
    public void testCastRealInfinityToVarcharPushed()
    {
        WarpCall warpExpression = castToVarcharEquals(realVariable, Slices.utf8Slice("Infinity"));

        NativeExpression expectedResult = new NativeExpression(
                PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.singleValue(RealType.REAL, (long) floatToIntBits(Float.POSITIVE_INFINITY)),
                false,
                false,
                Collections.emptyList(),
                TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(warpExpression, realVariable.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    private WarpVariable createExpectedVariable(Variable variable)
    {
        return new WarpVariable(assignments.get(variable.getName()), variable.getType());
    }

    private void assertPushdownStatsSum(int expectedCount)
    {
        PushdownPredicatesStats pushdownPredicatesStats = (PushdownPredicatesStats) metricsManager.get(PushdownPredicatesStats.createKey());
        assertThat(pushdownPredicatesStats.getCounters().values().stream().mapToLong(LongAdder::longValue).sum()).isEqualTo(expectedCount);
    }
}
