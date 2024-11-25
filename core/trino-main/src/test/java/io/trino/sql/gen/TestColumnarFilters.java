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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.TestingSourcePage;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.FunctionBundle;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.planner.Symbol;
import io.trino.testing.TestingSession;
import io.trino.type.FunctionType;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.Stream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnarFilters
{
    private static final Random RANDOM = new Random(5376453765L);
    private static final long CONSTANT = 64992484L;
    private static final int ROW_NUM_CHANNEL = 0;
    private static final int DOUBLE_CHANNEL = 1;
    private static final int INT_CHANNEL_B = 2;
    private static final int STRING_CHANNEL = 3;
    private static final int INT_CHANNEL_A = 4;
    private static final int INT_CHANNEL_C = 5;
    private static final int ARRAY_CHANNEL = 6;
    private static final int REAL_CHANNEL = 7;

    private static final String COL_ROW_NUM = "$col_" + ROW_NUM_CHANNEL;
    private static final String COL_DOUBLE = "$col_" + DOUBLE_CHANNEL;
    private static final String COL_INT_B = "$col_" + INT_CHANNEL_B;
    private static final String COL_STRING = "$col_" + STRING_CHANNEL;
    private static final String COL_INT_A = "$col_" + INT_CHANNEL_A;
    private static final String COL_INT_C = "$col_" + INT_CHANNEL_C;
    private static final String COL_ARRAY = "$col_" + ARRAY_CHANNEL;
    private static final String COL_REAL = "$col_" + REAL_CHANNEL;

    private static final Type ARRAY_CHANNEL_TYPE = new ArrayType(INTEGER);
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.<Symbol, Integer>builder()
            .put(new Symbol(BIGINT, COL_ROW_NUM), ROW_NUM_CHANNEL)
            .put(new Symbol(DOUBLE, COL_DOUBLE), DOUBLE_CHANNEL)
            .put(new Symbol(INTEGER, COL_INT_B), INT_CHANNEL_B)
            .put(new Symbol(VARCHAR, COL_STRING), STRING_CHANNEL)
            .put(new Symbol(INTEGER, COL_INT_A), INT_CHANNEL_A)
            .put(new Symbol(INTEGER, COL_INT_C), INT_CHANNEL_C)
            .put(new Symbol(ARRAY_CHANNEL_TYPE, COL_ARRAY), ARRAY_CHANNEL)
            .put(new Symbol(REAL, COL_REAL), REAL_CHANNEL)
            .buildOrThrow();
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));
    private static final FunctionBundle FUNCTION_BUNDLE = InternalFunctionBundle.builder()
            .scalar(NullableReturnFunction.class)
            .scalar(ConnectorSessionFunction.class)
            .scalar(InstanceFactoryFunction.class)
            .scalar(CustomIsDistinctFrom.class)
            .scalar(NonDeterministicMultiArgFunction.class)
            .build();
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution(FUNCTION_BUNDLE);
    private static final ColumnarFilterCompiler COMPILER = FUNCTION_RESOLUTION.getColumnarFilterCompiler();
    private static final PageFunctionCompiler PAGE_FUNCTION_COMPILER = FUNCTION_RESOLUTION.getPageFunctionCompiler();

    @Test
    public void testIsDistinctFrom()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // col IS DISTINCT FROM constant
        Expression isDistinctFromFilter = createNotExpression(new Comparison(
                IDENTICAL,
                new Constant(INTEGER, CONSTANT), new Reference(INTEGER, COL_INT_A)));
        assertThatColumnarFilterEvaluationIsSupported(isDistinctFromFilter);
        verifyFilter(inputPages, isDistinctFromFilter);

        // colA IS DISTINCT FROM colB
        isDistinctFromFilter = createNotExpression(new Comparison(
                IDENTICAL,
                new Reference(INTEGER, COL_INT_B), new Reference(INTEGER, COL_INT_A)));
        assertThatColumnarFilterEvaluationIsSupported(isDistinctFromFilter);
        verifyFilter(inputPages, isDistinctFromFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIsNull(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // colA IS NULL
        Expression isNullFilter = new IsNull(new Reference(INTEGER, COL_INT_A));
        assertThatColumnarFilterEvaluationIsSupported(isNullFilter);
        verifyFilter(inputPages, isNullFilter);

        // colA + colB IS NULL
        isNullFilter = new IsNull(call(
                FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(INTEGER, INTEGER)),
                new Reference(INTEGER, COL_INT_A),
                new Reference(INTEGER, COL_INT_B)));
        assertThatColumnarFilterEvaluationIsSupported(isNullFilter);
        verifyFilter(inputPages, isNullFilter);
    }

    @Test
    public void testConstantIsNull()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // constant IS NULL
        Expression isNullFilter = new IsNull(new Constant(INTEGER, CONSTANT));
        assertThatColumnarFilterEvaluationIsSupported(isNullFilter);
        verifyFilter(inputPages, isNullFilter);

        // null IS NULL
        isNullFilter = new IsNull(constantNull(INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(isNullFilter);
        verifyFilter(inputPages, isNullFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testNullableReturnFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // custom_is_null(col, NULL)
        Expression customNullableReturnFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("custom_is_null")
                        .addArgument(VARCHAR, new Reference(VARCHAR, "symbol"))
                        .build()
                        .function(),
                new Reference(VARCHAR, COL_STRING));
        assertThatColumnarFilterEvaluationIsSupported(customNullableReturnFilter);
        verifyFilter(inputPages, customNullableReturnFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testConnectorSessionFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // is_user_admin(connectorSession)
        Expression customConnectorSessionFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("is_user_admin")
                        .build()
                        .function());
        assertThatColumnarFilterEvaluationIsSupported(customConnectorSessionFilter);
        verifyFilter(inputPages, customConnectorSessionFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testInstanceFactoryFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // is_answer_to_universe(col)
        Expression customInstanceFactoryFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("is_answer_to_universe")
                        .addArgument(INTEGER, new Reference(INTEGER, "symbol"))
                        .build()
                        .function(),
                new Reference(INTEGER, COL_INT_A));
        assertThatColumnarFilterEvaluationIsSupported(customInstanceFactoryFilter);
        verifyFilter(inputPages, customInstanceFactoryFilter);
    }

    @Test
    public void testIsNullSubExpressionProducingRleNullBlock()
    {
        // Bare GeneratedPageProjection (defeats both wrap conditions in
        // ColumnarFilterEvaluatorWithProjectedArguments) emits an RLE-of-null block when
        // every projected position is null. The inner createDictionaryAwareEvaluator must
        // unwrap it before IsNullColumnarFilter / IsNotNullColumnarFilter casts to ValueBlock.
        List<Page> inputPages = ImmutableList.<Page>builder()
                .addAll(createInputPages(NullsProvider.RANDOM_NULLS, true))
                .addAll(createInputPages(NullsProvider.ALL_NULLS, false))
                .build();

        Expression isNull = new IsNull(call(
                FUNCTION_RESOLUTION.functionCallBuilder("non_deterministic_multi_arg")
                        .addArgument(INTEGER, new Reference(INTEGER, "a"))
                        .addArgument(INTEGER, new Reference(INTEGER, "b"))
                        .build()
                        .function(),
                new Reference(INTEGER, COL_INT_A),
                new Reference(INTEGER, COL_INT_B)));
        assertThatColumnarFilterEvaluationIsSupported(isNull);
        verifyFilter(inputPages, isNull);

        Expression isNotNull = createNotExpression(isNull);
        assertThatColumnarFilterEvaluationIsSupported(isNotNull);
        verifyFilter(inputPages, isNotNull);
    }

    @Test
    public void testBooleanConstant()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // WHERE true
        Expression trueFilter = new Constant(BOOLEAN, true);
        assertThatColumnarFilterEvaluationIsSupported(trueFilter);
        verifyFilter(inputPages, trueFilter);

        // WHERE false
        Expression falseFilter = new Constant(BOOLEAN, false);
        assertThatColumnarFilterEvaluationIsSupported(falseFilter);
        verifyFilter(inputPages, falseFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIsNotNull(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        Expression isNotNullFilter = createNotExpression(new IsNull(new Reference(INTEGER, COL_INT_A)));
        assertThatColumnarFilterEvaluationIsSupported(isNotNullFilter);
        verifyFilter(inputPages, isNotNullFilter);

        // colA + colB IS NOT NULL
        isNotNullFilter = createNotExpression(new IsNull(call(
                FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(INTEGER, INTEGER)),
                new Reference(INTEGER, COL_INT_A),
                new Reference(INTEGER, COL_INT_B))));
        assertThatColumnarFilterEvaluationIsSupported(isNotNullFilter);
        verifyFilter(inputPages, isNotNullFilter);
    }

    @Test
    public void testConstantIsNotNull()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // constant IS NOT NULL
        Expression isNotNullFilter = createNotExpression(new IsNull(new Constant(INTEGER, CONSTANT)));
        assertThatColumnarFilterEvaluationIsSupported(isNotNullFilter);
        verifyFilter(inputPages, isNotNullFilter);

        // null IS NOT NULL
        isNotNullFilter = createNotExpression(new IsNull(constantNull(INTEGER)));
        assertThatColumnarFilterEvaluationIsSupported(isNotNullFilter);
        verifyFilter(inputPages, isNotNullFilter);
    }

    @Test
    public void testNotEqual()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // NOT (constant = col)
        Expression notEqualFilter = createNotExpression(new Comparison(
                EQUAL,
                new Constant(INTEGER, CONSTANT), new Reference(INTEGER, COL_INT_A)));
        assertThatColumnarFilterEvaluationIsSupported(notEqualFilter);
        verifyFilter(inputPages, notEqualFilter);

        // NOT (constant = col + 1)
        notEqualFilter = createNotExpression(new Comparison(
                EQUAL,
                new Constant(INTEGER, CONSTANT),
                call(
                        FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(INTEGER, INTEGER)),
                        new Reference(INTEGER, COL_INT_A),
                        new Constant(INTEGER, 1L))));
        assertThatColumnarFilterEvaluationIsSupported(notEqualFilter);
        verifyFilter(inputPages, notEqualFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testLike(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        Expression likeFilter = call(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                new Reference(VARCHAR, COL_STRING), new Constant(LIKE_PATTERN, LikePattern.compile(Long.toString(CONSTANT), Optional.empty())));
        assertThatColumnarFilterEvaluationIsSupported(likeFilter);
        verifyFilter(inputPages, likeFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testComparison(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        for (Comparison.Operator operator : List.of(
                EQUAL,
                LESS_THAN,
                LESS_THAN_OR_EQUAL,
                GREATER_THAN,
                GREATER_THAN_OR_EQUAL,
                IDENTICAL)) {
            // constant OP col
            Expression filter = new Comparison(operator, new Constant(INTEGER, CONSTANT), new Reference(INTEGER, COL_INT_A));
            assertThatColumnarFilterEvaluationIsSupported(filter);
            verifyFilter(inputPages, filter);

            // col OP constant
            filter = new Comparison(operator, new Reference(DOUBLE, COL_DOUBLE), new Constant(DOUBLE, (double) CONSTANT));
            assertThatColumnarFilterEvaluationIsSupported(filter);
            verifyFilter(inputPages, filter);

            // colA OP colB
            filter = new Comparison(operator, new Reference(INTEGER, COL_INT_C), new Reference(INTEGER, COL_INT_A));
            assertThatColumnarFilterEvaluationIsSupported(filter);
            verifyFilter(inputPages, filter);

            // colA + 1 OP colB - 1 — sub-expressions on both sides
            filter = new Comparison(
                    operator,
                    call(
                            FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(INTEGER, INTEGER)),
                            new Reference(INTEGER, COL_INT_C),
                            new Constant(INTEGER, 1L)),
                    call(
                            FUNCTION_RESOLUTION.resolveOperator(SUBTRACT, ImmutableList.of(INTEGER, INTEGER)),
                            new Reference(INTEGER, COL_INT_A),
                            new Constant(INTEGER, 1L)));
            assertThatColumnarFilterEvaluationIsSupported(filter);
            verifyFilter(inputPages, filter);
        }

        // IDENTICAL against NULL — only IDENTICAL meaningfully compares against NULL
        Expression identicalNullFilter = new Comparison(IDENTICAL, constantNull(INTEGER), new Reference(INTEGER, COL_INT_A));
        assertThatColumnarFilterEvaluationIsNotSupported(identicalNullFilter);
        verifyFilter(inputPages, identicalNullFilter);

        // coalesce(colC, 0) = colA
        Expression eqFilter = new Comparison(
                EQUAL,
                new Coalesce(new Reference(INTEGER, COL_INT_C), new Constant(INTEGER, 0L)),
                new Reference(INTEGER, COL_INT_A));
        assertThatColumnarFilterEvaluationIsSupported(eqFilter);
        verifyFilter(inputPages, eqFilter);

        // cast(colA AS VARCHAR) = trim(col_string)
        eqFilter = new Comparison(
                EQUAL,
                call(
                        FUNCTION_RESOLUTION.getCoercion(INTEGER, VARCHAR),
                        new Reference(INTEGER, COL_INT_A)),
                call(
                        FUNCTION_RESOLUTION.functionCallBuilder("trim")
                                .addArgument(VARCHAR, new Reference(VARCHAR, "symbol"))
                                .build()
                                .function(),
                        new Reference(VARCHAR, COL_STRING)));
        assertThatColumnarFilterEvaluationIsSupported(eqFilter);
        verifyFilter(inputPages, eqFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testBetween(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // col BETWEEN constantA AND constantB
        Expression betweenFilter = new Between(
                new Reference(INTEGER, COL_INT_A),
                new Constant(INTEGER, CONSTANT - 5),
                new Constant(INTEGER, CONSTANT + 5));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);

        // colA BETWEEN colB AND constant
        betweenFilter = new Between(
                new Reference(INTEGER, COL_INT_A),
                new Reference(INTEGER, COL_INT_B),
                new Constant(INTEGER, CONSTANT + 5));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);

        // colA BETWEEN colB AND colC
        betweenFilter = new Between(
                new Reference(INTEGER, COL_INT_A),
                new Reference(INTEGER, COL_INT_B),
                new Reference(INTEGER, COL_INT_C));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);

        // colA - colB BETWEEN constantA AND constantB
        betweenFilter = new Between(
                call(
                        FUNCTION_RESOLUTION.resolveOperator(SUBTRACT, ImmutableList.of(INTEGER, INTEGER)),
                        new Reference(INTEGER, COL_INT_A),
                        new Reference(INTEGER, COL_INT_B)),
                new Constant(INTEGER, -5L),
                new Constant(INTEGER, 5L));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testOr(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        ResolvedFunction customIsDistinctFrom = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        Expression orFilter = new Logical(
                Logical.Operator.OR,
                ImmutableList.of(
                        call(customIsDistinctFrom, new Reference(INTEGER, COL_INT_A), new Constant(INTEGER, CONSTANT - 5)),
                        call(customIsDistinctFrom, new Reference(INTEGER, COL_INT_C), new Constant(INTEGER, CONSTANT + 5)),
                        call(customIsDistinctFrom, new Reference(INTEGER, COL_INT_B), new Constant(INTEGER, CONSTANT))));
        assertThatColumnarFilterEvaluationIsSupported(orFilter);
        verifyFilter(inputPages, orFilter);

        // colA - 5 < colC OR colA + 5 > colB
        orFilter = new Logical(
                Logical.Operator.OR,
                ImmutableList.of(
                        new Comparison(
                                LESS_THAN,
                                call(
                                        FUNCTION_RESOLUTION.resolveOperator(SUBTRACT, ImmutableList.of(INTEGER, INTEGER)),
                                        new Reference(INTEGER, COL_INT_A),
                                        new Constant(INTEGER, 5L)),
                                new Reference(INTEGER, COL_INT_C)),
                        new Comparison(
                                LESS_THAN,
                                new Reference(INTEGER, COL_INT_B),
                                call(
                                        FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(INTEGER, INTEGER)),
                                        new Reference(INTEGER, COL_INT_A),
                                        new Constant(INTEGER, 5L)))));
        assertThatColumnarFilterEvaluationIsSupported(orFilter);
        verifyFilter(inputPages, orFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testAnd(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        ResolvedFunction customIsDistinctFromIntegers = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        ResolvedFunction customIsDistinctFromVarchars = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(VARCHAR, new Reference(VARCHAR, "left"))
                .addArgument(VARCHAR, new Reference(VARCHAR, "right"))
                .build()
                .function();
        Expression andFilter = new Logical(
                Logical.Operator.AND,
                ImmutableList.of(
                        call(customIsDistinctFromIntegers, new Reference(INTEGER, COL_INT_A), new Constant(INTEGER, CONSTANT - 5)),
                        call(customIsDistinctFromVarchars, new Reference(VARCHAR, COL_STRING), new Constant(VARCHAR, Slices.utf8Slice(Long.toString(CONSTANT + 5)))),
                        call(customIsDistinctFromIntegers, new Reference(INTEGER, COL_INT_B), new Constant(INTEGER, CONSTANT))));
        assertThatColumnarFilterEvaluationIsSupported(andFilter);
        verifyFilter(inputPages, andFilter);
    }

    @Test
    public void testAndLazyColumnLoading()
    {
        // AND(false_constant, col_b = constant) — col_b should never be loaded
        // because the first conjunct eliminates all rows
        String colA = "$col_0";
        String colB = "$col_1";
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, colA), 0,
                new Symbol(BIGINT, colB), 1);

        Expression andFilter = new Logical(Logical.Operator.AND, ImmutableList.of(
                new Constant(BOOLEAN, false),
                new Comparison(EQUAL,
                        new Reference(BIGINT, colB), new Constant(BIGINT, CONSTANT))));

        TestingSourcePage testingPage = new TestingSourcePage(100,
                createLongSequenceBlock(0, 100),
                createLongSequenceBlock(0, 100));
        FilterEvaluator filterEvaluator = createColumnarFilterEvaluator(true, false, true, andFilter, layout, COMPILER, PAGE_FUNCTION_COMPILER, Optional.empty()).orElseThrow().get();
        filterEvaluator.evaluate(FULL_CONNECTOR_SESSION, SelectedPositions.positionsRange(0, 100), testingPage);

        // col_b (channel 1) should not have been loaded because the first conjunct returned no positions
        assertThat(testingPage.wasLoaded(1)).isFalse();
    }

    @Test
    public void testOrLazyColumnLoading()
    {
        // OR(col_a = col_a, col_b = constant) — col_b should never be loaded
        // because the first conjunct selects all rows (every non-null value equals itself)
        String colA = "$col_0";
        String colB = "$col_1";
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, colA), 0,
                new Symbol(BIGINT, colB), 1);

        Expression orFilter = new Logical(Logical.Operator.OR, ImmutableList.of(
                new Comparison(EQUAL,
                        new Reference(BIGINT, colA), new Reference(BIGINT, colA)),
                new Comparison(EQUAL,
                        new Reference(BIGINT, colB), new Constant(BIGINT, CONSTANT))));

        TestingSourcePage testingPage = new TestingSourcePage(100,
                createLongSequenceBlock(0, 100),
                createLongSequenceBlock(0, 100));
        FilterEvaluator filterEvaluator = createColumnarFilterEvaluator(true, false, true, orFilter, layout, COMPILER, PAGE_FUNCTION_COMPILER, Optional.empty()).orElseThrow().get();
        filterEvaluator.evaluate(FULL_CONNECTOR_SESSION, SelectedPositions.positionsRange(0, 100), testingPage);

        // col_b (channel 1) should not have been loaded because the first conjunct selected all rows
        assertThat(testingPage.wasLoaded(1)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIn(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // INTEGER type with small number of discontinuous constants
        // Uses switch case
        List<Expression> valueList = ImmutableList.of(
                constantNull(INTEGER),
                new Constant(INTEGER, CONSTANT + 1),
                new Constant(INTEGER, CONSTANT + 5),
                new Constant(INTEGER, CONSTANT + 10));
        Expression inFilter = new In(new Reference(INTEGER, COL_INT_A), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with large number of discontinuous constants
        // Uses LongBitSetFilter
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(INTEGER))
                .add(new Constant(INTEGER, CONSTANT - 10))
                .addAll(buildConstantsList(INTEGER, 100))
                .add(new Constant(INTEGER, CONSTANT + 110))
                .build();
        inFilter = new In(new Reference(INTEGER, COL_INT_A), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with large number of discontinuous constants from a wide range
        // Uses LongOpenHashSet
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(INTEGER))
                .add(new Constant(INTEGER, CONSTANT - 10))
                .addAll(buildConstantsList(INTEGER, 100))
                .add(new Constant(INTEGER, CONSTANT + 1073741824))
                .build();
        inFilter = new In(new Reference(INTEGER, COL_INT_A), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with continuous constants
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(INTEGER))
                .addAll(buildConstantsList(INTEGER, 100))
                .build();
        inFilter = new In(new Reference(INTEGER, COL_INT_A), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with only null constant
        valueList = ImmutableList.of(constantNull(INTEGER));
        inFilter = new In(new Reference(INTEGER, COL_INT_A), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // REAL type with large number of discontinuous constants
        // Uses LongOpenCustomHashSet
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(REAL))
                .add(new Constant(REAL, CONSTANT - 10))
                .addAll(buildConstantsList(REAL, 100))
                .add(new Constant(REAL, CONSTANT + 110))
                .build();
        inFilter = new In(new Reference(REAL, COL_REAL), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // VARCHAR type with small number of constants
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(VARCHAR))
                .addAll(buildConstantsList(VARCHAR, 3))
                .build();
        inFilter = new In(new Reference(VARCHAR, COL_STRING), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // VARCHAR type with large number of constants
        valueList = ImmutableList.<Expression>builder()
                .add(constantNull(VARCHAR))
                .addAll(buildConstantsList(VARCHAR, 100))
                .build();
        inFilter = new In(new Reference(VARCHAR, COL_STRING), valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // substr(col_string, 1, 5) IN (...)
        Expression substrValue = call(
                FUNCTION_RESOLUTION.resolveFunction("substr", fromTypes(VARCHAR, BIGINT, BIGINT)),
                new Reference(VARCHAR, COL_STRING),
                new Constant(BIGINT, 1L),
                new Constant(BIGINT, 5L));
        valueList = ImmutableList.of(
                constantNull(VARCHAR),
                new Constant(VARCHAR, Slices.utf8Slice("64990")),
                new Constant(VARCHAR, Slices.utf8Slice("64991")),
                new Constant(VARCHAR, Slices.utf8Slice("64992")),
                new Constant(VARCHAR, Slices.utf8Slice("64993")),
                new Constant(VARCHAR, Slices.utf8Slice("64994")));
        inFilter = new In(substrValue, valueList);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testInStructuralType(NullsProvider nullsProvider)
    {
        List<Page> inputPages = createInputPages(nullsProvider, false);
        // Structural type with indeterminate constants and small list
        List<Expression> valueList = ImmutableList.of(
                constantNull(ARRAY_CHANNEL_TYPE),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray()),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, null)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT + 2)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, CONSTANT + 1)));
        Expression inFilter = new In(new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY), valueList);
        // Structural types in "IN" clause are not supported for columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // Structural type with indeterminate constants and large list
        valueList = ImmutableList.of(
                constantNull(ARRAY_CHANNEL_TYPE),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray()),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, null)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT + 2)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, CONSTANT + 1)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, CONSTANT + 1, CONSTANT + 2)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT + 2, null)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT - 2, CONSTANT, CONSTANT - 1)),
                new Constant(ARRAY_CHANNEL_TYPE, createIntArray(CONSTANT, CONSTANT + 1)));
        inFilter = new In(new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY), valueList);
        // Structural types in "IN" clause are not supported for columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(inFilter);
        verifyFilter(inputPages, inFilter);
    }

    @Test
    public void testLambda()
    {
        // filter(col, x -> constant < x)
        ResolvedFunction arrayFilterFunction = FUNCTION_RESOLUTION.resolveFunction(
                "filter",
                fromTypes(ARRAY_CHANNEL_TYPE, new FunctionType(ImmutableList.of(INTEGER), BOOLEAN)));
        Expression lambdaExpression = call(
                arrayFilterFunction,
                new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY),
                new Lambda(
                        ImmutableList.of(new Symbol(INTEGER, "x")),
                        new Comparison(LESS_THAN, new Constant(INTEGER, (long) CONSTANT), new Reference(INTEGER, "x"))));
        assertThatColumnarFilterEvaluationIsNotSupported(lambdaExpression);
    }

    @Test
    public void testFilterWithoutInputChannels()
    {
        // rand() < constant
        Expression filter = new Comparison(
                LESS_THAN,
                call(FUNCTION_RESOLUTION.functionCallBuilder("rand").build().function()),
                new Constant(DOUBLE, (double) CONSTANT));
        assertThatColumnarFilterEvaluationIsSupported(filter);

        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        verifyFilter(inputPages, filter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testStructFilter(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // 0 < cardinality(col_array)
        ResolvedFunction cardinality = FUNCTION_RESOLUTION.functionCallBuilder("cardinality")
                .addArgument(ARRAY_CHANNEL_TYPE, new Reference(ARRAY_CHANNEL_TYPE, "symbol"))
                .build()
                .function();
        Expression cardinalityFilter = new Comparison(
                LESS_THAN,
                new Constant(BIGINT, 0L),
                call(cardinality, new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY)));
        assertThatColumnarFilterEvaluationIsSupported(cardinalityFilter);
        verifyFilter(inputPages, cardinalityFilter);

        // constant < col_array[1]
        Expression subscriptFilter = new Comparison(
                LESS_THAN,
                new Constant(INTEGER, CONSTANT),
                call(
                        FUNCTION_RESOLUTION.resolveOperator(SUBSCRIPT, ImmutableList.of(ARRAY_CHANNEL_TYPE, BIGINT)),
                        new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY),
                        new Constant(BIGINT, 1L)));
        assertThatColumnarFilterEvaluationIsSupported(subscriptFilter);

        Expression filter = new Logical(
                Logical.Operator.AND,
                ImmutableList.of(cardinalityFilter, subscriptFilter));
        assertThatColumnarFilterEvaluationIsSupported(filter);
        verifyFilter(inputPages, filter);
    }

    @Test
    public void testComparisonWithProjectedArgument()
    {
        // rand() < constant — exercises sub-expression eval for Comparison nodes (vs the Call path
        // covered by testFilterWithoutInputChannels). In Expression IR, the LESS_THAN operator is
        // a Comparison node, not a Call, so this routes through createComparisonExpressionEvaluator.
        Expression filter = new Comparison(
                LESS_THAN,
                call(FUNCTION_RESOLUTION.functionCallBuilder("rand").build().function()),
                new Constant(DOUBLE, (double) CONSTANT));
        assertThatColumnarFilterEvaluationIsSupported(filter);

        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        verifyFilter(inputPages, filter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testNotEqualWithProjectedArgument(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        // cardinality(col_array) <> constant — exercises NOT_EQUAL Comparison lowering to NOT(EQUAL)
        // followed by sub-expression eval projection of the cardinality call.
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        ResolvedFunction cardinality = FUNCTION_RESOLUTION.functionCallBuilder("cardinality")
                .addArgument(ARRAY_CHANNEL_TYPE, new Reference(ARRAY_CHANNEL_TYPE, "symbol"))
                .build()
                .function();
        Expression filter = new Comparison(
                NOT_EQUAL,
                call(cardinality, new Reference(ARRAY_CHANNEL_TYPE, COL_ARRAY)),
                new Constant(BIGINT, 0L));
        assertThatColumnarFilterEvaluationIsSupported(filter);
        verifyFilter(inputPages, filter);
    }

    public enum NullsProvider
    {
        NO_NULLS {
            @Override
            public Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.empty();
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            public Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.of(new boolean[positionCount]);
            }
        },
        ALL_NULLS {
            @Override
            public Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                Arrays.fill(nulls, true);
                return Optional.of(nulls);
            }
        },
        RANDOM_NULLS {
            @Override
            public Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    nulls[i] = RANDOM.nextBoolean();
                }
                return Optional.of(nulls);
            }
        },
        GROUPED_NULLS {
            @Override
            public Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                int maxGroupSize = 23;
                int position = 0;
                while (position < positionCount) {
                    int remaining = positionCount - position;
                    int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
                    Arrays.fill(nulls, position, position + groupSize, RANDOM.nextBoolean());
                    position += groupSize;
                }
                return Optional.of(nulls);
            }
        };

        public abstract Optional<boolean[]> getNulls(int positionCount);
    }

    private static Object[][] inputProviders()
    {
        return cartesianProduct(nullsProviders(), trueFalse());
    }

    private static Object[][] nullsProviders()
    {
        return Stream.of(NullsProvider.values()).collect(toDataProvider());
    }

    private static Expression createNotExpression(Expression expression)
    {
        return call(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), expression);
    }

    private static List<Page> processFilter(List<Page> inputPages, boolean columnarEvaluationEnabled, boolean filterReorderingEnabled, Expression filter)
    {
        PageProcessor compiledProcessor = FUNCTION_RESOLUTION.getExpressionCompiler().compilePageProcessor(
                        columnarEvaluationEnabled,
                        true,
                        false,
                        filterReorderingEnabled,
                        Optional.of(filter),
                        Optional.empty(),
                        ImmutableList.of(new Reference(BIGINT, COL_ROW_NUM)),
                        LAYOUT,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        ImmutableList.Builder<Page> outputPagesBuilder = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    FULL_CONNECTOR_SESSION,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    SourcePage.create(inputPage));
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputPagesBuilder.add(workProcessor.getResult());
            }
        }
        return outputPagesBuilder.build();
    }

    private static List<Page> createInputPages(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        long rowCount = 0;
        for (int pageCount = 0; pageCount < 20; pageCount++) {
            int positionsCount = RANDOM.nextInt(1024, 8192);
            long finalRowCount = rowCount;
            builder.add(new Page(
                    positionsCount,
                    createRowNumberBlock(finalRowCount, positionsCount),
                    createDoublesBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createStringsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createArraysBlock(positionsCount, nullsProvider),
                    createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded)));
            rowCount += positionsCount;
        }
        return builder.build();
    }

    private static Block createRowNumberBlock(long start, int positionsCount)
    {
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            values[i] = start + i;
        }
        return new LongArrayBlock(positionsCount, Optional.empty(), values);
    }

    private static Block createIntsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            int[] dictionaryValues = new int[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = toIntExact(CONSTANT - 10 + i);
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new IntArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        int[] values = new int[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = toIntExact(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10));
            }
        }
        return new IntArrayBlock(positionsCount, isNull, values);
    }

    private static Block createDoublesBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 200;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            long[] dictionaryValues = new long[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = doubleToLongBits(CONSTANT - 100 + i);
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new LongArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = doubleToLongBits(RANDOM.nextDouble(CONSTANT - 100, CONSTANT + 100));
            }
        }
        return new LongArrayBlock(positionsCount, isNull, values);
    }

    private static Block createStringsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, dictionarySize, dictionarySize * 10);
            for (int i = 0; i < nonNullDictionarySize; i++) {
                builder.writeEntry(Slices.utf8Slice(Long.toString(CONSTANT - 10 + i)));
            }
            if (containsNulls) {
                builder.appendNull();
            }
            return createDictionaryBlock(positionsCount, nullsProvider, builder.build());
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(Slices.utf8Slice(Long.toString(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10))));
            }
        }
        return builder.build();
    }

    private static Block createArraysBlock(int positionsCount, NullsProvider nullsProvider)
    {
        ArrayBlockBuilder builder = new ArrayBlockBuilder(INTEGER, null, positionsCount);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        for (int position = 0; position < positionsCount; position++) {
            if (isNull.isPresent() && isNull.get()[position]) {
                builder.appendNull();
            }
            else {
                builder.buildEntry(elementBuilder -> {
                    int valuesCount = RANDOM.nextInt(4);
                    for (int i = 0; i < valuesCount; i++) {
                        INTEGER.writeInt(elementBuilder, toIntExact(CONSTANT + i));
                    }
                    // Add a NULL value in the array 10% of the time
                    if (RANDOM.nextInt(100) < 10) {
                        elementBuilder.appendNull();
                    }
                });
            }
        }
        return builder.build();
    }

    private static Optional<boolean[]> getDictionaryIsNull(NullsProvider nullsProvider, int dictionarySize)
    {
        Optional<boolean[]> dictionaryIsNull = Optional.empty();
        if (nullsProvider != NullsProvider.NO_NULLS) {
            dictionaryIsNull = Optional.of(new boolean[dictionarySize]);
            if (nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL) {
                dictionaryIsNull.get()[dictionarySize - 1] = true;
            }
        }
        return dictionaryIsNull;
    }

    private static Block createDictionaryBlock(int positionsCount, NullsProvider nullsProvider, Block dictionary)
    {
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
        int dictionarySize = dictionary.getPositionCount();
        int nonNullDictionarySize = dictionarySize - (containsNulls ? 1 : 0);
        int[] ids = new int[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                ids[i] = dictionarySize - 1;
            }
            else {
                ids[i] = RANDOM.nextInt(nonNullDictionarySize);
            }
        }
        return DictionaryBlock.create(positionsCount, dictionary, ids);
    }

    private static List<Expression> buildConstantsList(Type type, int size)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (long i = 0; i < size; i++) {
            if (type == INTEGER) {
                builder.add(new Constant(type, CONSTANT + i));
            }
            else if (type == REAL) {
                builder.add(new Constant(type, CONSTANT + i));
            }
            else if (type == VARCHAR) {
                builder.add(new Constant(type, Slices.utf8Slice(Long.toString(RANDOM.nextLong(CONSTANT + i)))));
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        return builder.build();
    }

    private static Block createIntArray(Long... values)
    {
        IntArrayBlockBuilder builder = new IntArrayBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                INTEGER.writeInt(builder, toIntExact(value));
            }
        }
        return builder.build();
    }

    private static void verifyFilter(List<Page> inputPages, Expression filter)
    {
        // Tests the ColumnarFilter#filterPositionsRange implementation
        verifyFilterInternal(inputPages, filter);

        // Tests the ColumnarFilter#filterPositionsList implementation
        ResolvedFunction customIsDistinctFrom = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        Expression andFilter = new Logical(
                Logical.Operator.AND,
                ImmutableList.of(
                        call(customIsDistinctFrom, new Constant(INTEGER, CONSTANT + 3), new Reference(INTEGER, COL_INT_A)),
                        filter));
        // Adding an IS DISTINCT FROM filter first creates a list of filtered positions as input to
        // the filter implementation being tested while also keeping NULLs as input
        verifyFilterInternal(inputPages, andFilter);
    }

    private static void verifyFilterInternal(List<Page> inputPages, Expression filter)
    {
        List<Page> outputPagesExpected = processFilter(inputPages, false, false, filter);
        // Without filter reordering
        List<Page> outputPagesActual = processFilter(inputPages, true, false, filter);
        assertThat(outputPagesExpected).hasSize(outputPagesActual.size());

        for (int pageCount = 0; pageCount < outputPagesActual.size(); pageCount++) {
            assertPageEquals(ImmutableList.of(BIGINT), outputPagesActual.get(pageCount), outputPagesExpected.get(pageCount));
        }

        // With filter reordering
        outputPagesActual = processFilter(inputPages, true, true, filter);
        assertThat(outputPagesExpected).hasSize(outputPagesActual.size());

        for (int pageCount = 0; pageCount < outputPagesActual.size(); pageCount++) {
            assertPageEquals(ImmutableList.of(BIGINT), outputPagesActual.get(pageCount), outputPagesExpected.get(pageCount));
        }
    }

    private static void assertPageEquals(List<Type> types, Page actual, Page expected)
    {
        assertThat(actual.getChannelCount()).isEqualTo(expected.getChannelCount());
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        assertThat(types).hasSize(actual.getChannelCount());

        for (int channel = 0; channel < types.size(); channel++) {
            assertBlockEquals(types.get(channel), actual.getBlock(channel), expected.getBlock(channel));
        }
    }

    private static void assertThatColumnarFilterEvaluationIsSupported(Expression filterExpression)
    {
        assertThat(createColumnarFilterEvaluator(true, false, true, filterExpression, LAYOUT, COMPILER, PAGE_FUNCTION_COMPILER, Optional.empty())).isPresent();
    }

    private static void assertThatColumnarFilterEvaluationIsNotSupported(Expression filterExpression)
    {
        assertThat(createColumnarFilterEvaluator(true, false, true, filterExpression, LAYOUT, COMPILER, PAGE_FUNCTION_COMPILER, Optional.empty())).isEmpty();
    }

    @ScalarFunction("custom_is_distinct_from")
    public static final class CustomIsDistinctFrom
    {
        private CustomIsDistinctFrom() {}

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFromLong(@SqlNullable @SqlType("T") Long left, @SqlNullable @SqlType("T") Long right)
        {
            if (left == null && right == null) {
                return false;
            }
            if (left == null || right == null) {
                return true;
            }
            return left.equals(right);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFromSlice(@SqlNullable @SqlType("T") Slice left, @SqlNullable @SqlType("T") Slice right)
        {
            if (left == null && right == null) {
                return false;
            }
            if (left == null || right == null) {
                return true;
            }
            return left.equals(right);
        }
    }

    @ScalarFunction("custom_is_null")
    public static final class NullableReturnFunction
    {
        private NullableReturnFunction() {}

        @LiteralParameters("x")
        @SqlType(StandardTypes.BOOLEAN)
        @SqlNullable
        public static Boolean customIsNullVarchar(@SqlNullable @SqlType("varchar(x)") Slice slice)
        {
            return slice == null ? null : false;
        }
    }

    @ScalarFunction("is_user_admin")
    public static final class ConnectorSessionFunction
    {
        private ConnectorSessionFunction() {}

        @LiteralParameters("x")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isUserAdmin(ConnectorSession session)
        {
            return "admin".equals(session.getUser());
        }
    }

    @ScalarFunction(value = "non_deterministic_multi_arg", deterministic = false)
    public static final class NonDeterministicMultiArgFunction
    {
        private NonDeterministicMultiArgFunction() {}

        @SqlNullable
        @SqlType(StandardTypes.INTEGER)
        public static Long nonDeterministicMultiArg(@SqlType(StandardTypes.INTEGER) long a, @SqlType(StandardTypes.INTEGER) long b)
        {
            return a + b;
        }
    }

    @ScalarFunction("is_answer_to_universe")
    public static final class InstanceFactoryFunction
    {
        private final long precomputed;

        public InstanceFactoryFunction()
        {
            this.precomputed = Long.parseLong("42");
        }

        @SqlType(StandardTypes.BOOLEAN)
        public boolean isAnswerToUniverse(@SqlType(StandardTypes.INTEGER) long value)
        {
            return precomputed == value;
        }
    }
}
