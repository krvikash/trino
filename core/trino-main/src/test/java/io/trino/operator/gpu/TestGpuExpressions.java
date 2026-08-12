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
package io.trino.operator.gpu;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuDateTrunc.Field;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.LikePattern;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Math.clamp;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuExpressions
{
    // The nine {true, false, null} x {true, false, null} combinations, column-wise.
    private static final Boolean[] TRUTH_TABLE_LEFT = {true, true, true, false, false, false, null, null, null};
    private static final Boolean[] TRUTH_TABLE_RIGHT = {true, false, null, true, false, null, true, false, null};

    /**
     * Useful test strings, including interesting inputs and patterns for LIKE testing.
     */
    private final List<String> testStrings = ImmutableList.<String>builder()
            .add("test1", "other", "test2", "nothing", "testing", "%test%")
            .add("a", "xyz", "ab", "z", "yz", "abcd", "", "abcdefg", "xabc", "xyxw", "xaxxxbx", "abcdefghij")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab")
            .add("aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")
            .add("aaaabbbbaaaabbbbaaaabbbb")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaabbbbaaaabbbbaaaa", "aaaabbbbaaaabbbbcccc")
            .add("abababababacabababa", "bbbbbbbbxax", "bbbxxxxaz")
            .add("a".repeat(20) + "b".repeat(20) + "a".repeat(20) + "b".repeat(20) + "the quick brown fox jumps over the lazy dog")
            .add("ababaa", "papaya", "papapaya", "papapapaya", "papapapapaya", "papapapapapaya")
            .add("xyza1234567890123456")
            .add("%", "_", "-", "xxxxx_xxxxx")
            .add("__", "a%", "a_", "%a", "%z", "_z", "_%", "_a%", "_ab_", "_a%b_", "_%_%_%_%")
            .add("%a%a%a%a%a%a%", "%a%b%a%b%a%b%", "%aaaa%bbbb%aaaa%bbbb%aaaa%bbbb%")
            .add("%aaaaaaaaaaaaaaaaaaaaaaaaaa%", "%aab%bba%aab%bba%", "%abaca%")
            .add("%bcccccccca%", "%bbxxxxxa%", "%aaaaaaxaaaaaa%", "%abaaa%", "%paya%")
            .add("%a________________", "-%", "-_", "--", "%$_%")
            .add("Łania szła piękną łąką pod Warszawą")
            .add("ワルシャワ近郊の美しい草原を雌鹿が歩いていた。")
            .add("Слава Україні")
            .build();

    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeSmall(NullsProvider nullsProvider)
    {
        testLike(List.of(64), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeMany(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = Stream.generate(() -> 10_000).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeRandomPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 200_00).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    private void testLike(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(positionsCounts, nullsProvider);

        List<Optional<Character>> escapes = List.of(Optional.of('\\'), Optional.of('$'), Optional.empty());
        for (String pattern : testStrings) {
            for (Optional<Character> escape : escapes) {
                Expression expression = createLikeExpression(varcharChannel, pattern, escape);
                assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
            }
        }
    }

    @Test
    public void testBooleanConstant()
    {
        testConstant(new Constant(BOOLEAN, true));
        testConstant(new Constant(BOOLEAN, false));
        testConstant(new Constant(BOOLEAN, null));
    }

    @Test
    public void testTinyintConstant()
    {
        testConstant(new Constant(TINYINT, 42L));
        testConstant(new Constant(TINYINT, -1L));
        testConstant(new Constant(TINYINT, 0L));
        testConstant(new Constant(TINYINT, null));
    }

    @Test
    public void testSmallintConstant()
    {
        testConstant(new Constant(SMALLINT, 1234L));
        testConstant(new Constant(SMALLINT, -5678L));
        testConstant(new Constant(SMALLINT, null));
    }

    @Test
    public void testIntegerConstant()
    {
        testConstant(new Constant(INTEGER, 123456L));
        testConstant(new Constant(INTEGER, -789012L));
        testConstant(new Constant(INTEGER, 0L));
        testConstant(new Constant(INTEGER, null));
    }

    @Test
    public void testBigintConstant()
    {
        testConstant(new Constant(BIGINT, 1234567890123L));
        testConstant(new Constant(BIGINT, -9876543210L));
        testConstant(new Constant(BIGINT, 0L));
        testConstant(new Constant(BIGINT, null));
    }

    @Test
    public void testRealConstant()
    {
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(3.14f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(-2.5f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(0.0f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.POSITIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NEGATIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NaN)));
        testConstant(new Constant(REAL, null));
    }

    @Test
    public void testDoubleConstant()
    {
        testConstant(new Constant(DOUBLE, 3.14159265359));
        testConstant(new Constant(DOUBLE, -2.71828));
        testConstant(new Constant(DOUBLE, 0.0));
        testConstant(new Constant(DOUBLE, Double.POSITIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NEGATIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NaN));
        testConstant(new Constant(DOUBLE, null));
    }

    @Test
    public void testVarcharConstant()
    {
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("hello")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("Łania szła piękną łąką pod Warszawą")));
        testConstant(new Constant(VARCHAR, null));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testReference(NullsProvider nullsProvider)
    {
        Expression expression = new Reference(BIGINT, "ref0");
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000)));
        assertGpuMatchesCpu(inputPages, List.of(BIGINT), expression, Set.of(0));
    }

    @Test
    public void testRealToIntegerCastPrecision()
    {
        // Odd integers in [2^23, 2^24) are exactly representable in float32 but adding 0.5
        // requires 25 significand bits, exceeding float32's 24. IEEE 754 round-to-nearest-even
        // rounds the sum up to value+1 (because the last kept bit is 1), producing an off-by-one
        // after truncation. The GPU must widen to float64 for the rounding arithmetic to match
        // CPU semantics.
        int[] values = {
                (1 << 23) + 1,
                (1 << 24) - 1,
                -((1 << 23) + 1),
                -((1 << 24) - 1),
        };

        Expression expression = new Cast(field(0, REAL), INTEGER);
        Map<Symbol, Integer> layout = layoutFor(List.of(REAL));
        CompiledExpression compiledGpu = compileExpression(expression, layout).orElseThrow();
        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);

        for (int value : values) {
            long realBits = Float.floatToIntBits((float) value);
            Page input = new Page(writeNativeValue(REAL, realBits));
            assertGpuMatchesCpuForCast(List.of(input), List.of(REAL), expression, cpuProcessor, compiledGpu);
        }
    }

    @Test
    public void testNumericOperatorsCorrectnessSmoke()
    {
        List<Type> testedTypes = List.of(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(1, 0),
                createDecimalType(3, 0), // can hold max tinyint
                createDecimalType(5, 0), // can hold max smallint
                createDecimalType(10, 0), // can hold max integer
                createDecimalType(19, 0), // can hold max bigint
                createDecimalType(13, 0),
                createDecimalType(13, 2),
                createDecimalType(27, 0),
                createDecimalType(27, 5),
                createDecimalType(38),
                NUMBER,
                createVarcharType(1),
                createVarcharType(3),
                createVarcharType(5),
                createVarcharType(20),
                VARCHAR);
        List<TrinoNumber> testedNumbers = numericValuesToTest();

        LoadingCache<Type, List<@Nullable Object>> testedTypeValues = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> {
                    List<@Nullable Object> values = tryCastToAndFilter(NUMBER, type, testedNumbers);
                    verify(!values.isEmpty());
                    return values;
                }));

        LoadingCache<Type, List<Page>> unaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> testedTypeValues.getUnchecked(type).stream()
                        .map(value -> new Page(writeNativeValue(type, value)))
                        .collect(toImmutableList())));

        LoadingCache<Pair<Type, Type>, List<Page>> binaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(pair -> cartesianProduct(
                        pair.first(),
                        testedTypeValues.getUnchecked(pair.first()),
                        pair.second(),
                        testedTypeValues.getUnchecked(pair.second()))));

        Set<String> testedOperators = new HashSet<>();
        Set<String> gpuEnabledOperators = new HashSet<>();

        // Unary operators
        for (Type type : testedTypes) {
            for (OperatorType operator : OperatorType.values()) {
                if (operator.getArgumentCount() != 1) {
                    continue;
                }
                if (operator == OperatorType.CAST || operator == OperatorType.SATURATED_FLOOR_CAST) {
                    // Casts tested below in the (fromType, toType) cross-product.
                    continue;
                }

                ResolvedFunction function;
                try {
                    function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(type));
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                // Inspect coercions if any
                Type coerced = getOnlyElement(function.signature().getArgumentTypes());
                if (type.equals(coerced)) {
                    // Let's test, this is what we're here for
                }
                else if (testedTypes.contains(coerced)) {
                    // Will be tested explicitly on separate round
                    continue;
                }
                else {
                    // Might not be covered by explicit tests; expect this only for decimal coercions
                    verify(type instanceof DecimalType,
                            "Unexpected coercion for unary %s on %s: argument types %s",
                            operator,
                            type,
                            function.signature().getArgumentTypes());
                }

                testedOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                Expression expression = buildUnaryOperatorExpression(operator, function, field(0, coerced));
                Map<Symbol, Integer> layout = layoutFor(List.of(coerced));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout);
                if (gpuExpression.isEmpty()) {
                    // Not supported for GPU execution
                    continue;
                }
                gpuEnabledOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(coerced)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpu(List.of(input), List.of(coerced), expression, cpuProcessor, compiledGpu, false);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Cast operators: cross-product over (fromType, toType).
        for (Type fromType : testedTypes) {
            for (Type toType : testedTypes) {
                // CPU's REAL→BIGINT silently saturates for ±Inf and finite OOR (uses (long) cast which clamps),
                // while GPU correctly throws INVALID_CAST_ARGUMENT. Skip until CPU is fixed.
                // TODO: re-enable once CPU REAL→BIGINT throws consistently.
                if (fromType.equals(REAL) && toType.equals(BIGINT)) {
                    continue;
                }
                try {
                    FUNCTION_RESOLUTION.getCoercion(fromType, toType);
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                String label = "CAST %s AS %s".formatted(fromType.getDisplayName(), toType.getDisplayName());
                testedOperators.add(label);

                Expression expression = new Cast(field(0, fromType), toType);
                Map<Symbol, Integer> layout = layoutFor(List.of(fromType));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout);
                if (gpuExpression.isEmpty()) {
                    continue;
                }
                gpuEnabledOperators.add(label);

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(fromType)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpuForCast(List.of(input), List.of(fromType), expression, cpuProcessor, compiledGpu);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Binary operators: cross-product over (leftType, rightType)
        for (Type leftType : testedTypes) {
            for (Type rightType : testedTypes) {
                for (OperatorType operator : OperatorType.values()) {
                    if (operator.getArgumentCount() != 2) {
                        continue;
                    }
                    if (operator == OperatorType.EQUAL || operator == OperatorType.LESS_THAN || operator == OperatorType.LESS_THAN_OR_EQUAL) {
                        continue;
                    }

                    ResolvedFunction function;
                    try {
                        function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(leftType, rightType));
                    }
                    catch (OperatorNotFoundException e) {
                        continue;
                    }

                    switch (operator) {
                        case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO, NEGATION, SUBSCRIPT -> {
                            // Binary operators tested here
                        }
                        case EQUAL, COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST, LESS_THAN, LESS_THAN_OR_EQUAL, IDENTICAL -> {
                            // Covered by testComparison
                            assertThat(Set.copyOf(function.signature().getArgumentTypes()))
                                    .as(
                                            """
                                            Expect comparison operators to require pre-coerced inputs. If this is the case, comparisons have test coverage in testComparison.
                                            Otherwise test coverage needs to be revisited.
                                            """)
                                    .hasSize(1);
                        }
                        case CAST, SATURATED_FLOOR_CAST, HASH_CODE, XX_HASH_64, INDETERMINATE, READ_VALUE -> throw new AssertionError("Unreachable, not a binary operator");
                    }

                    // Inspect coercions if any
                    if (List.of(leftType, rightType).equals(function.signature().getArgumentTypes())) {
                        // Let's test, this is what we're here for
                    }
                    else if (testedTypes.containsAll(function.signature().getArgumentTypes())) {
                        // Will be tested explicitly on separate round
                        continue;
                    }
                    else {
                        // Might not be covered by explicit tests, so let's test this.
                        // This should be the case only for decimal types.
                        verify(leftType instanceof DecimalType || rightType instanceof DecimalType, "Neither is decimal: %s, %s", leftType, rightType);
                    }

                    assertThat(function.signature().getArgumentTypes()).hasSize(2);
                    Type leftCoerced = function.signature().getArgumentTypes().get(0);
                    Type rightCoerced = function.signature().getArgumentTypes().get(1);

                    testedOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    Expression expression = new Call(function, ImmutableList.of(field(0, leftCoerced), field(1, rightCoerced)));
                    Map<Symbol, Integer> layout = layoutFor(List.of(leftCoerced, rightCoerced));
                    Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout);
                    if (gpuExpression.isEmpty()) {
                        // Not supported for GPU execution
                        continue;
                    }
                    gpuEnabledOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    try {
                        CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                        assertThat(compiledGpu.inputChannels().getInputChannels())
                                .containsExactlyInAnyOrderElementsOf(Set.of(0, 1));
                        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                        for (Page input : binaryInputs.getUnchecked(new Pair<>(leftCoerced, rightCoerced))) {
                            assertThat(input.getPositionCount()).isEqualTo(1);
                            assertGpuMatchesCpu(List.of(input), List.of(leftCoerced, rightCoerced), expression, cpuProcessor, compiledGpu, false);
                        }
                    }
                    catch (AssertionError failure) {
                        failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                        throw failure;
                    }
                }
            }
        }

        // Self-test
        assertThat(testedOperators)
                .contains(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint",
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint");

        // Self-test
        assertThat(gpuEnabledOperators)
                .contains(
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(13,0) + decimal(13,0)",
                        "decimal(13,0) + decimal(13,2)",
                        "decimal(3,0) - decimal(3,0)",
                        "decimal(13,0) - decimal(13,0)",
                        "decimal(13,0) - decimal(13,2)",
                        "decimal(3,0) * decimal(3,0)",
                        "decimal(13,0) * decimal(13,0)",
                        "decimal(13,0) * decimal(13,2)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint",
                        "CAST double AS real",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "decimal(27,0) * decimal(3,0)")
                .doesNotContain(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint");
    }

    private static List<@Nullable TrinoNumber> numericValuesToTest()
    {
        List<Long> initial = List.of(
                0L,
                -1L,
                1L,
                (long) Byte.MIN_VALUE,
                (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE,
                (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MAX_VALUE);
        List<BigDecimal> offsets = Stream.of("0", "1", "-1", "0.33", "0.5", "0.66", "-0.33", "-0.5", "-0.66")
                .map(BigDecimal::new)
                .toList();
        List<TrinoNumber> testedNumbers = new ArrayList<>();
        testedNumbers.add(null);
        for (long value : initial) {
            BigDecimal asBigDecimal = BigDecimal.valueOf(value);
            for (BigDecimal offset : offsets) {
                testedNumbers.add(TrinoNumber.from(asBigDecimal.add(offset)));
            }
        }
        testedNumbers.add(TrinoNumber.from(BigDecimal.valueOf(Math.PI)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(false)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(true)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.NotANumber()));
        return testedNumbers;
    }

    private List<@Nullable Object> tryCastToAndFilter(Type sourceType, Type targetType, List<@Nullable ?> sourceNativeValues)
    {
        List<Object> nativeValues = new ArrayList<>();
        Expression castExpression = new Cast(field(0, sourceType), targetType);
        Map<Symbol, Integer> layout = layoutFor(List.of(sourceType));
        PageProcessor pageProcessor = compileCpuExpression(castExpression, layout);
        for (@Nullable Object sourceValue : sourceNativeValues) {
            Page sourcePage = new Page(writeNativeValue(sourceType, sourceValue));
            List<Page> result;
            try {
                result = executeWithCpu(pageProcessor, List.of(sourcePage));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()) || e.getErrorCode().equals(INVALID_CAST_ARGUMENT.toErrorCode())) {
                    continue;
                }
                throw e;
            }
            assertThat(result).hasSize(1);
            Page resultPage = getOnlyElement(result);
            assertThat(resultPage.getPositionCount()).isEqualTo(1);
            assertThat(resultPage.getChannelCount()).isEqualTo(1);
            @Nullable Object targetValue = readNativeValue(targetType, resultPage.getBlock(0), 0);
            nativeValues.add(targetValue);
        }
        return nativeValues;
    }

    private List<Page> cartesianProduct(Type leftType, List<?> leftValues, Type rightType, List<?> rightValues)
    {
        List<Page> pages = new ArrayList<>();
        for (Object leftValue : leftValues) {
            for (Object rightValue : rightValues) {
                pages.add(new Page(
                        writeNativeValue(leftType, leftValue),
                        writeNativeValue(rightType, rightValue)));
            }
        }
        return pages;
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testArithmeticOperators(NullsProvider nullsProvider)
    {
        testArithmetic(OperatorType.ADD, nullsProvider);
        testArithmetic(OperatorType.SUBTRACT, nullsProvider);
        testArithmetic(OperatorType.MULTIPLY, nullsProvider);
        testArithmetic(OperatorType.DIVIDE, nullsProvider);
        testArithmetic(OperatorType.MODULO, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalAdd(NullsProvider nullsProvider)
    {
        testShortDecimalAdd(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalAdd(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalAdd(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalAdd(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalAdd(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalSubtract(NullsProvider nullsProvider)
    {
        testShortDecimalSubtract(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalSubtract(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalSubtract(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalSubtract(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalSubtract(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalMultiply(NullsProvider nullsProvider)
    {
        testShortDecimalMultiply(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalMultiply(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (p1 + p2 > 18)
        testShortDecimalMultiply(createDecimalType(10, 2), createDecimalType(10, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(13, 0), createDecimalType(13, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalMultiply(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalAdd(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalSubtract(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalMultiply(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(20, 2), createDecimalType(18, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(19, 0), createDecimalType(19, 0), nullsProvider);
    }

    private void testDecimalBinaryOp(OperatorType operatorType, DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(leftType, rightType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(leftType, positionsCount, nullsProvider),
                createBlock(rightType, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(leftType, rightType)),
                ImmutableList.of(field(channelA, leftType), field(channelB, rightType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @MethodSource("comparisonTestCases")
    public void testComparison(Type type, ComparisonOperator operator, NullsProvider nullsProvider)
    {
        if (type == NUMBER) {
            // type currently not supported
            assertThat(isConvertible(type)).as("Expected %s to be not supported on GPU", type)
                    .isFalse();
            return;
        }

        List<Type> inputTypes = List.of(type, type);
        int channelA = 0;
        int channelB = 1;
        Expression expression = comparison(operator, field(channelA, type), field(channelB, type));
        Set<Integer> inputChannels = Set.of(channelA, channelB);

        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        if (operator == ComparisonOperator.IDENTICAL && (type == REAL || type == DOUBLE)) {
            // operator currently not supported
            assertThat(compileExpression(expression, layoutFor(inputTypes)))
                    .isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression, inputChannels);
    }

    public static Stream<Object[]> comparisonTestCases()
    {
        return Lists.cartesianProduct(
                        List.of(
                                BOOLEAN,
                                TINYINT,
                                SMALLINT,
                                INTEGER,
                                BIGINT,
                                REAL,
                                DOUBLE,
                                createDecimalType(3, 0),
                                createDecimalType(13, 0),
                                createDecimalType(27, 0),
                                createDecimalType(27, 5),
                                createDecimalType(38),
                                NUMBER,
                                VARCHAR),
                        ImmutableList.copyOf(ComparisonOperator.values()),
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testAnd(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // AND of two comparisons: a < 50 AND b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testOr(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // OR of two comparisons: a < 50 OR b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testAndNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @Test
    public void testOrNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testNot(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NOT of a comparison: NOT(a < 50)
        Expression comparison = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIsNull(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Expression expression = new IsNull(field(channelA, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBetween(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // a BETWEEN 10 AND 50
        Expression expression = between(
                field(channelA, BIGINT),
                new Constant(BIGINT, 10L),
                new Constant(BIGINT, 50L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBetweenNonTrivialValue(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (a + b) BETWEEN 10 AND 50 — the non-trivial value is bound with a Let so it is evaluated once
        Expression expression = between(
                new Call(
                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                        ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT))),
                new Constant(BIGINT, 10L),
                new Constant(BIGINT, 50L));
        assertThat(expression).isInstanceOf(Let.class);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testNestedLet(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // Let s1 = ref0 + ref1 in (Let s2 = s1 + ref2 in (s1 + s2))
        // Body references s1 (de Bruijn index 1) and s2 (index 0); the inner value references s1 (index 0).
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression inner = new Let(
                s2,
                add(s1.toSymbolReference(), field(2, BIGINT)),
                add(s1.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = new Let(s1, add(field(0, BIGINT), field(1, BIGINT)), inner);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1, 2));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSiblingLets(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (Let s1 = ref0 + ref1 in s1 + s1) + (Let s2 = ref0 - ref1 in s2 + s2)
        // Sibling bindings are never live simultaneously, so both reuse the same tail slot (index 0).
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression left = new Let(
                s1,
                add(field(0, BIGINT), field(1, BIGINT)),
                add(s1.toSymbolReference(), s1.toSymbolReference()));
        Expression right = new Let(
                s2,
                subtract(field(0, BIGINT), field(1, BIGINT)),
                add(s2.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = add(left, right);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    private static Expression add(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    private static Expression subtract(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testCoalesce(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Expression expression = new Coalesce(ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIf(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // IF(a < 50, a, b)
        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 50L));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(condition, field(channelA, BIGINT))),
                field(channelB, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testIfLazyEvaluation()
    {
        // IF(b != 0, a / b, a) must not throw division-by-zero on rows where b = 0,
        // because the CPU short-circuits and never evaluates the divide for those rows.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            // Mix zero, negative, and positive divisors so both branches are exercised.
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, aDivB)),
                field(channelA, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIfLazyEvaluationDefeatsInputMasking(NullsProvider nullsProvider)
    {
        // IF(b != 0, 10 / coalesce(b, 0), 42): the true branch reads b, but coalesce
        // resurrects a non-null 0 from a NULL b. CPU short-circuits and never invokes the
        // divide for rows with b = 0 or b IS NULL, so the result is 42 there. The GPU must
        // not evaluate the true branch on those rows either — input-NULL masking would
        // be defeated by coalesce and trigger DIVISION_BY_ZERO.
        int channelB = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        boolean[] nulls = nullsProvider.getNulls(positionsCount).orElse(new boolean[positionsCount]);
        for (int i = 0; i < positionsCount; i++) {
            if (nulls[i]) {
                bBuilder.appendNull();
            }
            else {
                // Mix zero and non-zero divisors so both branches are exercised.
                BIGINT.writeLong(bBuilder, i % 3 == 0 ? 0L : random.nextLong(1, 100));
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression coalesceB = new Coalesce(ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 0L)));
        Expression divide = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(new Constant(BIGINT, 10L), coalesceB));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, divide)),
                new Constant(BIGINT, 42L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testTypeOnlyCast(NullsProvider nullsProvider)
    {
        // VARCHAR(10) -> VARCHAR(20) shares the same cudf STRING dtype, so visitCast should
        // skip emitting GpuCast and forward the inner expression directly.
        int channelA = 0;
        Type sourceType = createVarcharType(10);
        Type targetType = createVarcharType(20);
        List<Type> inputTypes = List.of(sourceType);
        List<Page> inputPages = createVarcharPages(List.of(64), nullsProvider);

        Expression expression = new Cast(field(channelA, sourceType), targetType);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDateTimeExtract(NullsProvider nullsProvider)
    {
        for (String functionName : List.of("day", "hour", "minute", "second")) {
            for (int precision = 0; precision <= 9; precision++) {
                testDateTimeExtract(functionName, createTimestampType(precision), nullsProvider);
            }
        }
        // Trino only defines day() (aka day_of_month) for DATE — hour/minute/second are timestamp-only.
        testDateTimeExtract("day", DATE, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testYearExtract(NullsProvider nullsProvider)
    {
        // cuDF's year() returns INT16, so it can only represent years in the range [-32768, 32767].
        // Random test data spans the full long/int range and would overflow, so generate inputs in a
        // realistic range. The GPU compiler does not statically constrain the input range — callers
        // are responsible for ensuring values fit, which is the case for all production data sets.
        testYearExtract(DATE, nullsProvider);
        for (int precision = 0; precision <= 9; precision++) {
            testYearExtract(createTimestampType(precision), nullsProvider);
        }
    }

    private void testYearExtract(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(type);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createInRangeDateTimeBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testYearExtractOutOfRangeThrows()
    {
        // Year 100000 is outside cuDF's INT16 year range, so GPU must throw. CPU would happily return 100000.
        int channelA = 0;
        Type type = DATE;
        long outOfRangeDays = LocalDate.of(100_000, 6, 15).toEpochDay();
        List<Page> inputPages = List.of(new Page(writeNativeValue(type, outOfRangeDays)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)))
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32768, 32767]");
    }

    private static Block createInRangeDateTimeBlock(Type type, int positionsCount, NullsProvider nullsProvider)
    {
        // Constrain to roughly years [-10000, +10000] so cuDF's INT16 year extraction does not overflow.
        Random random = new Random(42);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else if (type == DATE) {
                // Days since epoch in ±10000 years (~3.65M days).
                type.writeLong(builder, random.nextInt(7_300_001) - 3_650_000);
            }
            else if (type instanceof TimestampType timestampType && timestampType.isShort()) {
                // Trino short TimestampType stores epochMicros; pick microsecond values in ±10000 years.
                long maxMicros = 10_000L * 365L * 24L * 60L * 60L * 1_000_000L;
                long micros = (random.nextLong() % maxMicros);
                // Precision p < 6 requires the last (6-p) decimal digits to be zero.
                long scale = 1L;
                for (int p = timestampType.getPrecision(); p < TimestampType.MAX_SHORT_PRECISION; p++) {
                    scale *= 10;
                }
                type.writeLong(builder, (micros / scale) * scale);
            }
            else if (type instanceof TimestampType timestampType) {
                // Limit to values that can be represented in 64-bit with nanosecond precision, also after e.g. date_trunc(day)
                long epochNanos = clamp(random.nextLong(), Long.MIN_VALUE + NANOSECONDS_PER_DAY, Long.MAX_VALUE);
                // Align to the declared precision (zero the last 9-p decimal digits of nanos).
                epochNanos = Timestamps.round(epochNanos, 9 - timestampType.getPrecision());
                long epochMicros = floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND);
                int picosOfMicro = floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                type.writeObject(builder, new LongTimestamp(epochMicros, picosOfMicro));
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        return builder.build();
    }

    private void testDateTimeExtract(String functionName, Type timestampType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(timestampType)),
                ImmutableList.of(field(channelA, timestampType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDateTrunc(NullsProvider nullsProvider)
    {
        for (String unit : Arrays.stream(Field.values()).map(Field::trinoDateTruncUnit).toList()) {
            for (int precision = 0; precision <= 9; precision++) {
                try {
                    testDateTrunc(unit, createTimestampType(precision), nullsProvider);
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("unit: " + unit));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    private void testDateTrunc(String unit, Type timestampType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        Type unitType = createVarcharType(unit.length());
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_trunc", fromTypes(unitType, timestampType)),
                ImmutableList.of(new Constant(unitType, Slices.utf8Slice(unit)), field(channelA, timestampType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testStringLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(List.of(64), nullsProvider);

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("length", fromTypes(VARCHAR)),
                ImmutableList.of(field(varcharChannel, VARCHAR)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSubstring(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        int startChannel = 1;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR, BIGINT);

        List<Page> inputPages = new ArrayList<>();
        inputPages.add(new Page(
                positionsCount,
                createBlock(VARCHAR, positionsCount, nullsProvider),
                createBigintBlock(positionsCount, nullsProvider, -20, 20)));
        // Edge cases that random data may not cover: start=0 (empty), negative start beyond string length, start past end
        for (long start : List.of(0L, 1L, -1L, -100L, 100L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE)) {
            inputPages.add(new Page(
                    writeNativeValue(VARCHAR, Slices.utf8Slice("hello")),
                    writeNativeValue(BIGINT, start)));
        }

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("substring", fromTypes(VARCHAR, BIGINT)),
                ImmutableList.of(field(varcharChannel, VARCHAR), field(startChannel, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel, startChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSubstringWithLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        int startChannel = 1;
        int lengthChannel = 2;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR, BIGINT, BIGINT);

        List<Page> inputPages = new ArrayList<>();
        inputPages.add(new Page(
                positionsCount,
                createBlock(VARCHAR, positionsCount, nullsProvider),
                createBigintBlock(positionsCount, nullsProvider, -20, 20),
                createBigintBlock(positionsCount, nullsProvider, -5, 20)));
        // Edge cases that random data may not cover: start=0, negative start, length<=0, start/length past end
        for (long start : List.of(0L, 1L, -1L, -100L, 100L)) {
            for (long length : List.of(-1L, 0L, 1L, 100L)) {
                inputPages.add(new Page(
                        writeNativeValue(VARCHAR, Slices.utf8Slice("hello")),
                        writeNativeValue(BIGINT, start),
                        writeNativeValue(BIGINT, length)));
            }
        }

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("substring", fromTypes(VARCHAR, BIGINT, BIGINT)),
                ImmutableList.of(field(varcharChannel, VARCHAR), field(startChannel, BIGINT), field(lengthChannel, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel, startChannel, lengthChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIn(NullsProvider nullsProvider)
    {
        testIn(BIGINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(INTEGER, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(SMALLINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(TINYINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(DOUBLE, List.of(-50.0, 0.0, 25.5, 50.0, 75.5), nullsProvider);
        testIn(REAL, List.of(
                (long) Float.floatToIntBits(-50.0f),
                (long) Float.floatToIntBits(0.0f),
                (long) Float.floatToIntBits(25.5f),
                (long) Float.floatToIntBits(50.0f),
                (long) Float.floatToIntBits(75.5f)), nullsProvider);
        testIn(VARCHAR, List.of(
                Slices.utf8Slice("test1"),
                Slices.utf8Slice("other"),
                Slices.utf8Slice("xyz")), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testInWithNull(NullsProvider nullsProvider)
    {
        testIn(BIGINT, Arrays.asList(10L, null, 50L), nullsProvider);
        testIn(BIGINT, Collections.singletonList(null), nullsProvider);
    }

    private void testIn(Type type, List<Object> inValues, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider)));

        ImmutableList.Builder<Expression> valueListBuilder = ImmutableList.builder();
        for (Object value : inValues) {
            valueListBuilder.add(new Constant(type, value));
        }

        Expression expression = new In(field(channelA, type), valueListBuilder.build());

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    private void testArithmetic(OperatorType operatorType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000),
                createBigintBlock(positionsCount, nullsProvider, 1, 100)));  // non-zero to avoid division by zero

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private void testConstant(Expression constantExpression)
    {
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, 0, 100)));

        CompiledExpression gpuExpression = compileExpression(constantExpression, layoutFor(List.of()))
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + constantExpression));
        assertThat(gpuExpression.inputChannels().getInputChannels()).isEmpty();

        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, constantExpression, gpuExpression);
        List<Page> cpuResults = executeWithCpu(inputPages, List.of(constantExpression), Map.of());
        assertSameDataInOrder(gpuResults, cpuResults, List.of(constantExpression.type()));
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, Expression expression, Set<Integer> expectedInputChannels)
    {
        assertGpuMatchesCpu(inputPages, inputTypes, expression, expectedInputChannels, false);
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, Expression expression, Set<Integer> expectedInputChannels, boolean allowMultipleInputsForExceptionTesting)
    {
        Map<Symbol, Integer> layout = layoutFor(inputTypes);
        PageProcessor pageProcessor = compileCpuExpression(expression, layout);
        CompiledExpression gpuExpression = compileExpression(expression, layout)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));

        assertThat(gpuExpression.inputChannels().getInputChannels())
                .containsExactlyInAnyOrderElementsOf(expectedInputChannels);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, pageProcessor, gpuExpression, allowMultipleInputsForExceptionTesting);
    }

    /**
     * Like {@link #assertGpuMatchesCpu(List, List, Expression, PageProcessor, CompiledExpression, boolean)},
     * but accepts {@code INVALID_CAST_ARGUMENT} from GPU when CPU returns {@code NUMERIC_VALUE_OUT_OF_RANGE}.
     * Used in the cast smoke loop.
     */
    // TODO: remove once CPU is fixed to consistently throw INVALID_CAST_ARGUMENT for ±Inf and finite OOR
    //  in float→int casts; switch the call site to plain assertGpuMatchesCpu.
    private void assertGpuMatchesCpuForCast(List<Page> inputPages, List<Type> inputTypes, Expression expression, PageProcessor cpuProcessor, CompiledExpression gpuExpression)
    {
        List<Page> cpuResults;
        try {
            cpuResults = executeWithCpu(cpuProcessor, inputPages);
        }
        catch (TrinoException cpuExecutionException) {
            assertThat(inputPages.stream().mapToLong(Page::getPositionCount).sum())
                    .describedAs("Cast smoke loop is single-row per page")
                    .isEqualTo(1);
            ErrorCodeSupplier[] acceptable = NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().equals(cpuExecutionException.getErrorCode())
                    ? new ErrorCodeSupplier[] {NUMERIC_VALUE_OUT_OF_RANGE, INVALID_CAST_ARGUMENT}
                    : new ErrorCodeSupplier[] {cpuExecutionException::getErrorCode};
            try {
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                        .hasErrorCode(acceptable);
            }
            catch (AssertionError failure) {
                failure.addSuppressed(new Exception("expression: " + expression));
                failure.addSuppressed(new Exception("inputTypes: " + inputTypes));
                failure.addSuppressed(new Exception("CPU execution exception", cpuExecutionException));
                throw failure;
            }
            return;
        }
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, expression, gpuExpression);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, Expression expression, PageProcessor cpuProcessor, CompiledExpression gpuExpression, boolean allowMultipleInputsForExceptionTesting)
    {
        List<Page> cpuResults;
        try {
            cpuResults = executeWithCpu(cpuProcessor, inputPages);
        }
        catch (TrinoException cpuExecutionException) {
            // The boolean flag is a safety mechanism not to nullify test coverage over large input data set when one of the rows triggers execution exception
            if (!allowMultipleInputsForExceptionTesting) {
                assertThat(inputPages.stream().mapToLong(Page::getPositionCount).sum())
                        .describedAs("When testing exception flows, it is recommended to test with single row inputs. Use the flag to suppress.")
                        .isEqualTo(1);
            }
            try {
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                        .hasErrorCode(cpuExecutionException::getErrorCode);
            }
            catch (AssertionError failure) {
                failure.addSuppressed(new Exception("expression: " + expression));
                failure.addSuppressed(new Exception("inputTypes: " + inputTypes));
                failure.addSuppressed(new Exception("CPU execution exception", cpuExecutionException));
                throw failure;
            }
            return;
        }
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, expression, gpuExpression);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
    }

    private List<Page> executeWithGpu(List<Page> inputPages, List<Type> inputTypes, Expression expression, CompiledExpression gpuExpression)
    {
        return executeGpuOperation(
                inputPages,
                inputTypes,
                List.of(expression.type()),
                (context, copyToDevice) -> new GpuProject(context, copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))));
    }

    private Expression createLikeExpression(int channel, String pattern, Optional<Character> escape)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                ImmutableList.of(
                        field(channel, VARCHAR),
                        new Constant(LIKE_PATTERN, LikePattern.compile(pattern, escape))));
    }

    private List<Page> createVarcharPages(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Iterator<String> strings = generateInputStrings().iterator();
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            builder.writeEntry(Slices.utf8Slice(strings.next()));
                        }
                    }
                    return new Page(positionsCount, builder.build());
                })
                .collect(toImmutableList());
    }

    private Stream<String> generateInputStrings()
    {
        Random random = new Random(42);
        Stream<String> randomStrings = Stream.generate(() -> {
            int length = random.nextInt(21);
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char) random.nextInt(0, Character.MIN_SURROGATE - 1);
            }
            return new String(chars);
        });

        return Streams.zip(
                        Stream.generate(() -> testStrings).flatMap(List::stream),
                        randomStrings,
                        List::of)
                .flatMap(List::stream);
    }

    private static Stream<Integer> randomInts(int minInclusive, int maxExclusive)
    {
        Random random = new Random(42); // Fixed seed for reproducibility
        return IntStream.generate(() -> random.nextInt(minInclusive, maxExclusive))
                .boxed();
    }

    private static Reference field(int channel, Type type)
    {
        return new Reference(type, "ref" + channel);
    }

    private static Block booleanBlock(Boolean[] values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, values.length);
        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }
        return builder.build();
    }

    private static Map<Symbol, Integer> layoutFor(List<Type> inputTypes)
    {
        ImmutableMap.Builder<Symbol, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < inputTypes.size(); i++) {
            builder.put(new Symbol(inputTypes.get(i), "ref" + i), i);
        }
        return builder.buildOrThrow();
    }

    private static Expression buildUnaryOperatorExpression(OperatorType operator, ResolvedFunction function, Expression argument)
    {
        if (operator == OperatorType.CAST) {
            return new Cast(argument, function.signature().getReturnType());
        }
        // Note: SATURATED_FLOOR_CAST is still expressed as a Call (operator function), not a Cast IR node.
        return new Call(function, ImmutableList.of(argument));
    }

    private record Pair<F, S>(F first, S second)
    {
        Pair
        {
            requireNonNull(first, "first is null");
            requireNonNull(second, "second is null");
        }
    }
}
