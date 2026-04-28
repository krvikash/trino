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
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.testing.TestingSession;
import io.trino.type.LikePattern;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuExpressions
{
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final GpuExpressionCompiler gpuCompiler = new GpuExpressionCompiler();

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
                RowExpression rowExpression = createLikeExpression(varcharChannel, pattern, escape);
                assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(varcharChannel));
            }
        }
    }

    @Test
    public void testBooleanConstant()
    {
        testConstant(constant(true, BOOLEAN));
        testConstant(constant(false, BOOLEAN));
        testConstant(constant(null, BOOLEAN));
    }

    @Test
    public void testTinyintConstant()
    {
        testConstant(constant(42L, TINYINT));
        testConstant(constant(-1L, TINYINT));
        testConstant(constant(0L, TINYINT));
        testConstant(constant(null, TINYINT));
    }

    @Test
    public void testSmallintConstant()
    {
        testConstant(constant(1234L, SMALLINT));
        testConstant(constant(-5678L, SMALLINT));
        testConstant(constant(null, SMALLINT));
    }

    @Test
    public void testIntegerConstant()
    {
        testConstant(constant(123456L, INTEGER));
        testConstant(constant(-789012L, INTEGER));
        testConstant(constant(0L, INTEGER));
        testConstant(constant(null, INTEGER));
    }

    @Test
    public void testBigintConstant()
    {
        testConstant(constant(1234567890123L, BIGINT));
        testConstant(constant(-9876543210L, BIGINT));
        testConstant(constant(0L, BIGINT));
        testConstant(constant(null, BIGINT));
    }

    @Test
    public void testRealConstant()
    {
        testConstant(constant((long) Float.floatToIntBits(3.14f), REAL));
        testConstant(constant((long) Float.floatToIntBits(-2.5f), REAL));
        testConstant(constant((long) Float.floatToIntBits(0.0f), REAL));
        testConstant(constant((long) Float.floatToIntBits(Float.POSITIVE_INFINITY), REAL));
        testConstant(constant((long) Float.floatToIntBits(Float.NEGATIVE_INFINITY), REAL));
        testConstant(constant((long) Float.floatToIntBits(Float.NaN), REAL));
        testConstant(constant(null, REAL));
    }

    @Test
    public void testDoubleConstant()
    {
        testConstant(constant(3.14159265359, DOUBLE));
        testConstant(constant(-2.71828, DOUBLE));
        testConstant(constant(0.0, DOUBLE));
        testConstant(constant(Double.POSITIVE_INFINITY, DOUBLE));
        testConstant(constant(Double.NEGATIVE_INFINITY, DOUBLE));
        testConstant(constant(Double.NaN, DOUBLE));
        testConstant(constant(null, DOUBLE));
    }

    @Test
    public void testVarcharConstant()
    {
        testConstant(constant(Slices.utf8Slice("hello"), VARCHAR));
        testConstant(constant(Slices.utf8Slice(""), VARCHAR));
        testConstant(constant(Slices.utf8Slice("Łania szła piękną łąką pod Warszawą"), VARCHAR));
        testConstant(constant(null, VARCHAR));
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
                NUMBER);
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
                        .map(value -> new Page(nativeValueToBlock(type, value)))
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

        // Unary operators: same shape, single-type loop.
        for (Type type : testedTypes) {
            for (OperatorType operator : OperatorType.values()) {
                if (operator.getArgumentCount() != 1) {
                    continue;
                }
                if (operator == OperatorType.CAST || operator == OperatorType.SATURATED_FLOOR_CAST) {
                    // Casts tested in TestGpuCasts // TODO unify tests into one
                    continue;
                }

                ResolvedFunction function;
                try {
                    function = functionResolution.resolveOperator(operator, List.of(type));
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
                    verify(
                            type instanceof DecimalType,
                            "Unexpected coercion for unary %s on %s: argument types %s",
                            operator,
                            type,
                            function.signature().getArgumentTypes());
                }

                testedOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                CallExpression expression = call(function, new InputReferenceExpression(0, coerced));
                Optional<CompiledExpression> gpuExpression = gpuCompiler.compileExpression(expression);
                if (gpuExpression.isEmpty()) {
                    // Not supported for GPU execution
                    continue;
                }
                gpuEnabledOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression);
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

        // Binary operators: cross-product over (leftType, rightType).
        for (Type leftType : testedTypes) {
            for (Type rightType : testedTypes) {
                for (OperatorType operator : OperatorType.values()) {
                    if (operator.getArgumentCount() != 2) {
                        continue;
                    }

                    ResolvedFunction function;
                    try {
                        function = functionResolution.resolveOperator(operator, List.of(leftType, rightType));
                    }
                    catch (OperatorNotFoundException e) {
                        continue;
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

                    CallExpression expression = call(function, new InputReferenceExpression(0, leftCoerced), new InputReferenceExpression(1, rightCoerced));
                    Optional<CompiledExpression> gpuExpression = gpuCompiler.compileExpression(expression);
                    if (gpuExpression.isEmpty()) {
                        // Not supported for GPU execution
                        continue;
                    }
                    gpuEnabledOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    try {
                        CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                        assertThat(compiledGpu.inputChannels().getInputChannels())
                                .containsExactlyInAnyOrderElementsOf(Set.of(0, 1));
                        PageProcessor cpuProcessor = compileCpuExpression(expression);
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
                        "real < real",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)");

        // Self-test
        assertThat(gpuEnabledOperators)
                .contains(
                        "bigint + bigint",
                        "real < real",
                        "decimal(3,0) < decimal(3,0)")
                .doesNotContain(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint",
                        "decimal(3,0) + decimal(3,0)", // short decimal arithmetic example
                        "decimal(27,5) + decimal(27,5)", // long decimal arithmetic example
                        "decimal(27,0) - decimal(3,0)"); // decimal arithmetic with different operand types
    }

    private static List<@Nullable TrinoNumber> numericValuesToTest()
    {
        List<Long> initial = List.of(
                0L,
                -1L, 1L,
                (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE, (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE,
                Long.MIN_VALUE, Long.MAX_VALUE);
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
        ResolvedFunction castFunction = functionResolution.getCoercion(sourceType, targetType);
        CallExpression castExpression = call(castFunction, new InputReferenceExpression(0, sourceType));
        PageProcessor pageProcessor = compileCpuExpression(castExpression);
        for (@Nullable Object sourceValue : sourceNativeValues) {
            Page sourcePage = new Page(nativeValueToBlock(sourceType, sourceValue));
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
                        nativeValueToBlock(leftType, leftValue),
                        nativeValueToBlock(rightType, rightValue)));
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
        testArithmetic(OperatorType.MODULUS, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testComparisonOperators(NullsProvider nullsProvider)
    {
        for (OperatorType operatorType : List.of(OperatorType.EQUAL, OperatorType.LESS_THAN, OperatorType.LESS_THAN_OR_EQUAL)) {
            testComparison(operatorType, BIGINT, nullsProvider);
            testComparison(operatorType, INTEGER, nullsProvider);
            testComparison(operatorType, SMALLINT, nullsProvider);
            testComparison(operatorType, TINYINT, nullsProvider);
            testComparison(operatorType, DOUBLE, nullsProvider);
            testComparison(operatorType, REAL, nullsProvider);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testAnd(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // AND of two comparisons: a < 50 AND b > 0
        RowExpression left = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                constant(50L, BIGINT));
        RowExpression right = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                constant(0L, BIGINT),
                field(channelB, BIGINT));
        RowExpression rowExpression = new SpecialForm(SpecialForm.Form.AND, BOOLEAN, List.of(left, right), List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testOr(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // OR of two comparisons: a < 50 OR b > 0
        RowExpression left = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                constant(50L, BIGINT));
        RowExpression right = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                constant(0L, BIGINT),
                field(channelB, BIGINT));
        RowExpression rowExpression = new SpecialForm(SpecialForm.Form.OR, BOOLEAN, List.of(left, right), List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testNot(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NOT of a comparison: NOT(a < 50)
        RowExpression comparison = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                constant(50L, BIGINT));
        RowExpression rowExpression = call(
                functionResolution.resolveFunction("$not", fromTypes(BOOLEAN)),
                comparison);

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIsNull(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        RowExpression rowExpression = new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, List.of(field(channelA, BIGINT)), List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBetween(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // a BETWEEN 10 AND 50
        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.BETWEEN,
                BOOLEAN,
                List.of(field(channelA, BIGINT), constant(10L, BIGINT), constant(50L, BIGINT)),
                List.of(functionResolution.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, List.of(BIGINT, BIGINT))));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testCoalesce(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.COALESCE,
                BIGINT,
                List.of(field(channelA, BIGINT), field(channelB, BIGINT)),
                List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIf(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // IF(a < 50, a, b)
        RowExpression condition = call(
                functionResolution.resolveOperator(OperatorType.LESS_THAN, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                constant(50L, BIGINT));
        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.IF,
                BIGINT,
                List.of(condition, field(channelA, BIGINT), field(channelB, BIGINT)),
                List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
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

        RowExpression bNotZero = call(
                functionResolution.resolveFunction("$not", fromTypes(BOOLEAN)),
                call(
                        functionResolution.resolveOperator(OperatorType.EQUAL, List.of(BIGINT, BIGINT)),
                        field(channelB, BIGINT),
                        constant(0L, BIGINT)));
        RowExpression aDivB = call(
                functionResolution.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                field(channelB, BIGINT));
        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.IF,
                BIGINT,
                List.of(bNotZero, aDivB, field(channelA, BIGINT)),
                List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
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

        RowExpression bNotZero = call(
                functionResolution.resolveFunction("$not", fromTypes(BOOLEAN)),
                call(
                        functionResolution.resolveOperator(OperatorType.EQUAL, List.of(BIGINT, BIGINT)),
                        field(channelB, BIGINT),
                        constant(0L, BIGINT)));
        RowExpression coalesceB = new SpecialForm(
                SpecialForm.Form.COALESCE,
                BIGINT,
                List.of(field(channelB, BIGINT), constant(0L, BIGINT)),
                List.of());
        RowExpression divide = call(
                functionResolution.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                constant(10L, BIGINT),
                coalesceB);
        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.IF,
                BIGINT,
                List.of(bNotZero, divide, constant(42L, BIGINT)),
                List.of());

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDateTimeExtract(NullsProvider nullsProvider)
    {
        for (String functionName : List.of("day", "hour", "minute", "second")) {
            for (Type type : List.of(TIMESTAMP_SECONDS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS)) {
                testDateTimeExtract(functionName, type, nullsProvider);
            }
        }
        // Trino only defines day() (aka day_of_month) for DATE — hour/minute/second are timestamp-only.
        testDateTimeExtract("day", DATE, nullsProvider);
    }

    private void testDateTimeExtract(String functionName, Type timestampType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        RowExpression rowExpression = call(
                functionResolution.resolveFunction(functionName, fromTypes(timestampType)),
                field(channelA, timestampType));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testStringLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(List.of(64), nullsProvider);

        RowExpression rowExpression = call(
                functionResolution.resolveFunction("length", fromTypes(VARCHAR)),
                field(varcharChannel, VARCHAR));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(varcharChannel));
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
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBlock(type, positionsCount, nullsProvider)));

        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        arguments.add(field(channelA, type));
        for (Object value : inValues) {
            arguments.add(constant(value, type));
        }

        RowExpression rowExpression = new SpecialForm(
                SpecialForm.Form.IN,
                BOOLEAN,
                arguments.build(),
                getInFunctionalDependencies(type));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA));
    }

    private List<ResolvedFunction> getInFunctionalDependencies(Type type)
    {
        return ImmutableList.of(
                functionResolution.resolveOperator(OperatorType.EQUAL, ImmutableList.of(type, type)),
                functionResolution.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(type)),
                functionResolution.resolveOperator(OperatorType.INDETERMINATE, ImmutableList.of(type)));
    }

    private void testArithmetic(OperatorType operatorType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000),
                createBigintBlock(positionsCount, nullsProvider, 1, 100)));  // non-zero to avoid division by zero

        RowExpression rowExpression = call(
                functionResolution.resolveOperator(operatorType, List.of(BIGINT, BIGINT)),
                field(channelA, BIGINT),
                field(channelB, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
    }

    private void testComparison(OperatorType operatorType, Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        RowExpression rowExpression = call(
                functionResolution.resolveOperator(operatorType, List.of(type, type)),
                field(channelA, type),
                field(channelB, type));

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, Set.of(channelA, channelB));
    }

    private void testConstant(RowExpression constantExpression)
    {
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, 0, 100)));

        CompiledExpression gpuExpression = gpuCompiler.compileExpression(constantExpression)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + constantExpression));
        assertThat(gpuExpression.inputChannels().getInputChannels()).isEmpty();

        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, constantExpression, gpuExpression);
        List<Page> cpuResults = executeWithCpu(constantExpression, inputPages);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(constantExpression.type()));
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, Set<Integer> expectedInputChannels)
    {
        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, expectedInputChannels, false);
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, Set<Integer> expectedInputChannels, boolean allowMultipleInputsForExceptionTesting)
    {
        PageProcessor pageProcessor = compileCpuExpression(rowExpression);
        CompiledExpression gpuExpression = gpuCompiler.compileExpression(rowExpression)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + rowExpression));

        assertThat(gpuExpression.inputChannels().getInputChannels())
                .containsExactlyInAnyOrderElementsOf(expectedInputChannels);

        assertGpuMatchesCpu(inputPages, inputTypes, rowExpression, pageProcessor, gpuExpression, allowMultipleInputsForExceptionTesting);
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, PageProcessor cpuProcessor, CompiledExpression gpuExpression, boolean allowMultipleInputsForExceptionTesting)
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
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, rowExpression, gpuExpression))
                        .hasErrorCode(cpuExecutionException::getErrorCode);
            }
            catch (AssertionError failure) {
                failure.addSuppressed(new Exception("rowExpression: " + rowExpression));
                failure.addSuppressed(new Exception("inputTypes: " + inputTypes));
                failure.addSuppressed(new Exception("CPU execution exception", cpuExecutionException));
                throw failure;
            }
            return;
        }
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, rowExpression, gpuExpression);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(rowExpression.type()));
    }

    private List<Page> executeWithGpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, CompiledExpression gpuExpression)
    {
        return executeGpuOperation(
                inputPages,
                inputTypes,
                List.of(rowExpression.type()),
                copyToDevice -> new GpuProject(copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))));
    }

    private List<Page> executeWithCpu(RowExpression expression, List<Page> inputPages)
    {
        return executeWithCpu(compileCpuExpression(expression), inputPages);
    }

    private PageProcessor compileCpuExpression(RowExpression expression)
    {
        return functionResolution.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        List.of(expression),
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);
    }

    private List<Page> executeWithCpu(PageProcessor compiledProcessor, List<Page> inputPages)
    {
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            Iterator<Optional<Page>> processed = compiledProcessor.process(FULL_CONNECTOR_SESSION, new DriverYieldSignal(), context, SourcePage.create(inputPage));
            stream(processed)
                    .flatMap(Optional::stream)
                    .forEachOrdered(outputPages::add);
        }
        return outputPages.build();
    }

    private RowExpression createLikeExpression(int channel, String pattern, Optional<Character> escape)
    {
        return call(
                functionResolution.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                field(channel, VARCHAR),
                constant(LikePattern.compile(pattern, escape), LIKE_PATTERN));
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

    private record Pair<F, S>(F first, S second)
    {
        Pair
        {
            requireNonNull(first, "first is null");
            requireNonNull(second, "second is null");
        }
    }
}
