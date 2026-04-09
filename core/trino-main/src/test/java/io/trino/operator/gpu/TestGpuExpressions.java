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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.testing.TestingSession;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

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

    private static Block createBigintBlock(int positionsCount, NullsProvider nullsProvider, long minValue, long maxValue)
    {
        Random random = new Random(42);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, random.nextLong(minValue, maxValue));
            }
        }
        return builder.build();
    }

    private static Block createBlock(Type type, int positionsCount, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else if (type == BIGINT) {
                BIGINT.writeLong(builder, random.nextLong(-1000, 1000));
            }
            else if (type == INTEGER) {
                INTEGER.writeLong(builder, random.nextInt(-1000, 1000));
            }
            else if (type == SMALLINT) {
                SMALLINT.writeLong(builder, random.nextInt(-1000, 1000));
            }
            else if (type == TINYINT) {
                TINYINT.writeLong(builder, random.nextInt(-100, 100));
            }
            else if (type == DOUBLE) {
                DOUBLE.writeDouble(builder, random.nextDouble(-1000, 1000));
            }
            else if (type == REAL) {
                REAL.writeLong(builder, Float.floatToIntBits((float) random.nextDouble(-1000, 1000)));
            }
            else if (type == VARCHAR) {
                VARCHAR.writeSlice(builder, Slices.utf8Slice("test" + random.nextInt(100)));
            }
            else {
                throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
        return builder.build();
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

        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, constantExpression, gpuExpression, Set.of(0));
        List<Page> cpuResults = executeWithCpu(inputPages, constantExpression);
        assertSameData(gpuResults, cpuResults, List.of(constantExpression.type()));
    }

    private void assertGpuMatchesCpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, Set<Integer> expectedInputChannels)
    {
        CompiledExpression gpuExpression = gpuCompiler.compileExpression(rowExpression)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + rowExpression));

        assertThat(gpuExpression.inputChannels().getInputChannels())
                .containsExactlyInAnyOrderElementsOf(expectedInputChannels);

        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, rowExpression, gpuExpression, expectedInputChannels);
        List<Page> cpuResults = executeWithCpu(inputPages, rowExpression);
        assertSameData(gpuResults, cpuResults, List.of(rowExpression.type()));
    }

    private List<Page> executeWithGpu(List<Page> inputPages, List<Type> inputTypes, RowExpression rowExpression, CompiledExpression gpuExpression, Set<Integer> deviceChannels)
    {
        Iterator<Page> input = inputPages.iterator();

        BufferPages bufferPages = new BufferPages();
        CopyToDevice copyToDevice = new CopyToDevice(
                bufferPages,
                inputTypes,
                deviceChannels);
        GpuProject gpuFilter = new GpuProject(copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression)));
        CopyToBlocks copyToBlocks = new CopyToBlocks(gpuFilter, List.of(rowExpression.type()));
        GpuPageToPages gpuPageToPages = new GpuPageToPages();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        while (true) {
            if (!input.hasNext()) {
                bufferPages.noMoreInput();
            }
            else if (bufferPages.needsInput()) {
                bufferPages.addInput(input.next());
            }

            gpuPageToPages.drain().forEachOrdered(outputPages::add);

            @Own GpuOperation.Result result = copyToBlocks.execute();
            switch (result) {
                case GpuOperation.Blocked _ -> throw new UnsupportedOperationException("Unsupported blocked future, what shall I do?");
                case GpuOperation.Data(GpuPage gpuPage) -> {
                    try (gpuPage) {
                        gpuPageToPages.add(gpuPage);
                    }
                }
                case GpuOperation.Yielded() -> {
                    // continue
                }
                case GpuOperation.Finished() -> {
                    checkState(gpuPageToPages.poll().isEmpty(), "gpuPageToPages should be drained at this point");
                    return outputPages.build();
                }
            }
        }
    }

    private List<Page> executeWithCpu(List<Page> inputPages, RowExpression expression)
    {
        PageProcessor compiledProcessor = functionResolution.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        List.of(expression),
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);

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

    private void assertSameData(List<Page> actual, List<Page> expected, List<Type> types)
    {
        assertThat(actual.stream().mapToInt(Page::getPositionCount).sum()).as("actual position count (sum over all returned pages)")
                .isEqualTo(expected.stream().mapToInt(Page::getPositionCount).sum());

        Streams.forEachPair(
                positions(actual),
                positions(expected),
                (left, right) -> {
                    assertThat(readValues(left.page, left.position, types))
                            .isEqualTo(readValues(right.page, right.position, types));
                });
    }

    private static Stream<Integer> randomInts(int minInclusive, int maxExclusive)
    {
        Random random = new Random(42); // Fixed seed for reproducibility
        return IntStream.generate(() -> random.nextInt(minInclusive, maxExclusive))
                .boxed();
    }

    private static List<Optional<Object>> readValues(Page page, int position, List<Type> types)
    {
        checkArgument(page.getChannelCount() == types.size());
        return IntStream.range(0, types.size())
                .mapToObj(column -> Optional.ofNullable(readNativeValue(types.get(column), page.getBlock(column), position)))
                .collect(toImmutableList());
    }

    private static Stream<PagePosition> positions(List<Page> pages)
    {
        return pages.stream().flatMap(TestGpuExpressions::positions);
    }

    private static Stream<PagePosition> positions(Page page)
    {
        return IntStream.range(0, page.getPositionCount())
                .mapToObj(i -> new PagePosition(page, i));
    }

    private record PagePosition(Page page, int position) {}
}
