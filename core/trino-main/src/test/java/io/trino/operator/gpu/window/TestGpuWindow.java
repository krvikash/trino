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
package io.trino.operator.gpu.window;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverContext;
import io.trino.operator.DummySpillerFactory;
import io.trino.operator.TestWindowOperator;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator;
import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToBlocks;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuTestUtils;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.window.GpuWindowCompiler.CompileResult;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.FLOATING_POINT_EDGE_CASES;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_FLOATING_POINT_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataWithoutOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.gpu.GpuTestUtils.drainToPages;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.TESTED_GPU_WINDOW_FUNCTIONS;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.compileWindow;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.createOperation;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

final class TestGpuWindow
{
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduler-%s"));
    private static final FrameInfo DEFAULT_FRAME = new FrameInfo(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private static final int POSITION_COUNT = 100;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @AfterAll
    static void shutdownExecutors()
    {
        SCHEDULED_EXECUTOR.shutdownNow();
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.window.GpuWindowTestUtils#TESTED_GPU_WINDOW_FUNCTIONS")
    void testNoSourceColumns(TestedWindowFunction function)
    {
        if (!function.argumentTypes().isEmpty()) {
            abort("Function taking arguments cannot be tested without source columns");
        }
        assertGpuWindowMatchesCpu(
                List.of(new Page(5)),
                List.of(),
                new int[0],
                new int[0],
                List.of(),
                List.of(BoundTestedWindowFunction.of(function.name())));
    }

    @ParameterizedTest
    @MethodSource("fourColumnsWithNullProviderAndSortOrder")
    void testMultiColumnKeys(WindowFunctionTestCase testCase, NullsProvider nullsProvider, SortOrder firstSortOrder, SortOrder secondSortOrder)
    {
        assertGpuWindowMatchesCpu(
                createPages(testCase.sourceTypes(), nullsProvider, 1, POSITION_COUNT),
                testCase.sourceTypes(),
                new int[] {0, 1},
                new int[] {2, 3},
                List.of(firstSortOrder, secondSortOrder),
                List.of(testCase.function()));
    }

    @ParameterizedTest
    @MethodSource("twoColumns")
    void testMultipleInputPages(WindowFunctionTestCase testCase)
    {
        assertGpuWindowMatchesCpu(
                createPages(testCase.sourceTypes(), NO_NULLS, 3, TARGET_ROW_COUNT),
                testCase.sourceTypes(),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(testCase.function()));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.window.GpuWindowTestUtils#TESTED_GPU_WINDOW_FUNCTIONS")
    void testCharKeysUsePaddedSemantics(TestedWindowFunction function)
    {
        Type charType = createCharType(2);
        BlockBuilder charBlockBuilder = charType.createBlockBuilder(null, 4);
        for (String value : List.of("a", "a", "a\u0001", "a\u0001")) {
            charType.writeSlice(charBlockBuilder, utf8Slice(value));
        }

        int keyChannelCount = 2;
        int[] argumentChannels = IntStream.range(keyChannelCount, keyChannelCount + function.argumentTypes().size()).toArray();

        assertGpuWindowMatchesCpu(
                List.of(appendArgumentBlocks(
                        function,
                        createLongsBlock(1L, 1L, 1L, 1L),
                        charBlockBuilder.build())),
                appendArgumentTypes(function, List.of(BIGINT, charType)),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(BoundTestedWindowFunction.of(function, argumentChannels)));

        assertGpuWindowMatchesCpu(
                List.of(appendArgumentBlocks(
                        function,
                        charBlockBuilder.build(),
                        createLongsBlock(1L, 2L, 3L, 4L))),
                appendArgumentTypes(function, List.of(charType, BIGINT)),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(BoundTestedWindowFunction.of(function, argumentChannels)));
    }

    @Test
    void testRankBreaksPeersOnSecondOrderKey()
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 1L),
                        createLongsBlock(10L, 10L, 10L, 20L),
                        createLongsBlock(3L, 1L, 2L, 1L))),
                List.of(BIGINT, BIGINT, BIGINT),
                new int[] {0},
                new int[] {1, 2},
                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                List.of(BoundTestedWindowFunction.of("rank"), BoundTestedWindowFunction.of("dense_rank")));
    }

    @Test
    void testRankGroupsRowsWithEqualOrderKeyTuple()
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 1L, 2L, 2L),
                        createLongsBlock(10L, 10L, 20L, 20L, 10L, 10L),
                        createLongsBlock(5L, 5L, 7L, 8L, 1L, 1L))),
                List.of(BIGINT, BIGINT, BIGINT),
                new int[] {0},
                new int[] {1, 2},
                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                List.of(BoundTestedWindowFunction.of("rank"), BoundTestedWindowFunction.of("dense_rank")));
    }

    @ParameterizedTest
    @MethodSource("floatingPointTestCases")
    void testOrderByFloatingPointEdgeCases(TestedWindowFunction function, Type type, SortOrder sortOrder, List<Double> values)
    {
        ImmutableList.Builder<Type> sourceTypes = ImmutableList.builder();
        sourceTypes.add(BIGINT, type);
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        blocks.add(createLongsBlock(Collections.nCopies(values.size(), 1L)));
        blocks.add(createFloatingPointBlock(type, values));

        int[] argumentChannels = new int[function.argumentTypes().size()];
        int nextArgumentChannel = 2;
        for (int argument = 0; argument < function.argumentTypes().size(); argument++) {
            Type argumentType = function.argumentTypes().get(argument);
            sourceTypes.add(argumentType);
            blocks.add(createBlock(argumentType, values.size(), NO_NULLS));
            argumentChannels[argument] = nextArgumentChannel++;
        }
        assertGpuWindowMatchesCpu(
                List.of(new Page(blocks.build().toArray(Block[]::new))),
                sourceTypes.build(),
                new int[] {0},
                new int[] {1},
                List.of(sortOrder),
                List.of(BoundTestedWindowFunction.of(function, argumentChannels)));
    }

    @ParameterizedTest
    @MethodSource("floatingPointTestCases")
    void testPartitionByFloatingPointEdgeCases(TestedWindowFunction function, Type type, SortOrder sortOrder, List<Double> values)
    {
        ImmutableList.Builder<Type> sourceTypes = ImmutableList.builder();
        sourceTypes.add(type, BIGINT);
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        blocks.add(createFloatingPointBlock(type, values));
        blocks.add(createLongsBlock(LongStream.range(0, values.size()).boxed().toList()));

        int[] argumentChannels = new int[function.argumentTypes().size()];
        int nextArgumentChannel = 2;
        for (int argument = 0; argument < function.argumentTypes().size(); argument++) {
            Type argumentType = function.argumentTypes().get(argument);
            sourceTypes.add(argumentType);
            blocks.add(createBlock(argumentType, values.size(), NO_NULLS));
            argumentChannels[argument] = nextArgumentChannel++;
        }
        assertGpuWindowMatchesCpu(
                List.of(new Page(blocks.build().toArray(Block[]::new))),
                sourceTypes.build(),
                new int[] {0},
                new int[] {1},
                List.of(sortOrder),
                List.of(BoundTestedWindowFunction.of(function, argumentChannels)));
    }

    @Test
    void testDuplicateRankingFunctions()
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 2L, 2L),
                        createLongsBlock(10L, 10L, 20L, 10L, 20L))),
                List.of(BIGINT, BIGINT),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(
                        BoundTestedWindowFunction.of("rank"),
                        BoundTestedWindowFunction.of("rank"),
                        BoundTestedWindowFunction.of("dense_rank"),
                        BoundTestedWindowFunction.of("dense_rank"),
                        BoundTestedWindowFunction.of("row_number"),
                        BoundTestedWindowFunction.of("row_number")));
    }

    @Test
    void testAvgShortDecimalSumExceeds64Bits()
    {
        // A short decimal is stored as a 64-bit value, and ten rows at the largest value decimal(18, 0) holds sum
        // past Long.MAX_VALUE, so the running sum fits only because it is accumulated as DECIMAL128 rather than in
        // the storage type of the input.
        DecimalType decimalType = createDecimalType(18, 0);
        int rowCount = 10;
        for (Long longValue : List.of(999_999_999_999_999_999L, -999_999_999_999_999_999L)) {
            BigInteger value = BigInteger.valueOf(longValue);
            List<BigInteger> unscaledValues = ImmutableList.<BigInteger>builder()
                    .add(value.subtract(BigInteger.valueOf(9L * value.signum())))  // to keep the divisions inexact
                    .addAll(Collections.nCopies(rowCount - 1, value))
                    .build();
            assertAvgMatchesCpu(decimalType, GpuTestUtils.decimalBlock(decimalType, unscaledValues));
        }
    }

    @Test
    void testAvgLongDecimalSumExceedsDeclaredPrecision()
    {
        // The input is already DECIMAL128 here, so unlike in testAvgShortDecimalSumExceeds64Bits the sum outgrows
        // the declared precision rather than the storage width: two values close to the precision-28 maximum
        // already add up to 29 digits, which is allowed only because the running sum is not narrowed to the
        // declared type.
        DecimalType decimalType = createDecimalType(28, 0);
        int rowCount = 10;
        BigInteger max = BigInteger.TEN.pow(28).subtract(BigInteger.ONE);
        for (BigInteger value : List.of(max, max.negate())) {
            List<BigInteger> unscaledValues = ImmutableList.<BigInteger>builder()
                    .add(value.subtract(BigInteger.valueOf(9L * value.signum()))) // to keep the divisions inexact
                    .addAll(Collections.nCopies(rowCount - 1, value))
                    .build();
            assertAvgMatchesCpu(decimalType, GpuTestUtils.decimalBlock(decimalType, unscaledValues));
        }
    }

    @Test
    void testAvgWideDecimalSumExceedsDeclaredPrecision()
    {
        // Above precision 28 the running sum is kept as 64-bit sums of the 32-bit chunks of the values, and like
        // in testAvgLongDecimalSumExceedsDeclaredPrecision it outgrows the declared precision without being
        // narrowed.
        int precision = 29;
        for (int scale : List.of(0, 2, 6, precision)) {
            DecimalType decimalType = createDecimalType(precision, scale);
            int rowCount = 10;
            BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
            for (BigInteger value : List.of(max, max.negate())) {
                List<BigInteger> unscaledValues = ImmutableList.<BigInteger>builder()
                        .add(value.subtract(BigInteger.valueOf(9L * value.signum()))) // to keep the divisions inexact
                        .addAll(Collections.nCopies(rowCount - 1, value))
                        .build();
                assertAvgMatchesCpu(decimalType, GpuTestUtils.decimalBlock(decimalType, unscaledValues));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 6, 38})
    void testAvgDecimalSumExceeds128Bits(int scale)
    {
        // Two values near the decimal(38, s) maximum add up past the signed 128-bit range, but their average is
        // still a valid decimal(38, s). The order keys make the first two rows peers, so they share a frame
        // whose sum overflows 128 bits, and so does the frame of the last row. The cancelling rows in between
        // return the sum to a small value.
        DecimalType decimalType = createDecimalType(38, scale);
        BigInteger max = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
        BigInteger nearMax = max.subtract(BigInteger.valueOf(7));
        for (int sign : List.of(1, -1)) {
            List<BigInteger> unscaledValues = List.of(
                    max.multiply(BigInteger.valueOf(sign)),
                    nearMax.multiply(BigInteger.valueOf(sign)),
                    max.multiply(BigInteger.valueOf(-sign)),
                    nearMax.multiply(BigInteger.valueOf(-sign)),
                    BigInteger.valueOf(3L * sign),
                    max.multiply(BigInteger.valueOf(sign)),
                    max.multiply(BigInteger.valueOf(sign)));
            assertAvgMatchesCpu(
                    List.of(new Page(
                            createLongsBlock(1L, 1L, 1L, 1L, 1L, 1L, 1L),
                            createLongsBlock(0L, 0L, 1L, 2L, 3L, 4L, 5L),
                            GpuTestUtils.decimalBlock(decimalType, unscaledValues))),
                    List.of(BIGINT, BIGINT, decimalType),
                    0,
                    1,
                    ASC_NULLS_LAST,
                    List.of(BoundTestedWindowFunction.of("avg", 2)));
        }
    }

    @Test
    void testAvgDecimalCompilesForAllPrecisionsAndScales()
    {
        int typeCount = 0;
        for (int precision = 1; precision <= MAX_PRECISION; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                DecimalType decimalType = createDecimalType(precision, scale);
                CompileResult result = compileWindow(
                        List.of(BIGINT, BIGINT, decimalType),
                        new int[] {0},
                        new int[] {1},
                        List.of(ASC_NULLS_LAST),
                        List.of(BoundTestedWindowFunction.of("avg", 2)));
                assertThat(result)
                        .as("avg(%s)", decimalType)
                        .isInstanceOfSatisfying(CompileResult.Success.class, success -> assertThat(success.outputTypes()).last().isEqualTo(decimalType));
                typeCount++;
            }
        }
        assertThat(typeCount).isEqualTo(779);
    }

    @ParameterizedTest
    @MethodSource("avgDecimalTypes")
    void testAvgDecimalHalfUpRounding(DecimalType decimalType)
    {
        // Unscaled values stay below 10 so that both the inputs and the results are valid for every tested
        // precision, including 1. A sum of 3 over two rows lands exactly halfway, which HALF_UP rounds away
        // from zero for both signs.
        assertAvgMatchesCpu(decimalType, 1L, 2L);
        assertAvgMatchesCpu(decimalType, -1L, -2L);
        // 7 / 3 rounds towards zero and 8 / 3 away from it
        assertAvgMatchesCpu(decimalType, 1L, 3L, 3L);
        assertAvgMatchesCpu(decimalType, -1L, -3L, -3L);
        assertAvgMatchesCpu(decimalType, 2L, 3L, 3L);
        assertAvgMatchesCpu(decimalType, -2L, -3L, -3L);
    }

    @ParameterizedTest
    @MethodSource("avgDecimalTypes")
    void testAvgDecimalFrameWithOnlyNulls(DecimalType decimalType)
    {
        // In the first partition, the cumulative frames of the first two rows hold nulls only, so their count is
        // zero and the average is null even though the partition has non-null rows further down. The second
        // partition holds nulls only, so its frames are null-only over the whole partition as well.
        assertAvgMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 1L, 2L, 2L),
                        createLongSequenceBlock(0, 6),
                        decimalBlock(decimalType, null, null, 2L, 4L, null, null))),
                List.of(BIGINT, BIGINT, decimalType),
                0,
                1,
                ASC_NULLS_LAST,
                List.of(BoundTestedWindowFunction.of("avg", 2)));
    }

    @ParameterizedTest
    @MethodSource("avgDecimalTypesAndSortOrders")
    void testAvgDecimalFrameWithOnlyNullsAndNullOrderKeys(DecimalType decimalType, SortOrder sortOrder)
    {
        // Rows with a null order key are peers of each other. With NULLS FIRST they open the partition, so their
        // shared frame holds nulls only; with NULLS LAST they close it, so their frame covers the whole partition.
        // The second partition holds null order keys and null values only.
        assertAvgMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 1L, 1L, 2L, 2L),
                        createLongsBlock(null, null, 1L, 2L, 2L, null, null),
                        decimalBlock(decimalType, null, null, 2L, null, 4L, null, null))),
                List.of(BIGINT, BIGINT, decimalType),
                0,
                1,
                sortOrder,
                List.of(BoundTestedWindowFunction.of("avg", 2)));
    }

    @Test
    void testAvgMultipleDecimalFunctions()
    {
        DecimalType shortDecimal = createDecimalType(9, 2);
        DecimalType longDecimal = createDecimalType(19, 4);
        List<Type> sourceTypes = List.of(BIGINT, BIGINT, shortDecimal, longDecimal);
        assertAvgMatchesCpu(
                createPages(sourceTypes, RANDOM_NULLS, 1, POSITION_COUNT),
                sourceTypes,
                0,
                1,
                ASC_NULLS_LAST,
                List.of(
                        BoundTestedWindowFunction.of("avg", 2),
                        BoundTestedWindowFunction.of("avg", 3),
                        BoundTestedWindowFunction.of("avg", 2),
                        BoundTestedWindowFunction.of("avg", 3)));
    }

    @Test
    void testAvgInterleavedWithScanFunctions()
    {
        // Prefix sum and scan computations run in separate batches, so their results must be put back in the order
        // of the functions rather than batch by batch
        DecimalType shortDecimal = createDecimalType(9, 2);
        DecimalType longDecimal = createDecimalType(19, 4);
        List<Type> sourceTypes = List.of(BIGINT, BIGINT, shortDecimal, longDecimal);
        assertAvgMatchesCpu(
                createPages(sourceTypes, RANDOM_NULLS, 1, POSITION_COUNT),
                sourceTypes,
                0,
                1,
                ASC_NULLS_LAST,
                List.of(
                        BoundTestedWindowFunction.of("avg", 2),
                        BoundTestedWindowFunction.of("rank"),
                        BoundTestedWindowFunction.of("avg", 3),
                        BoundTestedWindowFunction.of("row_number"),
                        BoundTestedWindowFunction.of("dense_rank")));
    }

    static Stream<DecimalType> avgDecimalTypes()
    {
        return Stream.of(
                createDecimalType(1, 0),
                createDecimalType(18, 0),
                createDecimalType(18, 9),
                createDecimalType(18, 18),
                createDecimalType(19, 0),
                createDecimalType(19, 10),
                createDecimalType(28, 0),
                createDecimalType(28, 14),
                createDecimalType(28, 28),
                createDecimalType(29, 0),
                createDecimalType(29, 2),
                createDecimalType(29, 6),
                createDecimalType(29, 29),
                createDecimalType(38, 0),
                createDecimalType(38, 2),
                createDecimalType(38, 6),
                createDecimalType(38, 38));
    }

    static Stream<Object[]> avgDecimalTypesAndSortOrders()
    {
        return Lists.cartesianProduct(
                        avgDecimalTypes().collect(toImmutableList()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    static Stream<WindowFunctionTestCase> twoColumns()
    {
        return allTypesAndFunctionsTestCases(2).stream();
    }

    static Stream<Object[]> fourColumnsWithNullProviderAndSortOrder()
    {
        return Lists.cartesianProduct(
                        allTypesAndFunctionsTestCases(4),
                        ImmutableList.copyOf(NullsProvider.values()),
                        ImmutableList.copyOf(SortOrder.values()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    private static List<WindowFunctionTestCase> allTypesAndFunctionsTestCases(int channelCount)
    {
        Random random = new Random(42);
        ImmutableList.Builder<WindowFunctionTestCase> builder = ImmutableList.builder();
        Set<List<Type>> typeTestCases = new HashSet<>();
        for (int testedTypeChannel = 0; testedTypeChannel < channelCount; testedTypeChannel++) {
            for (Type type : TESTED_GPU_TYPES) {
                List<Type> testCase = new ArrayList<>();
                for (int channel = 0; channel < channelCount; channel++) {
                    if (channel == testedTypeChannel) {
                        testCase.add(type);
                    }
                    else {
                        int index = random.nextInt(TESTED_GPU_TYPES.size());
                        testCase.add(TESTED_GPU_TYPES.get(index));
                    }
                }
                typeTestCases.add(testCase);
            }
        }
        for (TestedWindowFunction function : TESTED_GPU_WINDOW_FUNCTIONS) {
            for (List<Type> testCaseTypes : typeTestCases) {
                // argument channels follow the key channels
                int[] argumentChannels = IntStream.range(channelCount, channelCount + function.argumentTypes().size())
                        .toArray();
                BoundTestedWindowFunction boundFunction = BoundTestedWindowFunction.of(function, argumentChannels);
                List<Type> finalTypeTestCases = ImmutableList.<Type>builder()
                        .addAll(testCaseTypes)
                        .addAll(function.argumentTypes())
                        .build();
                builder.add(new WindowFunctionTestCase(boundFunction, finalTypeTestCases));
            }
        }
        return builder.build();
    }

    static Stream<Object[]> floatingPointTestCases()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_WINDOW_FUNCTIONS,
                        TESTED_GPU_FLOATING_POINT_TYPES,
                        ImmutableList.copyOf(SortOrder.values()).reverse(),
                        FLOATING_POINT_EDGE_CASES)
                .stream()
                .map(List::toArray);
    }

    private static List<Page> createPages(List<Type> types, NullsProvider nullsProvider, int pageCount, int positionsPerPage)
    {
        return ImmutableList.copyOf(GpuTestUtils.createPages(types, nullsProvider, pageCount, positionsPerPage, true));
    }

    private static List<Type> appendArgumentTypes(TestedWindowFunction function, List<Type> sourceTypes)
    {
        return ImmutableList.<Type>builder()
                .addAll(sourceTypes)
                .addAll(function.argumentTypes())
                .build();
    }

    private static Page appendArgumentBlocks(TestedWindowFunction function, Block... sourceBlocks)
    {
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        blocks.add(sourceBlocks);
        int positionCount = sourceBlocks[0].getPositionCount();
        for (Type argumentType : function.argumentTypes()) {
            blocks.add(createBlock(argumentType, positionCount, NO_NULLS));
        }
        return new Page(blocks.build().toArray(Block[]::new));
    }

    private static void assertAvgMatchesCpu(DecimalType decimalType, Long... unscaledValues)
    {
        assertAvgMatchesCpu(decimalType, decimalBlock(decimalType, unscaledValues));
    }

    private static void assertAvgMatchesCpu(DecimalType decimalType, Block values)
    {
        // Distinct order keys make every frame cumulative, so the last frame covers the whole partition and the
        // earlier frames are verified as well
        int rowCount = values.getPositionCount();
        assertAvgMatchesCpu(
                List.of(new Page(
                        createLongsBlock(Collections.nCopies(rowCount, 1L)),
                        createLongSequenceBlock(0, rowCount),
                        values)),
                List.of(BIGINT, BIGINT, decimalType),
                0,
                1,
                ASC_NULLS_LAST,
                List.of(BoundTestedWindowFunction.of("avg", 2)));
    }

    private static void assertAvgMatchesCpu(
            List<Page> inputPages,
            List<Type> sourceTypes,
            int partitionChannel,
            int orderChannel,
            SortOrder sortOrder,
            List<BoundTestedWindowFunction> functions)
    {
        for (int[] partitionChannels : List.of(new int[] {partitionChannel}, new int[0])) {
            // With order keys, the frame runs from the start of the partition to the last peer of the current row
            assertGpuWindowMatchesCpu(
                    inputPages,
                    sourceTypes,
                    partitionChannels,
                    new int[] {orderChannel},
                    List.of(sortOrder),
                    functions);
            // Without order keys, every row of the partition is a peer of the current row, so the frame is the whole partition.
            // Without partition keys either, that partition is the whole input.
            assertGpuWindowMatchesCpu(
                    inputPages,
                    sourceTypes,
                    partitionChannels,
                    new int[0],
                    List.of(),
                    functions);
        }
    }

    private static void assertGpuWindowMatchesCpu(
            List<Page> inputPages,
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<BoundTestedWindowFunction> functions)
    {
        CompileResult result = compileWindow(sourceTypes, partitionChannels, orderChannels, orderSortOrders, functions);
        if (isUnsupported(functions, orderChannels, sourceTypes)) {
            assertThat(result).isInstanceOf(CompileResult.Failure.class);
        }
        else {
            assertThat(result).isInstanceOf(CompileResult.Success.class);
            CompileResult.Success compiled = (CompileResult.Success) result;
            List<Type> outputTypes = compiled.outputTypes();
            List<Page> gpuResults = executeGpuWindow(inputPages, sourceTypes, outputTypes, compiled.stages());
            List<Page> cpuResults = executeCpuWindow(inputPages, sourceTypes, partitionChannels, orderChannels, orderSortOrders, functions);
            assertSameWindowDataWithoutOrder(
                    gpuResults,
                    cpuResults,
                    outputTypes,
                    sourceTypes,
                    partitionChannels,
                    orderChannels,
                    functions);
        }
    }

    private static boolean isUnsupported(List<BoundTestedWindowFunction> functions, int[] orderChannels, List<Type> sourceTypes)
    {
        return containsType(orderChannels, sourceTypes, VARBINARY) && containsFunction(functions, "rank", "dense_rank");
    }

    private static boolean containsType(int[] orderChannels, List<Type> sourceTypes, Type... types)
    {
        Set<Type> typeSet = ImmutableSet.copyOf(types);
        return Arrays.stream(orderChannels).mapToObj(sourceTypes::get).anyMatch(typeSet::contains);
    }

    private static boolean containsFunction(List<BoundTestedWindowFunction> functions, String... functionNames)
    {
        Set<String> functionNamesSet = ImmutableSet.copyOf(functionNames);
        return functions.stream().map(BoundTestedWindowFunction::name).anyMatch(functionNamesSet::contains);
    }

    private static void assertSameWindowDataWithoutOrder(
            List<Page> actual,
            List<Page> expected,
            List<Type> outputTypes,
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<BoundTestedWindowFunction> functions)
    {
        List<Integer> rowNumberOutputChannels = IntStream.range(0, functions.size())
                .filter(functionIndex -> functions.get(functionIndex).name().equals("row_number"))
                .map(functionIndex -> sourceTypes.size() + functionIndex)
                .boxed()
                .collect(toImmutableList());

        if (rowNumberOutputChannels.isEmpty()) {
            assertSameDataWithoutOrder(actual, expected, outputTypes);
            return;
        }

        // row_number is allowed to assign different numbers to different payload rows within one peer group.
        // Compare all other columns as complete rows so payload/function associations are still checked.
        List<Integer> stableOutputChannels = IntStream.range(0, outputTypes.size())
                .filter(channel -> !rowNumberOutputChannels.contains(channel))
                .boxed()
                .collect(toImmutableList());
        assertSameDataWithoutOrder(
                projectChannels(actual, stableOutputChannels),
                projectChannels(expected, stableOutputChannels),
                projectTypes(outputTypes, stableOutputChannels));

        // Compare row_number values against their logical scope, not against the payload row carrying them.
        // This preserves the check that numbers cannot move between partitions or peer groups.
        List<Integer> rowNumberScopeChannels = IntStream.concat(
                        Arrays.stream(partitionChannels),
                        Arrays.stream(orderChannels))
                .distinct()
                .boxed()
                .collect(toImmutableList());
        List<Integer> rowNumberChannels = ImmutableList.<Integer>builder()
                .addAll(rowNumberScopeChannels)
                .addAll(rowNumberOutputChannels)
                .build();
        assertSameDataWithoutOrder(
                projectChannels(actual, rowNumberChannels),
                projectChannels(expected, rowNumberChannels),
                projectTypes(outputTypes, rowNumberChannels));
    }

    private static List<Page> projectChannels(List<Page> pages, List<Integer> channels)
    {
        int[] channelArray = channels.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        return pages.stream()
                .map(page -> page.getColumns(channelArray))
                .collect(toImmutableList());
    }

    private static List<Type> projectTypes(List<Type> types, List<Integer> channels)
    {
        return channels.stream()
                .map(types::get)
                .collect(toImmutableList());
    }

    private static List<Page> executeGpuWindow(
            List<Page> inputPages,
            List<Type> sourceTypes,
            List<Type> outputTypes,
            List<GpuOperation.Factory> stages)
    {
        if (sourceTypes.isEmpty()) {
            return executeZeroColumnGpuWindow(inputPages, outputTypes, stages);
        }
        return executeGpuOperation(
                inputPages,
                sourceTypes,
                outputTypes,
                (context, source) -> createOperation(context, source, stages));
    }

    private static List<Page> executeZeroColumnGpuWindow(
            List<Page> inputPages,
            List<Type> outputTypes,
            List<GpuOperation.Factory> stages)
    {
        Iterator<Page> input = inputPages.iterator();
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        BufferPages bufferPages = new BufferPages(context.taskMemoryContext());
        GpuOperation operation = createOperation(context, bufferPages, stages);
        try (CopyToBlocks copyToBlocks = new CopyToBlocks(context, operation, outputTypes)) {
            return drainToPages(
                    () -> {
                        if (!input.hasNext()) {
                            bufferPages.noMoreInput();
                        }
                        else if (bufferPages.needsInput()) {
                            bufferPages.addInput(input.next());
                        }
                    },
                    copyToBlocks);
        }
    }

    private static List<Page> executeCpuWindow(
            List<Page> inputPages,
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<BoundTestedWindowFunction> functions)
    {
        List<Integer> outputChannels = IntStream.range(0, sourceTypes.size())
                .boxed()
                .collect(toImmutableList());
        ImmutableList.Builder<WindowFunctionDefinition> functionDefinitions = ImmutableList.builder();
        for (BoundTestedWindowFunction function : functions) {
            functionDefinitions.add(windowFunctionDefinition(function, sourceTypes));
        }
        WindowOperator.WindowOperatorFactory factory = TestWindowOperator.createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functionDefinitions.build(),
                Ints.asList(partitionChannels),
                ImmutableList.of(),
                Ints.asList(orderChannels),
                orderSortOrders,
                0,
                new DummySpillerFactory(),
                false);
        DriverContext driverContext = TestingTaskContext.builder(directExecutor(), SCHEDULED_EXECUTOR, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        return toPages(factory, driverContext, inputPages);
    }

    private static Block decimalBlock(DecimalType decimalType, Long... unscaledValues)
    {
        BlockBuilder builder = decimalType.createBlockBuilder(null, unscaledValues.length);
        for (Long value : unscaledValues) {
            if (value == null) {
                builder.appendNull();
            }
            else if (decimalType.isShort()) {
                decimalType.writeLong(builder, value);
            }
            else {
                decimalType.writeObject(builder, Int128.valueOf(BigInteger.valueOf(value)));
            }
        }
        return builder.build();
    }

    private static WindowFunctionDefinition windowFunctionDefinition(BoundTestedWindowFunction spec, List<Type> sourceTypes)
    {
        List<Type> argumentTypes = Arrays.stream(spec.argumentChannels())
                .mapToObj(sourceTypes::get)
                .collect(toImmutableList());
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(spec.name(), fromTypes(argumentTypes));
        FunctionManager functionManager = FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager();
        WindowFunctionSupplier supplier = switch (resolvedFunction.functionKind()) {
            case AGGREGATE -> new AggregationWindowFunctionSupplier(
                    resolvedFunction.signature(),
                    functionManager.getAggregationImplementation(resolvedFunction),
                    resolvedFunction.functionNullability());
            case WINDOW -> functionManager.getWindowFunctionSupplier(resolvedFunction);
            default -> throw new IllegalStateException("Unsupported function kind: " + resolvedFunction.functionKind());
        };
        return WindowFunctionDefinition.window(
                supplier,
                resolvedFunction.signature().getReturnType(),
                DEFAULT_FRAME,
                false,
                ImmutableList.of(),
                Arrays.stream(spec.argumentChannels()).boxed().toArray(Integer[]::new));
    }
}
