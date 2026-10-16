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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.slice.SliceInput;
import io.airlift.stats.TDigest;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.aggregation.AggregationMask;
import io.trino.operator.aggregation.AggregationTestUtils;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.gpu.aggregation.GpuAggregateFunction;
import io.trino.operator.gpu.aggregation.GpuAggregation;
import io.trino.operator.gpu.aggregation.GpuAggregationCompiler;
import io.trino.operator.gpu.aggregation.GpuCountNonNull;
import io.trino.operator.gpu.aggregation.GpuSum;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.FieldSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.IntStream;

import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.block.BlockAssertions.createDoubleRepeatBlock;
import static io.trino.block.BlockAssertions.getOnlyValue;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.FLOATING_POINT_EDGE_CASES;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataWithoutOrder;
import static io.trino.operator.gpu.GpuTestUtils.bigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.mergePages;
import static io.trino.operator.gpu.GpuTestUtils.writeFloatingPoint;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final int GROUP_KEY_CHANNEL = 0;
    // Index of the empty grouping set in ROLLUP(key), i.e. GROUPING SETS ((key), ())
    private static final int EMPTY_GROUPING_SET_ID = 1;
    // Small enough that the multi-batch tests compact incrementally (each ~100k-row batch exceeds it) and a
    // high-cardinality partial aggregation flushes, but large enough that low-cardinality and small inputs
    // aggregate into a single resident state without flushing.
    private static final long COMPACTION_THRESHOLD_BYTES = 256 * 1024;
    static final List<Type> APPROX_PERCENTILE_TYPES = List.of(BIGINT, DOUBLE, REAL);
    private static final List<Double> PERCENTILES = List.of(0.0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0);
    // Many more values than a t-digest has centroids
    private static final int APPROX_PERCENTILE_VALUE_COUNT = 5000;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testCountAllGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of());
        assertGlobalMatchesCpu(List.of(inputPage), "count", List.of(), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountAllGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "count", List.of(), SINGLE);
    }

    @Test
    void testGroupByCountAllEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BIGINT, BIGINT));
        assertGroupByMatchesCpu(List.of(inputPage), "count", List.of(), SINGLE);
    }

    @Test
    void testGroupByCountAll()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BIGINT, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "count", List.of(), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountNonNullGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "count", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testCountNonNullForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "count", List.of(type), SINGLE);
    }

    @Test
    void testCountNonNullGlobalFinal()
    {
        // Simulate merging partial COUNT(column) results: 10 + 20 + 5 = 35
        BlockBuilder countBuilder = BIGINT.createBlockBuilder(null, 3);
        BIGINT.writeLong(countBuilder, 10);
        BIGINT.writeLong(countBuilder, 20);
        BIGINT.writeLong(countBuilder, 5);

        Page inputPage = new Page(countBuilder.build());

        Object result = executeGpuGlobalFinalAggregation(
                inputPage,
                List.of(BIGINT),
                new GpuCountNonNull(0, BIGINT, INT64));

        assertThat(result).isEqualTo(35L);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByCountNonNullForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "count", List.of(type), SINGLE);
    }

    @Test
    void testGroupByCountNonNullFinal()
    {
        // Simulate merging partial COUNT(column) results:
        // Group 0: counts 10, 20 -> merged count 30
        // Group 1: counts 5, 15 -> merged count 20
        BlockBuilder groupByBuilder = BIGINT.createBlockBuilder(null, 4);
        BIGINT.writeLong(groupByBuilder, 0);
        BIGINT.writeLong(groupByBuilder, 0);
        BIGINT.writeLong(groupByBuilder, 1);
        BIGINT.writeLong(groupByBuilder, 1);

        BlockBuilder countBuilder = BIGINT.createBlockBuilder(null, 4);
        BIGINT.writeLong(countBuilder, 10);
        BIGINT.writeLong(countBuilder, 20);
        BIGINT.writeLong(countBuilder, 5);
        BIGINT.writeLong(countBuilder, 15);

        Page inputPage = new Page(groupByBuilder.build(), countBuilder.build());

        List<Page> results = executeGpuFinalAggregation(
                inputPage,
                BIGINT,
                BIGINT,
                new GpuCountNonNull(1, BIGINT, INT64));

        assertSameDataInOrder(
                results,
                rowPagesBuilder(BIGINT, BIGINT)
                        .row(0L, 30L)
                        .row(1L, 20L)
                        .build(),
                List.of(BIGINT, BIGINT));
    }

    @Test
    void testSumGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BIGINT));
        assertGlobalMatchesCpu(List.of(inputPage), "sum", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSumGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 1, 10);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "sum", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testSumForAllTypes(Type type)
    {
        if (type != BIGINT && type != DOUBLE && type != REAL) {
            assertCompileNotSupported("sum", List.of(type), false, SINGLE);
            return;
        }
        Block block = createInputBlockForSum(type, 100);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "sum", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupBySumForAllTypes(Type type)
    {
        if (type != BIGINT && type != DOUBLE && type != REAL) {
            assertCompileNotSupported("sum", List.of(type), true, SINGLE);
            return;
        }
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createInputBlockForSum(type, 100);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "sum", List.of(type), SINGLE);
    }

    private static Block createInputBlockForSum(Type type, int positionCount)
    {
        if (type == BIGINT) {
            return createBigintBlock(positionCount, RANDOM_NULLS, -10000, 10001); // Small range to avoid overflow
        }
        return createBlock(type, positionCount, RANDOM_NULLS);
    }

    @Test
    void testMinGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BIGINT));
        assertGlobalMatchesCpu(List.of(inputPage), "min", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMinGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "min", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testMinForAllTypes(Type type)
    {
        if (type == VARBINARY) {
            assertCompileNotSupported("min", List.of(type), false);
            return;
        }
        Block block = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "min", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByMinForAllTypes(Type type)
    {
        if (type == VARBINARY) {
            assertCompileNotSupported("min", List.of(type), true);
            return;
        }
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "min", List.of(type), SINGLE);
    }

    @Test
    void testMaxGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BIGINT));
        assertGlobalMatchesCpu(List.of(inputPage), "max", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMaxGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "max", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testMaxForAllTypes(Type type)
    {
        if (type == VARBINARY) {
            assertCompileNotSupported("max", List.of(type), false);
            return;
        }
        Block block = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "max", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByMaxForAllTypes(Type type)
    {
        if (type == VARBINARY) {
            assertCompileNotSupported("max", List.of(type), true);
            return;
        }
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "max", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_FLOATING_POINT_TYPES")
    void testMinFloatingPointNaN(Type type)
    {
        for (List<Double> group : FLOATING_POINT_EDGE_CASES) {
            Page inputPage = new Page(createFloatingPointBlock(type, group));
            assertGlobalMatchesCpu(List.of(inputPage), "min", List.of(type), SINGLE);
        }
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_FLOATING_POINT_TYPES")
    void testMaxFloatingPointNaN(Type type)
    {
        for (List<Double> group : FLOATING_POINT_EDGE_CASES) {
            Page inputPage = new Page(createFloatingPointBlock(type, group));
            assertGlobalMatchesCpu(List.of(inputPage), "max", List.of(type), SINGLE);
        }
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_FLOATING_POINT_TYPES")
    void testGroupByMinFloatingPointNaN(Type type)
    {
        Page inputPage = createGroupByFloatingPointNanPage(type);
        assertGroupByMatchesCpu(List.of(inputPage), "min", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_FLOATING_POINT_TYPES")
    void testGroupByMaxFloatingPointNaN(Type type)
    {
        Page inputPage = createGroupByFloatingPointNanPage(type);
        assertGroupByMatchesCpu(List.of(inputPage), "max", List.of(type), SINGLE);
    }

    private static Page createGroupByFloatingPointNanPage(Type type)
    {
        int totalRows = FLOATING_POINT_EDGE_CASES.stream().mapToInt(List::size).sum();
        BlockBuilder groupBuilder = BIGINT.createBlockBuilder(null, totalRows);
        BlockBuilder valueBuilder = type.createBlockBuilder(null, totalRows);
        for (int groupId = 0; groupId < FLOATING_POINT_EDGE_CASES.size(); groupId++) {
            for (Double value : FLOATING_POINT_EDGE_CASES.get(groupId)) {
                BIGINT.writeLong(groupBuilder, groupId);
                if (value == null) {
                    valueBuilder.appendNull();
                }
                else {
                    writeFloatingPoint(valueBuilder, type, value);
                }
            }
        }
        return new Page(groupBuilder.build(), valueBuilder.build());
    }

    @Test
    void testMinMaxCharPaddedSemantics()
    {
        // U+0001 is less than space (U+0020), so naive byte comparison would produce wrong result without proper CHAR padding
        Type charType = createCharType(2);
        BlockBuilder valueBuilder = charType.createBlockBuilder(null, 4);
        for (String value : List.of("a", "a", "a\u0001", "a\u0001")) {
            charType.writeSlice(valueBuilder, utf8Slice(value));
        }
        Page inputPage = new Page(valueBuilder.build());
        assertGlobalMatchesCpu(List.of(inputPage), "min", List.of(charType), SINGLE);
        assertGlobalMatchesCpu(List.of(inputPage), "max", List.of(charType), SINGLE);
    }

    @Test
    void testGroupByMinMaxCharPaddedSemantics()
    {
        // U+0001 is less than space (U+0020), so naive byte comparison would produce wrong result without proper CHAR padding
        Type charType = createCharType(2);
        BlockBuilder groupByBuilder = BIGINT.createBlockBuilder(null, 4);
        BlockBuilder valueBuilder = charType.createBlockBuilder(null, 4);
        for (String value : List.of("a", "a", "a\u0001", "a\u0001")) {
            BIGINT.writeLong(groupByBuilder, 0);
            charType.writeSlice(valueBuilder, utf8Slice(value));
        }
        Page inputPage = new Page(groupByBuilder.build(), valueBuilder.build());
        assertGroupByMatchesCpu(List.of(inputPage), "min", List.of(charType), SINGLE);
        assertGroupByMatchesCpu(List.of(inputPage), "max", List.of(charType), SINGLE);
    }

    @Test
    void testGroupByMultipleBatchesCountAll()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGroupByMatchesCpu(inputPages, "count", List.of(), SINGLE);
    }

    @Test
    void testGroupByMultipleBatchesCountNonNull()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGroupByMatchesCpu(inputPages, "count", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGroupByMultipleBatchesSum()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGroupByMatchesCpu(inputPages, "sum", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGroupByMultipleBatchesMin()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "min", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGroupByMultipleBatchesMax()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "max", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesCountAll()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of(), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesCountNonNull()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesSum()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGlobalMatchesCpu(inputPages, "sum", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesMin()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "min", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesMax()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "max", List.of(BIGINT), SINGLE);
    }

    @Test
    void testGlobalMultipleBatchesCountAllPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of(), PARTIAL);
    }

    @Test
    void testGlobalMultipleBatchesCountNonNullPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGlobalMultipleBatchesSumPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGlobalMatchesCpu(inputPages, "sum", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGlobalMultipleBatchesMinPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "min", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGlobalMultipleBatchesMaxPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "max", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGroupByMultipleBatchesCountAllPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGroupByMatchesCpu(inputPages, "count", List.of(), PARTIAL);
    }

    @Test
    void testGroupByMultipleBatchesSumPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGroupByMatchesCpu(inputPages, "sum", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGroupByMultipleBatchesMinPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "min", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGroupByMultipleBatchesMaxPartial()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "max", List.of(BIGINT), PARTIAL);
    }

    @Test
    void testGroupByHighCardinalityPartialFlushes()
    {
        // Many distinct groups so the compacted partial state grows past COMPACTION_THRESHOLD_BYTES and the
        // partial aggregation flushes mid-stream, spreading a group's intermediate across multiple output pages.
        int groupCount = 60_000;
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, groupCount), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, groupCount), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, groupCount), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        List<Page> results = assertGroupByMatchesCpu(inputPages, "sum", List.of(BIGINT), PARTIAL);
        assertThat(results).as("Expected the partial aggregation to flush multiple pages").hasSizeGreaterThan(1);
    }

    @Test
    void testBoolOrGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BOOLEAN));
        assertGlobalMatchesCpu(List.of(inputPage), "bool_or", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testBoolOrGlobal(NullsProvider nullsProvider)
    {
        Block block = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "bool_or", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testGroupByBoolOr(NullsProvider nullsProvider)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "bool_or", List.of(BOOLEAN), SINGLE);
    }

    @Test
    void testBoolAndGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BOOLEAN));
        assertGlobalMatchesCpu(List.of(inputPage), "bool_and", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testBoolAndGlobal(NullsProvider nullsProvider)
    {
        Block block = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "bool_and", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testGroupByBoolAnd(NullsProvider nullsProvider)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "bool_and", List.of(BOOLEAN), SINGLE);
    }

    @Test
    void testCountIfGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BOOLEAN));
        assertGlobalMatchesCpu(List.of(inputPage), "count_if", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountIfGlobal(NullsProvider nullsProvider)
    {
        Block block = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "count_if", List.of(BOOLEAN), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testGroupByCountIf(NullsProvider nullsProvider)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, nullsProvider);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "count_if", List.of(BOOLEAN), SINGLE);
    }

    @Test
    void testMaskedCountAllGlobal()
    {
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, 0, 100);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "count", List.of());
    }

    @Test
    void testMaskedCountNonNullGlobal()
    {
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, 0, 100);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "count", List.of(BIGINT));
    }

    @Test
    void testMaskedCountIfGlobal()
    {
        Block valueBlock = createBlock(BOOLEAN, 100, RANDOM_NULLS);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "count_if", List.of(BOOLEAN));
    }

    @Test
    void testMaskedSumBigintGlobal()
    {
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, 1, 10);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "sum", List.of(BIGINT));
    }

    @Test
    void testMaskedSumDoubleGlobal()
    {
        Block valueBlock = createBlock(DOUBLE, 100, RANDOM_NULLS);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "sum", List.of(DOUBLE));
    }

    @Test
    void testMaskedMinGlobal()
    {
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, -1000, 1000);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "min", List.of(BIGINT));
    }

    @Test
    void testMaskedMaxGlobal()
    {
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, -1000, 1000);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGlobalMatchesCpu(new Page(valueBlock, maskBlock), "max", List.of(BIGINT));
    }

    @Test
    void testGroupByMaskedSum()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, 1, 10);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGroupByMatchesCpu(new Page(groupByBlock, valueBlock, maskBlock), "sum", List.of(BIGINT));
    }

    @Test
    void testGroupByMaskedCountAll()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBigintBlock(100, RANDOM_NULLS, 0, 100);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGroupByMatchesCpu(new Page(groupByBlock, valueBlock, maskBlock), "count", List.of());
    }

    @Test
    void testGroupByMaskedCountIf()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, RANDOM_NULLS);
        Block maskBlock = createMaskBlock(100);
        assertMaskedGroupByMatchesCpu(new Page(groupByBlock, valueBlock, maskBlock), "count_if", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAnyValueGlobalEmpty(Type type)
    {
        Page inputPage = createEmptyPage(List.of(type));
        assertGlobalMatchesCpu(List.of(inputPage), "any_value", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAnyValueGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "any_value", List.of(BIGINT), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAnyValueForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(block);
        assertGlobalMatchesCpu(List.of(inputPage), "any_value", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByAnyValueForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);
        assertGroupByMatchesCpu(List.of(inputPage), "any_value", List.of(type), SINGLE);
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentileGlobalEmpty(Type type)
    {
        Page inputPage = createEmptyPage(List.of(type, DOUBLE));
        for (Step step : List.of(SINGLE, PARTIAL)) {
            assertGlobalMatchesCpu(List.of(inputPage), "approx_percentile", List.of(type, DOUBLE), step);
        }
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentileAllNull(Type type)
    {
        Page inputPage = new Page(nullBlock(type, 3), createDoubleRepeatBlock(0.5, 3));
        for (Step step : List.of(SINGLE, PARTIAL)) {
            assertGlobalMatchesCpu(List.of(inputPage), "approx_percentile", List.of(type, DOUBLE), step);
        }
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentileSingleValue(Type type)
    {
        // A t-digest of one value answers that value for every percentile
        Block valueBlock = createApproxPercentileValueBlock(type, 1, false);
        for (double percentile : PERCENTILES) {
            Page inputPage = new Page(valueBlock, createDoubleRepeatBlock(percentile, 1));
            for (Step step : List.of(SINGLE, PARTIAL)) {
                assertGlobalMatchesCpu(List.of(inputPage), "approx_percentile", List.of(type, DOUBLE), step);
            }
        }
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentileGlobal(Type type)
    {
        Block valueBlock = createApproxPercentileValueBlock(type, APPROX_PERCENTILE_VALUE_COUNT, true);
        List<Double> values = nonNullValues(type, valueBlock);
        for (double percentile : PERCENTILES) {
            Page inputPage = new Page(valueBlock, createDoubleRepeatBlock(percentile, APPROX_PERCENTILE_VALUE_COUNT));
            for (Step step : List.of(SINGLE, PARTIAL)) {
                assertApproxPercentile(type, values, percentile, runGpuApproxPercentileGlobal(type, List.of(inputPage), step));
            }
        }
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testGroupByApproxPercentile(Type type)
    {
        int groupCount = 5;
        int positionCount = groupCount * APPROX_PERCENTILE_VALUE_COUNT;
        Block groupByBlock = createGroupByBlock(positionCount, groupCount);
        Block valueBlock = createApproxPercentileValueBlock(type, positionCount, true);
        for (double percentile : PERCENTILES) {
            Page inputPage = new Page(groupByBlock, valueBlock, createDoubleRepeatBlock(percentile, positionCount));
            Map<Long, List<Double>> valuesByGroup = nonNullValuesByGroup(type, List.of(inputPage));
            for (Step step : List.of(SINGLE, PARTIAL)) {
                assertApproxPercentiles(type, valuesByGroup, _ -> percentile, runGpuApproxPercentileGrouped(type, List.of(inputPage), step));
            }
        }
    }

    @Test
    void testGroupByApproxPercentileWithPercentilePerGroup()
    {
        int groupCount = 5;
        int positionCount = groupCount * APPROX_PERCENTILE_VALUE_COUNT;
        Block groupByBlock = createGroupByBlock(positionCount, groupCount);
        BlockBuilder percentileBuilder = DOUBLE.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            DOUBLE.writeDouble(percentileBuilder, 0.2 * BIGINT.getLong(groupByBlock, position));
        }
        Page inputPage = new Page(groupByBlock, createApproxPercentileValueBlock(DOUBLE, positionCount, true), percentileBuilder.build());
        Map<Long, List<Double>> valuesByGroup = nonNullValuesByGroup(DOUBLE, List.of(inputPage));
        for (Step step : List.of(SINGLE, PARTIAL)) {
            assertApproxPercentiles(DOUBLE, valuesByGroup, group -> 0.2 * group, runGpuApproxPercentileGrouped(DOUBLE, List.of(inputPage), step));
        }
    }

    @Test
    void testApproxPercentileSkipsRowsWithNullPercentile()
    {
        // The value on a row with a NULL percentile is neither aggregated nor validated
        Block valueBlock = createFloatingPointBlock(DOUBLE, Arrays.asList(Double.NaN, 1.0, 2.0, 3.0, 4.0, Double.POSITIVE_INFINITY, 5.0, null));
        Block percentileBlock = createFloatingPointBlock(DOUBLE, Arrays.asList(null, 0.5, 0.5, null, 0.5, null, 0.5, 0.5));
        Page inputPage = new Page(valueBlock, percentileBlock);
        for (Step step : List.of(SINGLE, PARTIAL)) {
            assertApproxPercentile(DOUBLE, List.of(1.0, 2.0, 4.0, 5.0), 0.5, runGpuApproxPercentileGlobal(DOUBLE, List.of(inputPage), step));
        }
    }

    @Test
    void testGroupByMaskedApproxPercentile()
    {
        int groupCount = 5;
        int positionCount = groupCount * APPROX_PERCENTILE_VALUE_COUNT;
        Page inputPage = new Page(
                createGroupByBlock(positionCount, groupCount),
                createApproxPercentileValueBlock(DOUBLE, positionCount, true),
                createDoubleRepeatBlock(0.75, positionCount),
                createMaskBlock(positionCount));
        Map<Long, List<Double>> valuesByGroup = nonNullValuesByGroup(DOUBLE, List.of(filterByMask(inputPage)));

        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation("approx_percentile", List.of(DOUBLE, DOUBLE), true, SINGLE, true);
        List<Page> results = runGpuPipeline(List.of(inputPage), List.of(BIGINT, DOUBLE, DOUBLE, BOOLEAN), compiled);
        assertApproxPercentiles(DOUBLE, valuesByGroup, _ -> 0.75, groupedResults(DOUBLE, results));
    }

    @ParameterizedTest
    @EnumSource(value = Step.class, names = {"SINGLE", "PARTIAL"})
    void testGlobalMultipleBatchesApproxPercentile(Step step)
    {
        // Large enough to compact incrementally
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000), createDoubleRepeatBlock(0.3, TARGET_ROW_COUNT)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -2000, 0), createDoubleRepeatBlock(0.3, TARGET_ROW_COUNT)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 0, 3000), createDoubleRepeatBlock(0.3, TARGET_ROW_COUNT)));
        List<Double> values = new ArrayList<>();
        for (Page inputPage : inputPages) {
            values.addAll(nonNullValues(BIGINT, inputPage.getBlock(0)));
        }
        assertApproxPercentile(BIGINT, values, 0.3, runGpuApproxPercentileGlobal(BIGINT, inputPages, step));
    }

    @ParameterizedTest
    @EnumSource(value = Step.class, names = {"SINGLE", "PARTIAL"})
    void testGroupByMultipleBatchesApproxPercentile(Step step)
    {
        // Large enough to compact incrementally
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000), createDoubleRepeatBlock(0.9, TARGET_ROW_COUNT)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -2000, 0), createDoubleRepeatBlock(0.9, TARGET_ROW_COUNT)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 0, 3000), createDoubleRepeatBlock(0.9, TARGET_ROW_COUNT)));
        assertApproxPercentiles(BIGINT, nonNullValuesByGroup(BIGINT, inputPages), _ -> 0.9, runGpuApproxPercentileGrouped(BIGINT, inputPages, step));
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentilePartialStateIsCpuState(Type type)
    {
        // The GPU partial state deserializes as an airlift t-digest of all values
        Block valueBlock = createApproxPercentileValueBlock(type, APPROX_PERCENTILE_VALUE_COUNT, true);
        Page inputPage = new Page(valueBlock, createDoubleRepeatBlock(0.4, APPROX_PERCENTILE_VALUE_COUNT));
        List<Double> values = nonNullValues(type, valueBlock);

        SliceInput state = VARBINARY.getSlice(runGpuPartialGlobal("approx_percentile", List.of(type, DOUBLE), inputPage), 0).getInput();
        assertThat(state.readDouble()).isEqualTo(0.4);
        TDigest digest = TDigest.deserialize(state.readSlice(state.readInt()));
        assertThat(state.isReadable()).isFalse();
        assertThat(digest.getCount()).isEqualTo(values.size());
        assertThat(digest.getMin()).isEqualTo(Collections.min(values));
        assertThat(digest.getMax()).isEqualTo(Collections.max(values));
        assertWithinRankError(values, 0.4, digest.valueAt(0.4));
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentilePartialFinalGlobal(Type type)
    {
        // Each of the CPU and the GPU merges the partial states of either
        List<Page> inputPages = List.of(
                new Page(createApproxPercentileValueBlock(type, APPROX_PERCENTILE_VALUE_COUNT, true), createDoubleRepeatBlock(0.6, APPROX_PERCENTILE_VALUE_COUNT)),
                new Page(createApproxPercentileValueBlock(type, 1, false), createDoubleRepeatBlock(0.6, 1)),
                new Page(nullBlock(type, 3), createDoubleRepeatBlock(0.6, 3)));
        List<Double> values = new ArrayList<>();
        for (Page inputPage : inputPages) {
            values.addAll(nonNullValues(type, inputPage.getBlock(0)));
        }

        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(type, DOUBLE)).getIntermediateType();
        BlockBuilder cpuStates = intermediateType.createBlockBuilder(null, inputPages.size());
        BlockBuilder gpuStates = intermediateType.createBlockBuilder(null, inputPages.size());
        BlockBuilder mixedStates = intermediateType.createBlockBuilder(null, inputPages.size());
        for (int i = 0; i < inputPages.size(); i++) {
            Block cpuState = runCpuPartialGlobal("approx_percentile", List.of(type, DOUBLE), inputPages.get(i));
            Block gpuState = runGpuPartialGlobal("approx_percentile", List.of(type, DOUBLE), inputPages.get(i));
            cpuStates.appendBlockRange(cpuState, 0, 1);
            gpuStates.appendBlockRange(gpuState, 0, 1);
            mixedStates.appendBlockRange(i % 2 == 0 ? cpuState : gpuState, 0, 1);
        }
        for (Block states : List.of(cpuStates.build(), gpuStates.build(), mixedStates.build())) {
            assertApproxPercentile(type, values, 0.6, runCpuFinal("approx_percentile", List.of(type, DOUBLE), states));
            assertApproxPercentile(type, values, 0.6, runGpuFinalGlobal("approx_percentile", List.of(type, DOUBLE), states, intermediateType));
        }

        Block nullStates = runCpuPartialGlobal("approx_percentile", List.of(type, DOUBLE), inputPages.getLast());
        assertThat(runGpuFinalGlobal("approx_percentile", List.of(type, DOUBLE), nullStates, intermediateType)).isNull();
    }

    @ParameterizedTest
    @FieldSource("APPROX_PERCENTILE_TYPES")
    void testApproxPercentilePartialFinalGrouped(Type type)
    {
        int groupCount = 5;
        int positionCount = groupCount * APPROX_PERCENTILE_VALUE_COUNT;
        Page rawInput = new Page(
                createGroupByBlock(positionCount, groupCount),
                createApproxPercentileValueBlock(type, positionCount, true),
                createDoubleRepeatBlock(0.1, positionCount));
        Map<Long, List<Double>> valuesByGroup = nonNullValuesByGroup(type, List.of(rawInput));
        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(type, DOUBLE)).getIntermediateType();

        Page cpuPartial = runCpuPartialGrouped("approx_percentile", List.of(type, DOUBLE), rawInput, intermediateType);
        Page gpuPartial = runGpuPartialGrouped("approx_percentile", List.of(type, DOUBLE), rawInput, intermediateType);
        for (Page partial : List.of(cpuPartial, gpuPartial)) {
            assertApproxPercentiles(type, valuesByGroup, _ -> 0.1, runCpuFinalGrouped("approx_percentile", List.of(type, DOUBLE), partial));
            assertApproxPercentiles(type, valuesByGroup, _ -> 0.1, runGpuFinalGrouped("approx_percentile", List.of(type, DOUBLE), partial, intermediateType));
        }
    }

    @Test
    void testApproxPercentileFailures()
    {
        assertApproxPercentileFails(DOUBLE, createFloatingPointBlock(DOUBLE, Arrays.asList(1.0, null, Double.NaN, Double.POSITIVE_INFINITY)), 0.5, "value must be finite; was NaN");
        assertApproxPercentileFails(DOUBLE, createFloatingPointBlock(DOUBLE, Arrays.asList(1.0, Double.POSITIVE_INFINITY, Double.NaN)), 0.5, "value must be finite; was Infinity");
        assertApproxPercentileFails(REAL, createFloatingPointBlock(REAL, Arrays.asList(null, Double.NEGATIVE_INFINITY, 1.0)), 0.5, "value must be finite; was -Infinity");
        // 2^53 + 1 is the smallest positive long without an exact double representation
        assertApproxPercentileFails(BIGINT, bigintBlock(1L, null, (1L << 53) + 1, Long.MAX_VALUE - 1), 0.5, "no exact double representation for long: 9007199254740993");
        assertApproxPercentileFails(BIGINT, bigintBlock(1L, Long.MAX_VALUE - 1), 0.5, "no exact double representation for long: 9223372036854775806");
        assertApproxPercentileFails(BIGINT, bigintBlock(1L, 2L), 1.5, "Percentile must be between 0 and 1");
        assertApproxPercentileFails(BIGINT, bigintBlock(1L, 2L), -0.1, "Percentile must be between 0 and 1");
        assertApproxPercentileFails(BIGINT, bigintBlock(1L, 2L), Double.NaN, "Percentile must be between 0 and 1");
    }

    @Test
    void testApproxPercentileLongExtremes()
    {
        // Long.MAX_VALUE rounds to 2^63, which converts back to Long.MAX_VALUE
        for (long value : List.of(Long.MIN_VALUE, Long.MAX_VALUE, (1L << 53) + 2)) {
            Page inputPage = new Page(bigintBlock(value), createDoubleRepeatBlock(0.5, 1));
            for (Step step : List.of(SINGLE, PARTIAL)) {
                assertGlobalMatchesCpu(List.of(inputPage), "approx_percentile", List.of(BIGINT, DOUBLE), step);
            }
        }
    }

    @Test
    void testApproxPercentileNotSupported()
    {
        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(BIGINT, DOUBLE)).getIntermediateType();
        assertThat(tryCompileAggregation("approx_percentile", List.of(BIGINT, DOUBLE), List.of(intermediateType), false, INTERMEDIATE, false))
                .isInstanceOf(GpuAggregationCompiler.AggregationCompileResult.Failure.class);
        for (Step step : List.of(SINGLE, PARTIAL)) {
            // Weighted
            assertCompileNotSupported("approx_percentile", List.of(DOUBLE, DOUBLE, DOUBLE), false, step);
            // Weighted, with accuracy
            assertCompileNotSupported("approx_percentile", List.of(DOUBLE, DOUBLE, DOUBLE, DOUBLE), false, step);
            // Array of percentiles
            assertCompileNotSupported("approx_percentile", List.of(DOUBLE, new ArrayType(DOUBLE)), false, step);
        }
    }

    private void assertApproxPercentileFails(Type type, Block valueBlock, double percentile, String expectedMessage)
    {
        List<Type> argumentTypes = List.of(type, DOUBLE);
        Page inputPage = new Page(valueBlock, createDoubleRepeatBlock(percentile, valueBlock.getPositionCount()));

        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(argumentTypes));
        assertTrinoExceptionThrownBy(() -> AggregationTestUtils.aggregation(function, inputPage))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage(expectedMessage);

        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation("approx_percentile", argumentTypes, false, SINGLE, false);
        assertTrinoExceptionThrownBy(() -> runGpuPipeline(List.of(inputPage), argumentTypes, compiled))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage(expectedMessage);
    }

    /// The result of a global `approx_percentile`, through a CPU FINAL step for PARTIAL
    private Object runGpuApproxPercentileGlobal(Type type, List<Page> inputPages, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation("approx_percentile", List.of(type, DOUBLE), false, step, false);
        List<Page> results = runGpuPipeline(inputPages, List.of(type, DOUBLE), compiled);
        if (step == PARTIAL) {
            Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(type, DOUBLE)).getIntermediateType();
            return runCpuFinal("approx_percentile", List.of(type, DOUBLE), mergePages(results, List.of(intermediateType)).getBlock(0));
        }
        return getOnlyValue(type, getOnlyElement(results).getBlock(0));
    }

    /// The results of a grouped `approx_percentile`, through a CPU FINAL step for PARTIAL
    private Map<Object, Object> runGpuApproxPercentileGrouped(Type type, List<Page> inputPages, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation("approx_percentile", List.of(type, DOUBLE), true, step, false);
        List<Page> results = runGpuPipeline(inputPages, List.of(BIGINT, type, DOUBLE), compiled);
        if (step != PARTIAL) {
            return groupedResults(type, results);
        }
        // A partial aggregation may flush the state of a group more than once
        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction("approx_percentile", fromTypes(type, DOUBLE)).getIntermediateType();
        Map<Object, BlockBuilder> statesByGroup = new HashMap<>();
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                statesByGroup.computeIfAbsent(BIGINT.getObjectValue(page.getBlock(0), position), _ -> intermediateType.createBlockBuilder(null, results.size()))
                        .appendBlockRange(page.getBlock(1), position, 1);
            }
        }
        Map<Object, Object> result = new HashMap<>();
        statesByGroup.forEach((group, states) -> result.put(group, runCpuFinal("approx_percentile", List.of(type, DOUBLE), states.build())));
        return result;
    }

    private static Map<Object, Object> groupedResults(Type type, List<Page> results)
    {
        Map<Object, Object> result = new HashMap<>();
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                verify(result.put(BIGINT.getObjectValue(page.getBlock(0), position), type.getObjectValue(page.getBlock(1), position)) == null);
            }
        }
        return result;
    }

    /// The values aggregated by `approx_percentile(channel 1, channel 2)`, by the group in channel 0
    private static Map<Long, List<Double>> nonNullValuesByGroup(Type type, List<Page> inputPages)
    {
        Map<Long, List<Double>> valuesByGroup = new HashMap<>();
        for (Page inputPage : inputPages) {
            Block valueBlock = inputPage.getBlock(1);
            Block percentileBlock = inputPage.getBlock(2);
            for (int position = 0; position < inputPage.getPositionCount(); position++) {
                if (!valueBlock.isNull(position) && !percentileBlock.isNull(position)) {
                    valuesByGroup.computeIfAbsent(BIGINT.getLong(inputPage.getBlock(0), position), _ -> new ArrayList<>())
                            .add(toDouble(type, valueBlock, position));
                }
            }
        }
        return valuesByGroup;
    }

    private static List<Double> nonNullValues(Type type, Block block)
    {
        List<Double> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                values.add(toDouble(type, block, position));
            }
        }
        return values;
    }

    private static double toDouble(Type type, Block block, int position)
    {
        if (type == BIGINT) {
            return BIGINT.getLong(block, position);
        }
        if (type == REAL) {
            return REAL.getFloat(block, position);
        }
        return DOUBLE.getDouble(block, position);
    }

    private static void assertApproxPercentiles(Type type, Map<Long, List<Double>> valuesByGroup, LongFunction<Double> percentileOfGroup, Map<Object, Object> results)
    {
        assertThat(results.keySet()).isEqualTo(ImmutableSet.copyOf(valuesByGroup.keySet()));
        valuesByGroup.forEach((group, values) -> assertApproxPercentile(type, values, percentileOfGroup.apply(group), results.get(group)));
    }

    /// @param actual the result as of {@link Type#getObjectValue}
    private static void assertApproxPercentile(Type type, List<Double> values, double percentile, Object actual)
    {
        if (values.isEmpty()) {
            assertThat(actual).isNull();
            return;
        }
        double value;
        if (type == BIGINT) {
            value = (long) actual;
        }
        else if (type == REAL) {
            value = (float) actual;
        }
        else {
            verify(type == DOUBLE, "Unexpected type: %s", type);
            value = (double) actual;
        }
        assertWithinRankError(values, percentile, value);
    }

    /// The result must lie between the values at the ranks one percent of the value count away, inclusive, which
    /// t-digests with the default compression of 100 meet with ease for the value counts of these tests
    private static void assertWithinRankError(List<Double> values, double percentile, double actual)
    {
        List<Double> sorted = values.stream().sorted().collect(toImmutableList());
        int lastRank = sorted.size() - 1;
        double rank = percentile * lastRank;
        double maxRankError = 0.01 * sorted.size() + 1;
        double low = sorted.get((int) Math.max(0, Math.floor(rank - maxRankError)));
        double high = sorted.get((int) Math.min(lastRank, Math.ceil(rank + maxRankError)));
        // A REAL or BIGINT result is a rounded double
        double tolerance = Math.max(1, Math.ulp((float) Math.max(Math.abs(low), Math.abs(high))));
        assertThat(actual)
                .as("percentile %s of %s values", percentile, values.size())
                .isBetween(low - tolerance, high + tolerance);
    }

    private static Block createApproxPercentileValueBlock(Type type, int positionCount, boolean withNulls)
    {
        Random random = new Random(42);
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            // Normally distributed, with duplicates
            long value = Math.round(random.nextGaussian() * 1000);
            if (withNulls && random.nextInt(5) == 0) {
                builder.appendNull();
            }
            else if (type == BIGINT) {
                BIGINT.writeLong(builder, value);
            }
            else {
                // Exact in both REAL and DOUBLE
                writeFloatingPoint(builder, type, value / 4.0);
            }
        }
        return builder.build();
    }

    private static Block nullBlock(Type type, int positionCount)
    {
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            builder.appendNull();
        }
        return builder.build();
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testCountPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("count", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testCountIfPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("count_if", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testSumPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("sum", type, sumIntermediateMayDiffer(type));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testMinPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("min", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testMaxPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("max", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAvgPartialFinalGlobalForAllTypes(Type type)
    {
        // avg(decimal) uses VARBINARY intermediates with different CPU/GPU layouts.
        runPartialFinalGlobal("avg", type, type instanceof DecimalType);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAnyValuePartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("any_value", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testBoolAndPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("bool_and", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testBoolOrPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("bool_or", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByCountPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("count", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByCountIfPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("count_if", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupBySumPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("sum", type, sumIntermediateMayDiffer(type));
    }

    private static boolean sumIntermediateMayDiffer(Type type)
    {
        // sum(decimal): CPU emits variable-length rows, GPU a uniform 16-byte layout.
        // sum(double|real): floating-point sum is non-associative, so CPU sequential and GPU parallel
        //         reductions may produce different last-bit-level bytes that still round to the same
        //         final value within the verifier's tolerance.
        return type instanceof DecimalType || type == DOUBLE || type == REAL;
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByMinPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("min", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByMaxPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("max", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByAvgPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("avg", type, type instanceof DecimalType);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByAnyValuePartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("any_value", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByBoolAndPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("bool_and", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByBoolOrPartialFinalForAllTypes(Type type)
    {
        runPartialFinalGrouped("bool_or", type, false);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupingSetsDefaultOutputForAllKeyTypes(Type keyType)
    {
        // ROLLUP(key) is the grouping sets ((key), ()). The empty one is a global aggregation, so an
        // input with no rows still has a result for it: a null key, that grouping set's id, and count 0.
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileRollupCount(keyType, SINGLE);
        List<Type> inputTypes = List.of(keyType, BIGINT);

        List<Page> results = runGpuPipeline(List.of(createEmptyPage(inputTypes)), inputTypes, compiled);

        Page result = mergePages(results, compiled.finalOutputTypes());
        assertThat(result.getPositionCount()).isEqualTo(1);
        assertThat(result.getBlock(0).isNull(0))
                .as("grouping key must be null in the default output")
                .isTrue();
        assertThat(BIGINT.getLong(result.getBlock(1), 0))
                .as("groupId must identify the empty grouping set")
                .isEqualTo(EMPTY_GROUPING_SET_ID);
        assertThat(BIGINT.getLong(result.getBlock(2), 0)).isEqualTo(0);
    }

    @Test
    void testGroupingSetsWithoutEmptySetHasNoDefaultOutput()
    {
        // GROUPING SETS ((key), (key)) has no empty grouping set, so nothing is a global aggregation and
        // an input with no rows produces no row at all.
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileGroupingSetsCount(BIGINT, 2, ImmutableSet.of(), SINGLE);
        List<Type> inputTypes = List.of(BIGINT, BIGINT);

        List<Page> results = runGpuPipeline(List.of(createEmptyPage(inputTypes)), inputTypes, compiled);

        assertThat(results.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(0);
    }

    @Test
    void testGroupingSetsDefaultOutputPerEmptyGroupingSet()
    {
        // GROUPING SETS ((key), (), ()) has two empty grouping sets, and each one is its own global
        // aggregation, so the default output carries one row per id.
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileGroupingSetsCount(BIGINT, 3, ImmutableSet.of(1, 2), SINGLE);
        List<Type> inputTypes = List.of(BIGINT, BIGINT);

        List<Page> results = runGpuPipeline(List.of(createEmptyPage(inputTypes)), inputTypes, compiled);

        Page result = mergePages(results, compiled.finalOutputTypes());
        assertThat(result.getPositionCount()).isEqualTo(2);
        List<Long> groupIds = new ArrayList<>();
        for (int position = 0; position < result.getPositionCount(); position++) {
            assertThat(result.getBlock(0).isNull(position)).isTrue();
            groupIds.add(BIGINT.getLong(result.getBlock(1), position));
            assertThat(BIGINT.getLong(result.getBlock(2), position)).isEqualTo(0);
        }
        assertThat(groupIds).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void testFinalStepOfGroupingSetsHasNoDefaultOutput()
    {
        // Only a step that outputs partial state, or SINGLE, produces the default output; the FINAL step
        // merges the rows its PARTIAL peers already emitted for the empty grouping set.
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileRollupCount(BIGINT, FINAL);
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT);

        List<Page> results = runGpuPipeline(List.of(createEmptyPage(inputTypes)), inputTypes, compiled);

        assertThat(results.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(0);
    }

    private static GpuAggregationCompiler.AggregationCompileResult.Success compileRollupCount(Type keyType, Step step)
    {
        return compileGroupingSetsCount(keyType, 2, ImmutableSet.of(EMPTY_GROUPING_SET_ID), step);
    }

    /**
     * Compiles {@code count(*)} over the aggregation Trino plans for {@code GROUPING SETS}: a group-by on
     * the grouping key plus the groupId key that a {@code GroupIdNode} adds. Channel 0 is the key,
     * channel 1 the groupId.
     */
    private static GpuAggregationCompiler.AggregationCompileResult.Success compileGroupingSetsCount(
            Type keyType,
            int groupingSetCount,
            Set<Integer> globalGroupingSets,
            Step step)
    {
        Symbol key = new Symbol(keyType, "key");
        Symbol groupId = new Symbol(BIGINT, "groupId");
        List<Symbol> groupingKeys = ImmutableList.of(key, groupId);
        Symbol output = new Symbol(BIGINT, "out");

        // A step reading intermediate state takes the partial count as its argument; a raw one takes none.
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                .put(key, 0)
                .put(groupId, 1);
        List<Expression> arguments = ImmutableList.of();
        if (!step.isInputRaw()) {
            Symbol partialCount = new Symbol(BIGINT, "partialCount");
            layout.put(partialCount, 2);
            arguments = ImmutableList.of(partialCount.toSymbolReference());
        }

        Aggregation count = new Aggregation(
                FUNCTION_RESOLUTION.resolveFunction("count", fromTypes()),
                arguments,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        AggregationNode node = new AggregationNode(
                new PlanNodeId("agg"),
                new ValuesNode(new PlanNodeId("source"), groupingKeys),
                ImmutableMap.of(output, count),
                AggregationNode.groupingSets(groupingKeys, groupingSetCount, globalGroupingSets),
                ImmutableList.of(),
                step,
                Optional.of(groupId));

        return switch (GpuAggregationCompiler.compile(node, layout.buildOrThrow(), COMPACTION_THRESHOLD_BYTES)) {
            case GpuAggregationCompiler.AggregationCompileResult.Failure failure -> throw new AssertionError("Failed to compile grouping sets count: " + failure.reason());
            case GpuAggregationCompiler.AggregationCompileResult.Success success -> success;
        };
    }

    @ParameterizedTest
    @EnumSource(value = Step.class, names = {"PARTIAL", "SINGLE", "FINAL"})
    void testGlobalAggregationFinishAfterCompaction(Step step)
    {
        // Only PARTIAL flushes before finish, leaving an empty buffer. SINGLE and FINAL retain their state.
        Block values = createBigintBlock(1_000, NullsProvider.NO_NULLS, 0, 1_000);

        List<Page> results = executeGpuOperation(
                List.of(new Page(values)),
                List.of(BIGINT),
                List.of(BIGINT),
                (context, copyToDevice) -> new GpuAggregation.Factory(
                        List.of(new GpuSum(0, BIGINT, INT64)),
                        new int[] {},
                        List.of(),
                        Optional.empty(),
                        /*inputRaw=*/ step.isInputRaw(),
                        /*outputPartial=*/ step.isOutputPartial(),
                        /*compactionThresholdBytes=*/ 1,
                        1)
                        .create(context, copyToDevice));

        assertThat(results).hasSize(step == PARTIAL ? 2 : 1);
        if (step == PARTIAL) {
            assertThat(results.getLast().getPositionCount()).isEqualTo(1);
            assertThat(results.getLast().getBlock(0).isNull(0)).isTrue();
        }

        // Partial sums are additive, so folding everything emitted gives the total.
        long actual = 0;
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (!page.getBlock(0).isNull(position)) {
                    actual += BIGINT.getLong(page.getBlock(0), position);
                }
            }
        }
        long expected = 0;
        for (int position = 0; position < values.getPositionCount(); position++) {
            expected += BIGINT.getLong(values, position);
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testGroupingSetsPartialFlushBeforeFinish()
    {
        List<Type> types = List.of(BIGINT, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder(types)
                .row(4L, 0L, 7L)
                .row(null, (long) EMPTY_GROUPING_SET_ID, 7L)
                .build();
        List<Page> partialResults = executeGpuOperation(
                input,
                types,
                types,
                (context, copyToDevice) -> new GpuAggregation.Factory(
                        List.of(new GpuCountNonNull(2, BIGINT, INT64)),
                        new int[] {0, 1},
                        List.of(BIGINT, BIGINT),
                        Optional.of(new GpuAggregation.DefaultOutput(1, List.of(EMPTY_GROUPING_SET_ID))),
                        /*inputRaw=*/ true,
                        /*outputPartial=*/ true,
                        /*compactionThresholdBytes=*/ 1,
                        3)
                        .create(context, copyToDevice));

        assertThat(partialResults).hasSize(2);
        Page defaultOutput = partialResults.getLast();
        assertThat(defaultOutput.getPositionCount()).isEqualTo(1);
        assertThat(defaultOutput.getBlock(0).isNull(0)).isTrue();
        assertThat(BIGINT.getLong(defaultOutput.getBlock(1), 0)).isEqualTo(EMPTY_GROUPING_SET_ID);
        assertThat(BIGINT.getLong(defaultOutput.getBlock(2), 0)).isEqualTo(0);

        List<Page> finalResults = runGpuPipeline(partialResults, types, compileRollupCount(BIGINT, FINAL));
        assertSameDataWithoutOrder(
                finalResults,
                rowPagesBuilder(types)
                        .row(4L, 0L, 1L)
                        .row(null, (long) EMPTY_GROUPING_SET_ID, 1L)
                        .build(),
                types);
    }

    private static Block createGroupByBlock(int positionsCount, int numGroups)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(builder, i % numGroups);
        }
        return builder.build();
    }

    private static Page createEmptyPage(List<Type> types)
    {
        if (types.isEmpty()) {
            return new Page(0, BIGINT.createBlockBuilder(null, 0).build());
        }
        Block[] blocks = types.stream()
                .map(type -> type.createBlockBuilder(null, 0).build())
                .toArray(Block[]::new);
        return new Page(0, blocks);
    }

    private void assertGlobalMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, false, step, false);
        List<Type> inputTypes = argumentTypes.isEmpty() ? List.of(BIGINT) : List.copyOf(argumentTypes);

        List<Page> results = runGpuPipeline(inputPages, inputTypes, compiled);

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Object gpuResult;
        if (step.isOutputPartial()) {
            // A partial aggregation may flush its state as several pages; the final re-merge is associative, so
            // concatenate the intermediates and fold them together with the CPU FINAL aggregation.
            Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes)).getIntermediateType();
            Block merged = mergePages(results, List.of(intermediateType)).getBlock(0);
            gpuResult = runCpuFinal(functionName, cpuFinalArgumentTypes(argumentTypes, resolvedFunction), merged);
        }
        else {
            checkState(results.size() == 1, "Expected single result page");
            Page resultPage = results.getFirst();
            checkState(resultPage.getPositionCount() == 1, "Expected single row");
            gpuResult = getOnlyValue(resolvedFunction.signature().getReturnType(), resultPage.getBlock(0));
        }

        Page inputPage = mergePages(inputPages, inputTypes);
        assertAggregation(FUNCTION_RESOLUTION, functionName, fromTypes(argumentTypes), gpuResult, inputPage);
    }

    private List<Page> assertGroupByMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, true, step, false);
        List<Type> inputTypes = ImmutableList.<Type>builder()
                .add(BIGINT)
                .addAll(argumentTypes.isEmpty() ? List.of(BIGINT) : argumentTypes)
                .build();

        List<Page> results = runGpuPipeline(inputPages, inputTypes, compiled);

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));

        Map<Object, Object> gpuResult = new HashMap<>();
        if (step.isOutputPartial()) {
            // A partial aggregation may flush its state before seeing all rows, so a group's intermediate can be
            // split across several output pages. Collect each group's partial states and fold them together with
            // the CPU FINAL aggregation (which is associative) before comparing.
            Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes)).getIntermediateType();
            Map<Object, BlockBuilder> partialsByGroup = new HashMap<>();
            for (Page page : results) {
                Block groupBlock = page.getBlock(0);
                Block intermediateBlock = page.getBlock(1);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    Object groupKey = BIGINT.getObjectValue(groupBlock, position);
                    partialsByGroup.computeIfAbsent(groupKey, _ -> intermediateType.createBlockBuilder(null, results.size()))
                            .appendBlockRange(intermediateBlock, position, 1);
                }
            }
            for (Map.Entry<Object, BlockBuilder> entry : partialsByGroup.entrySet()) {
                verify(gpuResult.put(entry.getKey(), runCpuFinal(functionName, cpuFinalArgumentTypes(argumentTypes, resolvedFunction), entry.getValue().build())) == null);
            }
        }
        else {
            Type outputType = resolvedFunction.signature().getReturnType();
            for (Page page : results) {
                Block groupBlock = page.getBlock(0);
                Block valueBlock = page.getBlock(1);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    Object groupKey = BIGINT.getObjectValue(groupBlock, position);
                    verify(gpuResult.put(groupKey, outputType.getObjectValue(valueBlock, position)) == null);
                }
            }
        }

        Page inputPage = mergePages(inputPages, inputTypes);
        Map<Object, Object> cpuResult = executeCpuGroupByAggregation(inputPage, BIGINT, functionName, argumentTypes);

        assertThat(gpuResult.keySet()).isEqualTo(cpuResult.keySet());
        for (Object groupKey : gpuResult.keySet()) {
            Object gpuValue = gpuResult.get(groupKey);
            Object cpuValue = cpuResult.get(groupKey);
            assertThat(AggregationTestUtils.makeValidityAssertion(cpuValue).apply(gpuValue, cpuValue))
                    .as("Group %s: expected %s but was %s", groupKey, cpuValue, gpuValue)
                    .isTrue();
        }
        return results;
    }

    private void assertMaskedGlobalMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, false, SINGLE, true);
        ImmutableList.Builder<Type> inputTypesBuilder = ImmutableList.builder();
        if (argumentTypes.isEmpty()) {
            inputTypesBuilder.add(BIGINT);
        }
        else {
            inputTypesBuilder.addAll(argumentTypes);
        }
        inputTypesBuilder.add(BOOLEAN);
        List<Type> inputTypes = inputTypesBuilder.build();

        List<Page> results = runGpuPipeline(List.of(inputPage), inputTypes, compiled);
        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Object gpuResult = getOnlyValue(resolvedFunction.signature().getReturnType(), resultPage.getBlock(0));

        Page filteredPage = filterByMask(inputPage);
        assertAggregation(FUNCTION_RESOLUTION, functionName, fromTypes(argumentTypes), gpuResult, filteredPage);
    }

    private void assertMaskedGroupByMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, true, SINGLE, true);
        ImmutableList.Builder<Type> inputTypesBuilder = ImmutableList.<Type>builder()
                .add(BIGINT);
        if (argumentTypes.isEmpty()) {
            inputTypesBuilder.add(BIGINT);
        }
        else {
            inputTypesBuilder.addAll(argumentTypes);
        }
        inputTypesBuilder.add(BOOLEAN);
        List<Type> inputTypes = inputTypesBuilder.build();

        List<Page> results = runGpuPipeline(List.of(inputPage), inputTypes, compiled);

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Type outputType = resolvedFunction.signature().getReturnType();

        Map<Object, Object> gpuResult = new HashMap<>();
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Object groupKey = BIGINT.getObjectValue(page.getBlock(0), position);
                Object groupValue = outputType.getObjectValue(page.getBlock(1), position);
                verify(gpuResult.put(groupKey, groupValue) == null);
            }
        }

        int perGroupMaskChannel = inputPage.getChannelCount() - 2;
        Map<Object, Object> cpuResult = executeCpuGroupByAggregation(inputPage, BIGINT,
                page -> {
                    TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes));
                    int[] valueChannels = argumentTypes.isEmpty() ? new int[] {0} : IntStream.range(0, argumentTypes.size()).toArray();
                    Aggregator aggregator = function.createAggregatorFactory(SINGLE, Ints.asList(valueChannels), OptionalInt.of(perGroupMaskChannel))
                            .createAggregator(new AggregationMetrics());
                    if (page.getPositionCount() > 0) {
                        aggregator.processPage(page);
                    }
                    Block block = AggregationTestUtils.getFinalBlock(function.getFinalType(), aggregator);
                    return getOnlyValue(function.getFinalType(), block);
                });

        assertThat(gpuResult.keySet()).isEqualTo(cpuResult.keySet());
        for (Object groupKey : gpuResult.keySet()) {
            Object gpuValue = gpuResult.get(groupKey);
            Object cpuValue = cpuResult.get(groupKey);
            assertThat(AggregationTestUtils.makeValidityAssertion(cpuValue).apply(gpuValue, cpuValue))
                    .as("Group %s: expected %s but was %s", groupKey, cpuValue, gpuValue)
                    .isTrue();
        }
    }

    private static GpuAggregationCompiler.AggregationCompileResult.Success compileAggregation(String functionName, List<Type> argumentTypes, boolean grouped, Step step, boolean masked)
    {
        return switch (tryCompileAggregation(functionName, argumentTypes, grouped, step, masked)) {
            case GpuAggregationCompiler.AggregationCompileResult.Failure _ -> throw new AssertionError("Failed to compile %s over %s".formatted(functionName, argumentTypes));
            case GpuAggregationCompiler.AggregationCompileResult.Success success -> success;
        };
    }

    private static GpuAggregationCompiler.AggregationCompileResult.Success compileAggregation(String functionName, List<Type> argumentTypes, List<Type> stepInputTypes, boolean grouped, Step step, boolean masked)
    {
        return switch (tryCompileAggregation(functionName, argumentTypes, stepInputTypes, grouped, step, masked)) {
            case GpuAggregationCompiler.AggregationCompileResult.Failure _ -> throw new AssertionError("Failed to compile %s over %s".formatted(functionName, argumentTypes));
            case GpuAggregationCompiler.AggregationCompileResult.Success success -> success;
        };
    }

    private static void assertCompileNotSupported(String functionName, List<Type> argumentTypes, boolean grouped)
    {
        for (Step step : Step.values()) {
            assertCompileNotSupported(functionName, argumentTypes, grouped, step);
        }
    }

    private static void assertCompileNotSupported(String functionName, List<Type> argumentTypes, boolean grouped, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult result;
        try {
            result = tryCompileAggregation(functionName, argumentTypes, grouped, step, false);
        }
        catch (TrinoException e) {
            // Trino has no such function for these argument types, so the GPU compiler is never invoked.
            verifyFunctionResolutionError(functionName, e);
            return;
        }
        assertThat(result).isInstanceOf(GpuAggregationCompiler.AggregationCompileResult.Failure.class);
    }

    private static GpuAggregationCompiler.AggregationCompileResult tryCompileAggregation(String functionName, List<Type> argumentTypes, boolean grouped, Step step, boolean masked)
    {
        return tryCompileAggregation(functionName, argumentTypes, argumentTypes, grouped, step, masked);
    }

    private static GpuAggregationCompiler.AggregationCompileResult tryCompileAggregation(String functionName, List<Type> argumentTypes, List<Type> stepInputTypes, boolean grouped, Step step, boolean masked)
    {
        ImmutableList.Builder<Symbol> sourceSymbols = ImmutableList.builder();
        List<Symbol> groupingKeys = List.of();

        if (grouped) {
            Symbol groupSymbol = new Symbol(BIGINT, "grp");
            groupingKeys = List.of(groupSymbol);
            sourceSymbols.add(groupSymbol);
        }

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        if (!argumentTypes.isEmpty()) {
            for (int i = 0; i < stepInputTypes.size(); i++) {
                Symbol argument = new Symbol(stepInputTypes.get(i), "val" + i);
                sourceSymbols.add(argument);
                arguments.add(new Reference(argument.type(), argument.name()));
            }
        }
        else {
            sourceSymbols.add(new Symbol(BIGINT, "unused"));
        }

        Optional<Symbol> maskSymbol = Optional.empty();
        if (masked) {
            Symbol mask = new Symbol(BOOLEAN, "mask");
            sourceSymbols.add(mask);
            maskSymbol = Optional.of(mask);
        }

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Type outputType;
        if (step.isOutputPartial()) {
            TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes));
            outputType = function.getIntermediateType();
        }
        else {
            outputType = resolvedFunction.signature().getReturnType();
        }
        Symbol outputSymbol = new Symbol(outputType, "out");

        Aggregation aggregation = new Aggregation(
                resolvedFunction,
                arguments.build(),
                false,
                Optional.empty(),
                Optional.empty(),
                maskSymbol);

        ValuesNode source = new ValuesNode(new PlanNodeId("source"), sourceSymbols.build());

        AggregationNode node = new AggregationNode(
                new PlanNodeId("agg"),
                source,
                ImmutableMap.of(outputSymbol, aggregation),
                groupingKeys.isEmpty()
                        ? AggregationNode.globalAggregation()
                        : AggregationNode.singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                step,
                Optional.empty());

        ImmutableMap.Builder<Symbol, Integer> layoutBuilder = ImmutableMap.builder();
        List<Symbol> allSourceSymbols = source.getOutputSymbols();
        for (int i = 0; i < allSourceSymbols.size(); i++) {
            layoutBuilder.put(allSourceSymbols.get(i), i);
        }

        return GpuAggregationCompiler.compile(node, layoutBuilder.buildOrThrow(), COMPACTION_THRESHOLD_BYTES);
    }

    private static List<Page> runGpuPipeline(List<Page> inputPages, List<Type> inputTypes, GpuAggregationCompiler.AggregationCompileResult.Success compiled)
    {
        return executeGpuOperation(
                inputPages,
                inputTypes,
                compiled.finalOutputTypes(),
                (context, copyToDevice) -> {
                    GpuOperation current = copyToDevice;
                    for (GpuOperation.Factory factory : compiled.stages()) {
                        current = factory.create(context, current);
                    }
                    return current;
                });
    }

    private Map<Object, Object> executeCpuGroupByAggregation(
            Page inputPage,
            Type groupByType,
            String cpuFunctionName,
            List<Type> cpuParamTypes)
    {
        return executeCpuGroupByAggregation(inputPage, groupByType,
                page -> {
                    TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(cpuFunctionName, fromTypes(cpuParamTypes));
                    return AggregationTestUtils.aggregation(function, page);
                });
    }

    private Map<Object, Object> executeCpuGroupByAggregation(
            Page inputPage,
            Type groupByType,
            Function<Page, Object> perGroupAggregation)
    {
        Block groupBlock = inputPage.getBlock(GROUP_KEY_CHANNEL);
        int dataChannelCount = inputPage.getChannelCount() - 1;

        Map<Object, List<Integer>> groupPositions = new HashMap<>();
        for (int i = 0; i < inputPage.getPositionCount(); i++) {
            Object groupKey = groupByType.getObjectValue(groupBlock, i);
            groupPositions.computeIfAbsent(groupKey, _ -> new ArrayList<>()).add(i);
        }

        Map<Object, Object> result = new HashMap<>();
        for (Map.Entry<Object, List<Integer>> entry : groupPositions.entrySet()) {
            int[] positions = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
            Page groupPage = inputPage.getColumns(IntStream.rangeClosed(1, dataChannelCount).toArray())
                    .getPositions(positions, 0, positions.length);
            verify(result.put(entry.getKey(), perGroupAggregation.apply(groupPage)) == null);
        }
        return result;
    }

    private static Page filterByMask(Page inputPage)
    {
        int maskChannel = inputPage.getChannelCount() - 1;
        Block maskBlock = inputPage.getBlock(maskChannel);
        AggregationMask mask = AggregationMask.createSelectAll(inputPage.getPositionCount());
        mask.applyMaskBlock(maskBlock);
        return mask.filterPage(inputPage.getColumns(IntStream.range(0, maskChannel).toArray()));
    }

    private Object executeGpuGlobalFinalAggregation(
            Page inputPage,
            List<Type> inputTypes,
            GpuAggregateFunction gpuAggregate)
    {
        List<Page> results = executeGpuAggregation(inputPage, inputTypes, Optional.empty(), List.of(gpuAggregate), false);

        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");
        return getOnlyValue(gpuAggregate.outputType(), resultPage.getBlock(0));
    }

    private List<Page> executeGpuFinalAggregation(
            Page inputPage,
            Type groupByKeyType,
            Type groupByValueType,
            GpuAggregateFunction gpuAggregate)
    {
        return executeGpuAggregation(
                inputPage,
                List.of(groupByKeyType, groupByValueType),
                Optional.of(groupByKeyType),
                List.of(gpuAggregate),
                false);
    }

    private List<Page> executeGpuAggregation(
            Page inputPage,
            List<Type> inputTypes,
            Optional<Type> groupByType,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw)
    {
        ImmutableList.Builder<Type> outputTypesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> groupByTypesBuilder = ImmutableList.builder();
        ImmutableSet.Builder<Integer> groupByChannelsBuilder = ImmutableSet.builder();

        if (groupByType.isPresent()) {
            outputTypesBuilder.add(groupByType.get());
            groupByTypesBuilder.add(groupByType.get());
            groupByChannelsBuilder.add(GROUP_KEY_CHANNEL);
        }
        aggregates.forEach(agg -> outputTypesBuilder.add(agg.outputType()));

        int[] groupByChannels = groupByChannelsBuilder.build().stream()
                .mapToInt(Integer::intValue)
                .toArray();

        return executeGpuOperation(
                List.of(inputPage),
                inputTypes,
                outputTypesBuilder.build(),
                (context, copyToDevice) -> {
                    GpuAggregation.Factory factory = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypesBuilder.build(), Optional.empty(), inputRaw, /*outputPartial=*/ false, COMPACTION_THRESHOLD_BYTES, inputTypes.size());
                    return factory.create(context, copyToDevice);
                });
    }

    /// The argument types to resolve the CPU FINAL aggregation by. A single-argument aggregation resolves by its return
    /// type, so that for example count() finalizes like count(x).
    private static List<Type> cpuFinalArgumentTypes(List<Type> argumentTypes, ResolvedFunction resolvedFunction)
    {
        if (argumentTypes.size() > 1) {
            return argumentTypes;
        }
        return List.of(resolvedFunction.signature().getReturnType());
    }

    private static Object runCpuFinal(String functionName, Type paramType, Block intermediate)
    {
        return runCpuFinal(functionName, List.of(paramType), intermediate);
    }

    private static Object runCpuFinal(String functionName, List<Type> argumentTypes, Block intermediate)
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes));
        AggregatorFactory finalFactory = function.createAggregatorFactory(FINAL, List.of(0), OptionalInt.empty());
        Aggregator aggregator = finalFactory.createAggregator(new AggregationMetrics());
        aggregator.processPage(new Page(intermediate));
        Block finalBlock = AggregationTestUtils.getFinalBlock(function.getFinalType(), aggregator);
        return getOnlyValue(function.getFinalType(), finalBlock);
    }

    private static Block createMaskBlock(int positionCount)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (i % 7 == 0) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, i % 3 != 0);
            }
        }
        return builder.build();
    }

    /**
     * Verifies the PARTIAL/FINAL split for ({@code functionName}, {@code argumentType}) by running every
     * combination of {CPU, GPU} PARTIAL × {CPU, GPU} FINAL and checking they all agree with the CPU-only
     * golden result. Combinations whose GPU leg the compiler doesn't support are skipped. When both GPU
     * legs are unsupported the test is a no-op (SINGLE-step coverage handles that case separately).
     *
     * @param intermediateMayDiffer if true, skip the block-equality check between CPU PARTIAL and GPU
     *         PARTIAL outputs (e.g. {@code sum(decimal)}/{@code avg(decimal)} where CPU emits variable-
     *         length intermediates and GPU emits a uniform 16-byte layout)
     */
    private void runPartialFinalGlobal(String functionName, Type argumentType, boolean intermediateMayDiffer)
    {
        ResolvedFunction resolvedFunction;
        try {
            resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentType));
        }
        catch (TrinoException e) {
            // Trino has no such function for this argument type; the GPU compiler is never invoked.
            verifyFunctionResolutionError(functionName, e);
            return;
        }
        if (!resolvedFunction.signature().getArgumentTypes().equals(List.of(argumentType))) {
            // Trino's planner would insert a coercion (e.g. sum(tinyint) → sum(bigint)); the GPU
            // compiler never sees argumentType directly, so there is no PARTIAL/FINAL combination to verify.
            return;
        }
        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentType)).getIntermediateType();

        boolean gpuPartialSupported = tryCompileAggregation(functionName, List.of(argumentType), List.of(argumentType), false, PARTIAL, false) instanceof GpuAggregationCompiler.AggregationCompileResult.Success;
        boolean gpuFinalSupported = tryCompileAggregation(functionName, List.of(argumentType), List.of(intermediateType), false, FINAL, false) instanceof GpuAggregationCompiler.AggregationCompileResult.Success;
        if (!gpuPartialSupported && !gpuFinalSupported) {
            // SINGLE-step coverage already asserts no GPU path; nothing to verify here.
            return;
        }

        Page rawInput = new Page(generateRawInputBlock(functionName, argumentType));

        Block cpuPartial = runCpuPartialGlobal(functionName, List.of(argumentType), rawInput);

        Optional<Block> gpuPartial = Optional.empty();
        if (gpuPartialSupported) {
            Block block = runGpuPartialGlobal(functionName, List.of(argumentType), rawInput);
            if (!intermediateMayDiffer) {
                assertBlocksEqual(block, cpuPartial, intermediateType);
            }
            gpuPartial = Optional.of(block);
        }

        Object golden = runCpuFinal(functionName, argumentType, cpuPartial);

        if (gpuFinalSupported) {
            assertGoldenEqual(runGpuFinalGlobal(functionName, List.of(argumentType), cpuPartial, intermediateType), golden);
        }
        if (gpuPartial.isPresent()) {
            assertGoldenEqual(runCpuFinal(functionName, argumentType, gpuPartial.get()), golden);
        }
        if (gpuFinalSupported && gpuPartial.isPresent()) {
            assertGoldenEqual(runGpuFinalGlobal(functionName, List.of(argumentType), gpuPartial.get(), intermediateType), golden);
        }
    }

    private static Block generateRawInputBlock(String functionName, Type type)
    {
        if ("sum".equals(functionName) && type == BIGINT) {
            return createBigintBlock(100, RANDOM_NULLS, -10000, 10001);
        }
        if ("sum".equals(functionName) && type instanceof DecimalType decimalType) {
            // Cap magnitude so 100 random values can't overflow DECIMAL128 when summed.
            // GpuTestUtils#createBlock would otherwise generate values up to 10^precision, and
            // 100 random values of precision 38 readily exceed DECIMAL128 capacity.
            return createSmallDecimalBlock(decimalType, 100);
        }
        return createBlock(type, 100, RANDOM_NULLS);
    }

    private static Block createSmallDecimalBlock(DecimalType type, int positionCount)
    {
        Random random = new Random(42);
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (i % 7 == 0) {
                builder.appendNull();
            }
            else {
                long unscaled = random.nextLong(-1_000_000L, 1_000_001L);
                if (type.isShort()) {
                    type.writeLong(builder, unscaled);
                }
                else {
                    type.writeObject(builder, Int128.valueOf(unscaled));
                }
            }
        }
        return builder.build();
    }

    private static Block runCpuPartialGlobal(String functionName, List<Type> argumentTypes, Page rawInput)
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes));
        int[] channels = IntStream.range(0, argumentTypes.size()).toArray();
        Aggregator aggregator = function.createAggregatorFactory(PARTIAL, Ints.asList(channels), OptionalInt.empty())
                .createAggregator(new AggregationMetrics());
        if (rawInput.getPositionCount() > 0) {
            aggregator.processPage(rawInput);
        }
        return AggregationTestUtils.getFinalBlock(function.getIntermediateType(), aggregator);
    }

    private Block runGpuPartialGlobal(String functionName, List<Type> argumentTypes, Page rawInput)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, false, PARTIAL, false);
        List<Page> results = runGpuPipeline(List.of(rawInput), argumentTypes, compiled);
        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");
        return resultPage.getBlock(0);
    }

    private Object runGpuFinalGlobal(String functionName, List<Type> argumentTypes, Block intermediate, Type intermediateType)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, List.of(intermediateType), false, FINAL, false);
        List<Page> results = runGpuPipeline(List.of(new Page(intermediate)), List.of(intermediateType), compiled);
        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        return getOnlyValue(resolvedFunction.signature().getReturnType(), resultPage.getBlock(0));
    }

    private static void assertBlocksEqual(Block actual, Block expected, Type type)
    {
        assertSameDataInOrder(List.of(new Page(actual)), List.of(new Page(expected)), List.of(type));
    }

    private static void assertGoldenEqual(Object actual, Object golden)
    {
        assertThat(AggregationTestUtils.makeValidityAssertion(golden).apply(actual, golden))
                .as("expected %s but was %s", golden, actual)
                .isTrue();
    }

    private void runPartialFinalGrouped(String functionName, Type argumentType, boolean intermediateMayDiffer)
    {
        ResolvedFunction resolvedFunction;
        try {
            resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentType));
        }
        catch (TrinoException e) {
            verifyFunctionResolutionError(functionName, e);
            return;
        }
        if (!resolvedFunction.signature().getArgumentTypes().equals(List.of(argumentType))) {
            return;
        }
        Type intermediateType = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentType)).getIntermediateType();

        boolean gpuPartialSupported = tryCompileAggregation(functionName, List.of(argumentType), List.of(argumentType), true, PARTIAL, false) instanceof GpuAggregationCompiler.AggregationCompileResult.Success;
        boolean gpuFinalSupported = tryCompileAggregation(functionName, List.of(argumentType), List.of(intermediateType), true, FINAL, false) instanceof GpuAggregationCompiler.AggregationCompileResult.Success;
        if (!gpuPartialSupported && !gpuFinalSupported) {
            return;
        }

        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = generateRawInputBlock(functionName, argumentType);
        Page rawInput = new Page(groupByBlock, valueBlock);

        Page cpuPartial = runCpuPartialGrouped(functionName, List.of(argumentType), rawInput, intermediateType);

        Optional<Page> gpuPartial = Optional.empty();
        if (gpuPartialSupported) {
            Page page = runGpuPartialGrouped(functionName, List.of(argumentType), rawInput, intermediateType);
            if (!intermediateMayDiffer) {
                assertSameDataWithoutOrder(List.of(page), List.of(cpuPartial), List.of(BIGINT, intermediateType));
            }
            gpuPartial = Optional.of(page);
        }

        Map<Object, Object> golden = runCpuFinalGrouped(functionName, argumentType, cpuPartial);

        if (gpuFinalSupported) {
            assertGoldenMapEqual(runGpuFinalGrouped(functionName, List.of(argumentType), cpuPartial, intermediateType), golden);
        }
        if (gpuPartial.isPresent()) {
            assertGoldenMapEqual(runCpuFinalGrouped(functionName, argumentType, gpuPartial.get()), golden);
        }
        if (gpuFinalSupported && gpuPartial.isPresent()) {
            assertGoldenMapEqual(runGpuFinalGrouped(functionName, List.of(argumentType), gpuPartial.get(), intermediateType), golden);
        }
    }

    private static Page runCpuPartialGrouped(String functionName, List<Type> argumentTypes, Page rawInput, Type intermediateType)
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(argumentTypes));
        Block groupBlock = rawInput.getBlock(GROUP_KEY_CHANNEL);

        Map<Object, List<Integer>> groupPositions = new LinkedHashMap<>();
        for (int i = 0; i < rawInput.getPositionCount(); i++) {
            Object groupKey = BIGINT.getObjectValue(groupBlock, i);
            groupPositions.computeIfAbsent(groupKey, _ -> new ArrayList<>()).add(i);
        }

        int dataChannelCount = rawInput.getChannelCount() - 1;
        BlockBuilder groupBuilder = BIGINT.createBlockBuilder(null, groupPositions.size());
        BlockBuilder intermediateBuilder = intermediateType.createBlockBuilder(null, groupPositions.size());

        int[] channels = IntStream.range(0, argumentTypes.size()).toArray();
        for (Map.Entry<Object, List<Integer>> entry : groupPositions.entrySet()) {
            int[] positions = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
            Page groupPage = rawInput.getColumns(IntStream.rangeClosed(1, dataChannelCount).toArray())
                    .getPositions(positions, 0, positions.length);
            Aggregator aggregator = function.createAggregatorFactory(PARTIAL, Ints.asList(channels), OptionalInt.empty())
                    .createAggregator(new AggregationMetrics());
            if (groupPage.getPositionCount() > 0) {
                aggregator.processPage(groupPage);
            }
            Block partial = AggregationTestUtils.getFinalBlock(intermediateType, aggregator);
            BIGINT.writeLong(groupBuilder, (Long) entry.getKey());
            intermediateBuilder.appendBlockRange(partial, 0, 1);
        }

        return new Page(groupBuilder.build(), intermediateBuilder.build());
    }

    private Page runGpuPartialGrouped(String functionName, List<Type> argumentTypes, Page rawInput, Type intermediateType)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, true, PARTIAL, false);
        List<Type> inputTypes = ImmutableList.<Type>builder()
                .add(BIGINT)
                .addAll(argumentTypes.isEmpty() ? List.of(BIGINT) : argumentTypes)
                .build();
        List<Page> results = runGpuPipeline(List.of(rawInput), inputTypes, compiled);
        return mergePages(results, List.of(BIGINT, intermediateType));
    }

    private static Map<Object, Object> runCpuFinalGrouped(String functionName, Type argumentType, Page intermediatePage)
    {
        return runCpuFinalGrouped(functionName, List.of(argumentType), intermediatePage);
    }

    private static Map<Object, Object> runCpuFinalGrouped(String functionName, List<Type> argumentTypes, Page intermediatePage)
    {
        Map<Object, Object> result = new HashMap<>();
        Block groupBlock = intermediatePage.getBlock(0);
        Block intermediateBlock = intermediatePage.getBlock(1);
        for (int i = 0; i < intermediatePage.getPositionCount(); i++) {
            Object groupKey = BIGINT.getObjectValue(groupBlock, i);
            Object value = runCpuFinal(functionName, argumentTypes, intermediateBlock.getRegion(i, 1));
            verify(result.put(groupKey, value) == null);
        }
        return result;
    }

    private Map<Object, Object> runGpuFinalGrouped(String functionName, List<Type> argumentTypes, Page intermediatePage, Type intermediateType)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, List.of(intermediateType), true, FINAL, false);
        List<Page> results = runGpuPipeline(List.of(intermediatePage), List.of(BIGINT, intermediateType), compiled);
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Type returnType = resolvedFunction.signature().getReturnType();
        Map<Object, Object> result = new HashMap<>();
        for (Page page : results) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                Object groupKey = BIGINT.getObjectValue(page.getBlock(0), i);
                Object value = returnType.getObjectValue(page.getBlock(1), i);
                verify(result.put(groupKey, value) == null);
            }
        }
        return result;
    }

    private static void assertGoldenMapEqual(Map<Object, Object> actual, Map<Object, Object> golden)
    {
        assertThat(actual.keySet()).isEqualTo(golden.keySet());
        for (Object groupKey : golden.keySet()) {
            Object expected = golden.get(groupKey);
            Object got = actual.get(groupKey);
            assertThat(AggregationTestUtils.makeValidityAssertion(expected).apply(got, expected))
                    .as("Group %s: expected %s but was %s", groupKey, expected, got)
                    .isTrue();
        }
    }

    private static void verifyFunctionResolutionError(String functionName, TrinoException e)
    {
        assertThat(e)
                .hasMessageFindingMatch("^Unexpected parameters \\(.*\\) for function \\Q" + functionName)
                .hasStackTraceContaining("io.trino.metadata.BuiltinFunctionResolver");
    }
}
