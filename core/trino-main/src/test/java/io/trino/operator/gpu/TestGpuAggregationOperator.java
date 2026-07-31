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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.block.BlockAssertions.getOnlyValue;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataWithoutOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final int GROUP_KEY_CHANNEL = 0;

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
    void testAnyValueGlobalEmpty()
    {
        Page inputPage = createEmptyPage(List.of(BIGINT));
        assertGlobalMatchesCpu(List.of(inputPage), "any_value", List.of(BIGINT), SINGLE);
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
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testCountPartialFinalGlobalForAllTypes(Type type)
    {
        runPartialFinalGlobal("count", type, false);
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
        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Object gpuResult;
        if (step.isOutputPartial()) {
            gpuResult = runCpuFinal(functionName, resolvedFunction.signature().getReturnType(), resultPage.getBlock(0));
        }
        else {
            gpuResult = getOnlyValue(resolvedFunction.signature().getReturnType(), resultPage.getBlock(0));
        }

        Page inputPage = mergePages(inputPages, inputTypes);
        assertAggregation(FUNCTION_RESOLUTION, functionName, fromTypes(argumentTypes), gpuResult, inputPage);
    }

    private void assertGroupByMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes, Step step)
    {
        GpuAggregationCompiler.AggregationCompileResult.Success compiled = compileAggregation(functionName, argumentTypes, true, step, false);
        List<Type> inputTypes = ImmutableList.<Type>builder()
                .add(BIGINT)
                .addAll(argumentTypes.isEmpty() ? List.of(BIGINT) : argumentTypes)
                .build();

        List<Page> results = runGpuPipeline(inputPages, inputTypes, compiled);

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Type outputType = step.isOutputPartial() ? VARBINARY : resolvedFunction.signature().getReturnType();

        Map<Object, Object> gpuResult = new HashMap<>();
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Object groupKey = BIGINT.getObjectValue(page.getBlock(0), position);
                Object groupValue;
                if (step.isOutputPartial()) {
                    groupValue = runCpuFinal(functionName, resolvedFunction.signature().getReturnType(), page.getBlock(1).getRegion(position, 1));
                }
                else {
                    groupValue = outputType.getObjectValue(page.getBlock(1), position);
                }
                verify(gpuResult.put(groupKey, groupValue) == null);
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

        List<Expression> arguments = ImmutableList.of();
        if (!argumentTypes.isEmpty()) {
            Symbol val = new Symbol(stepInputTypes.getFirst(), "val");
            sourceSymbols.add(val);
            arguments = ImmutableList.of(new Reference(stepInputTypes.getFirst(), "val"));
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
                arguments,
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

        return GpuAggregationCompiler.compile(node, layoutBuilder.buildOrThrow(), /*compactionThresholdBytes=*/ 1);
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
                    GpuAggregation.Factory factory = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypesBuilder.build(), inputRaw, /*compactionThresholdBytes=*/ 1, inputTypes.size());
                    return factory.create(context, copyToDevice);
                });
    }

    private static Object runCpuFinal(String functionName, Type paramType, Block intermediate)
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(paramType));
        AggregatorFactory finalFactory = function.createAggregatorFactory(FINAL, List.of(0), OptionalInt.empty());
        Aggregator aggregator = finalFactory.createAggregator(new AggregationMetrics());
        aggregator.processPage(new Page(intermediate));
        Block finalBlock = AggregationTestUtils.getFinalBlock(function.getFinalType(), aggregator);
        return getOnlyValue(function.getFinalType(), finalBlock);
    }

    private static Page mergePages(List<Page> pages, List<Type> types)
    {
        int channelCount = types.size();
        int totalPositions = pages.stream().mapToInt(Page::getPositionCount).sum();
        Block[] mergedBlocks = new Block[channelCount];
        for (int channel = 0; channel < channelCount; channel++) {
            BlockBuilder builder = types.get(channel).createBlockBuilder(null, totalPositions);
            for (Page page : pages) {
                builder.appendBlockRange(page.getBlock(channel), 0, page.getPositionCount());
            }
            mergedBlocks[channel] = builder.build();
        }
        return new Page(totalPositions, mergedBlocks);
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
        Map<Object, Object> result = new HashMap<>();
        Block groupBlock = intermediatePage.getBlock(0);
        Block intermediateBlock = intermediatePage.getBlock(1);
        for (int i = 0; i < intermediatePage.getPositionCount(); i++) {
            Object groupKey = BIGINT.getObjectValue(groupBlock, i);
            Object value = runCpuFinal(functionName, argumentType, intermediateBlock.getRegion(i, 1));
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
