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
import io.trino.operator.gpu.aggregation.GpuAggregationCompiler.CompileResult;
import io.trino.operator.gpu.aggregation.GpuCountNonNull;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.block.BlockAssertions.getOnlyValue;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
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
        assertGlobalMatchesCpu(createEmptyPage(List.of()), "count", List.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountAllGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);
        assertGlobalMatchesCpu(new Page(block), "count", List.of());
    }

    @Test
    void testGroupByCountAllEmpty()
    {
        assertGroupByMatchesCpu(createEmptyPage(List.of(BIGINT, BIGINT)), "count", List.of());
    }

    @Test
    void testGroupByCountAll()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BIGINT, 100, RANDOM_NULLS);
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "count", List.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountNonNullGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);
        assertGlobalMatchesCpu(new Page(block), "count", List.of(BIGINT));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testCountNonNullForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);
        assertGlobalMatchesCpu(new Page(block), "count", List.of(type));
    }

    @Test
    void testCountNonNullGlobalMerge()
    {
        // Simulate merging partial COUNT(column) results: 10 + 20 + 5 = 35
        BlockBuilder countBuilder = BIGINT.createBlockBuilder(null, 3);
        BIGINT.writeLong(countBuilder, 10);
        BIGINT.writeLong(countBuilder, 20);
        BIGINT.writeLong(countBuilder, 5);

        Page inputPage = new Page(countBuilder.build());

        Object result = executeGpuGlobalMergeAggregation(
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
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "count", List.of(type));
    }

    @Test
    void testGroupByCountNonNullMerge()
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

        List<Page> results = executeGpuMergeAggregation(
                inputPage,
                BIGINT,
                BIGINT,
                new GpuCountNonNull(1, BIGINT, INT64));

        assertThat(results).hasSize(1);
        Page resultPage = results.getFirst();
        assertThat(resultPage.getPositionCount()).isEqualTo(2);

        Map<Long, Long> resultMap = new HashMap<>();
        for (int i = 0; i < resultPage.getPositionCount(); i++) {
            long groupKey = BIGINT.getLong(resultPage.getBlock(0), i);
            long count = BIGINT.getLong(resultPage.getBlock(1), i);
            resultMap.put(groupKey, count);
        }
        assertThat(resultMap).containsEntry(0L, 30L);
        assertThat(resultMap).containsEntry(1L, 20L);
    }

    @Test
    void testSumGlobalEmpty()
    {
        assertGlobalMatchesCpu(createEmptyPage(List.of(BIGINT)), "sum", List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSumGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 1, 10);
        assertGlobalMatchesCpu(new Page(block), "sum", List.of(BIGINT));
    }

    @ParameterizedTest
    @MethodSource("sumSupportedTypes")
    void testSumForAllTypes(Type type)
    {
        Block block = createInputBlockForSum(type, 100);
        assertGlobalMatchesCpu(new Page(block), "sum", List.of(type));
    }

    @ParameterizedTest
    @MethodSource("sumSupportedTypes")
    void testGroupBySumForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createInputBlockForSum(type, 100);
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "sum", List.of(type));
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
        assertGlobalMatchesCpu(createEmptyPage(List.of(BIGINT)), "min", List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMinGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        assertGlobalMatchesCpu(new Page(block), "min", List.of(BIGINT));
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
        assertGlobalMatchesCpu(new Page(block), "min", List.of(type));
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
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "min", List.of(type));
    }

    @Test
    void testMaxGlobalEmpty()
    {
        assertGlobalMatchesCpu(createEmptyPage(List.of(BIGINT)), "max", List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMaxGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        assertGlobalMatchesCpu(new Page(block), "max", List.of(BIGINT));
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
        assertGlobalMatchesCpu(new Page(block), "max", List.of(type));
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
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "max", List.of(type));
    }

    @Test
    void testGroupByMultipleBatchesCountAll()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGroupByMatchesCpu(inputPages, "count", List.of());
    }

    @Test
    void testGroupByMultipleBatchesCountNonNull()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGroupByMatchesCpu(inputPages, "count", List.of(BIGINT));
    }

    @Test
    void testGroupByMultipleBatchesSum()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGroupByMatchesCpu(inputPages, "sum", List.of(BIGINT));
    }

    @Test
    void testGroupByMultipleBatchesMin()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "min", List.of(BIGINT));
    }

    @Test
    void testGroupByMultipleBatchesMax()
    {
        List<Page> inputPages = List.of(
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createGroupByBlock(TARGET_ROW_COUNT, 5), createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGroupByMatchesCpu(inputPages, "max", List.of(BIGINT));
    }

    @Test
    void testGlobalMultipleBatchesCountAll()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of());
    }

    @Test
    void testGlobalMultipleBatchesCountNonNull()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, RANDOM_NULLS)));
        assertGlobalMatchesCpu(inputPages, "count", List.of(BIGINT));
    }

    @Test
    void testGlobalMultipleBatchesSum()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, 1, 10)));
        assertGlobalMatchesCpu(inputPages, "sum", List.of(BIGINT));
    }

    @Test
    void testGlobalMultipleBatchesMin()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "min", List.of(BIGINT));
    }

    @Test
    void testGlobalMultipleBatchesMax()
    {
        List<Page> inputPages = List.of(
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)),
                new Page(createBigintBlock(TARGET_ROW_COUNT, RANDOM_NULLS, -1000, 1000)));
        assertGlobalMatchesCpu(inputPages, "max", List.of(BIGINT));
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
        assertGlobalMatchesCpu(createEmptyPage(List.of(BOOLEAN)), "bool_or", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testBoolOrGlobal(NullsProvider nullsProvider)
    {
        Block block = createBlock(BOOLEAN, 100, nullsProvider);
        assertGlobalMatchesCpu(new Page(block), "bool_or", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testGroupByBoolOr(NullsProvider nullsProvider)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, nullsProvider);
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "bool_or", List.of(BOOLEAN));
    }

    @Test
    void testBoolAndGlobalEmpty()
    {
        assertGlobalMatchesCpu(createEmptyPage(List.of(BOOLEAN)), "bool_and", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testBoolAndGlobal(NullsProvider nullsProvider)
    {
        Block block = createBlock(BOOLEAN, 100, nullsProvider);
        assertGlobalMatchesCpu(new Page(block), "bool_and", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testGroupByBoolAnd(NullsProvider nullsProvider)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BOOLEAN, 100, nullsProvider);
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "bool_and", List.of(BOOLEAN));
    }

    @ParameterizedTest
    @MethodSource("shortDecimalTypes")
    void testAvgDecimalPartialGlobal(DecimalType type)
    {
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page input = new Page(valueBlock);

        assertGlobalMatchesCpu(input, "avg", List.of(type), PARTIAL);
    }

    @ParameterizedTest
    @MethodSource("shortDecimalTypes")
    void testAvgDecimalPartialGroupBy(DecimalType type)
    {
        Block groupKeys = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page input = new Page(groupKeys, valueBlock);

        assertGroupByMatchesCpu(input, "avg", List.of(type), PARTIAL);
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
        assertGlobalMatchesCpu(createEmptyPage(List.of(BIGINT)), "any_value", List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testAnyValueGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);
        assertGlobalMatchesCpu(new Page(block), "any_value", List.of(BIGINT));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAnyValueForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);
        assertGlobalMatchesCpu(new Page(block), "any_value", List.of(type));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGroupByAnyValueForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        assertGroupByMatchesCpu(new Page(groupByBlock, valueBlock), "any_value", List.of(type));
    }

    static Stream<Type> sumSupportedTypes()
    {
        return Stream.of(BIGINT, DOUBLE);
    }

    static Stream<DecimalType> shortDecimalTypes()
    {
        return Stream.of(
                createDecimalType(12, 2),
                createDecimalType(6, 0),
                createDecimalType(18, 6));
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

    private void assertGlobalMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes)
    {
        assertGlobalMatchesCpu(inputPage, functionName, argumentTypes, SINGLE);
    }

    private void assertGlobalMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes, AggregationNode.Step step)
    {
        assertGlobalMatchesCpu(List.of(inputPage), functionName, argumentTypes, step);
    }

    private void assertGlobalMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes)
    {
        assertGlobalMatchesCpu(inputPages, functionName, argumentTypes, SINGLE);
    }

    private void assertGlobalMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes, AggregationNode.Step step)
    {
        CompileResult compiled = compileAggregation(functionName, argumentTypes, false, step);
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

    private void assertGroupByMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes)
    {
        assertGroupByMatchesCpu(List.of(inputPage), functionName, argumentTypes, SINGLE);
    }

    private void assertGroupByMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes)
    {
        assertGroupByMatchesCpu(inputPages, functionName, argumentTypes, SINGLE);
    }

    private void assertGroupByMatchesCpu(Page inputPage, String functionName, List<Type> argumentTypes, AggregationNode.Step step)
    {
        assertGroupByMatchesCpu(List.of(inputPage), functionName, argumentTypes, step);
    }

    private void assertGroupByMatchesCpu(List<Page> inputPages, String functionName, List<Type> argumentTypes, AggregationNode.Step step)
    {
        CompileResult compiled = compileAggregation(functionName, argumentTypes, true, step);
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
                    groupValue = runCpuFinal(functionName, resolvedFunction.signature().getReturnType(), page.getBlock(1), position);
                }
                else {
                    groupValue = outputType.getObjectValue(page.getBlock(1), position);
                }
                Object previous = gpuResult.put(groupKey, groupValue);
                assertThat(previous).as("Duplicate group key in GPU result: %s", groupKey).isNull();
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
        CompileResult compiled = compileAggregation(functionName, argumentTypes, false, SINGLE, true);
        ImmutableList.Builder<Type> inputTypesBuilder = ImmutableList.builder();
        if (argumentTypes.isEmpty()) {
            inputTypesBuilder.add(BIGINT);
        }
        else {
            inputTypesBuilder.addAll(argumentTypes);
        }
        inputTypesBuilder.add(BOOLEAN);
        List<Type> inputTypes = inputTypesBuilder.build();

        List<Page> results = runGpuPipeline(inputPage, inputTypes, compiled);
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
        CompileResult compiled = compileAggregation(functionName, argumentTypes, true, SINGLE, true);
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

        List<Page> results = runGpuPipeline(inputPage, inputTypes, compiled);

        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(argumentTypes));
        Type outputType = resolvedFunction.signature().getReturnType();

        Map<Object, Object> gpuResult = new HashMap<>();
        for (Page page : results) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Object groupKey = BIGINT.getObjectValue(page.getBlock(0), position);
                Object groupValue = outputType.getObjectValue(page.getBlock(1), position);
                Object previous = gpuResult.put(groupKey, groupValue);
                assertThat(previous).as("Duplicate group key in GPU result: %s", groupKey).isNull();
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

    private static CompileResult compileAggregation(String functionName, List<Type> argumentTypes, boolean grouped, AggregationNode.Step step)
    {
        return compileAggregation(functionName, argumentTypes, grouped, step, false);
    }

    private static CompileResult compileAggregation(String functionName, List<Type> argumentTypes, boolean grouped, AggregationNode.Step step, boolean masked)
    {
        return tryCompileAggregation(functionName, argumentTypes, grouped, step, masked)
                .orElseThrow(() -> new AssertionError("Failed to compile %s over %s".formatted(functionName, argumentTypes)));
    }

    private static void assertCompileNotSupported(String functionName, List<Type> argumentTypes, boolean grouped)
    {
        for (AggregationNode.Step step : AggregationNode.Step.values()) {
            assertThat(tryCompileAggregation(functionName, argumentTypes, grouped, step, false)).isEmpty();
        }
    }

    private static Optional<CompileResult> tryCompileAggregation(String functionName, List<Type> argumentTypes, boolean grouped, AggregationNode.Step step, boolean masked)
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
            Symbol val = new Symbol(argumentTypes.getFirst(), "val");
            sourceSymbols.add(val);
            arguments = ImmutableList.of(new Reference(argumentTypes.getFirst(), "val"));
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

    private static List<Page> runGpuPipeline(Page inputPage, List<Type> inputTypes, CompileResult compiled)
    {
        return runGpuPipeline(List.of(inputPage), inputTypes, compiled);
    }

    private static List<Page> runGpuPipeline(List<Page> inputPages, List<Type> inputTypes, CompileResult compiled)
    {
        return executeGpuOperation(
                inputPages,
                inputTypes,
                compiled.finalOutputTypes(),
                copyToDevice -> {
                    GpuOperation current = copyToDevice;
                    for (GpuOperation.Factory factory : compiled.stages()) {
                        current = factory.create(current);
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
            result.put(entry.getKey(), perGroupAggregation.apply(groupPage));
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

    private Object executeGpuGlobalMergeAggregation(
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

    private List<Page> executeGpuMergeAggregation(
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
                copyToDevice -> {
                    GpuAggregation.Factory factory = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypesBuilder.build(), inputRaw, /*compactionThresholdBytes=*/ 1, inputTypes.size());
                    return factory.create(copyToDevice);
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

    private static Object runCpuFinal(String functionName, Type paramType, Block intermediate, int position)
    {
        return runCpuFinal(functionName, paramType, intermediate.getRegion(position, 1));
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
}
