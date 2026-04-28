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
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.AggregationTestUtils;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.gpu.aggregation.GpuAggregateFunction;
import io.trino.operator.gpu.aggregation.GpuAggregation;
import io.trino.operator.gpu.aggregation.GpuCountAll;
import io.trino.operator.gpu.aggregation.GpuCountNonNull;
import io.trino.operator.gpu.aggregation.GpuMax;
import io.trino.operator.gpu.aggregation.GpuMin;
import io.trino.operator.gpu.aggregation.GpuSum;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.block.BlockAssertions.getOnlyValue;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final int GROUP_KEY_CHANNEL = 0;
    private static final int GROUP_VALUE_CHANNEL = 1;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testCountAllGlobalEmpty()
    {
        assertGpuMatchesCpu(
                createEmptyPage(List.of()),
                List.of(BIGINT),
                new GpuCountAll(BIGINT, INT64),
                "count",
                List.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountAllGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(BIGINT),
                new GpuCountAll(BIGINT, INT64),
                "count",
                List.of());
    }

    @Test
    void testGroupByCountAllEmpty()
    {
        assertGpuGroupByMatchesCpu(
                createEmptyPage(List.of(BIGINT, BIGINT)),
                BIGINT,
                BIGINT,
                new GpuCountAll(BIGINT, INT64),
                "count",
                List.of());
    }

    @Test
    void testGroupByCountAll()
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(BIGINT, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);

        assertGpuGroupByMatchesCpu(
                inputPage,
                BIGINT,
                BIGINT,
                new GpuCountAll(BIGINT, INT64),
                "count",
                List.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCountNonNullGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 0, 100);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(BIGINT),
                new GpuCountNonNull(0, BIGINT, INT64),
                "count",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @MethodSource("allConvertibleTypes")
    void testCountNonNullForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(type),
                new GpuCountNonNull(0, BIGINT, INT64),
                "count",
                List.of(type));
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
    @MethodSource("allConvertibleTypes")
    void testGroupByCountNonNullForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);

        assertGpuGroupByMatchesCpu(
                inputPage,
                BIGINT,
                type,
                new GpuCountNonNull(1, BIGINT, INT64),
                "count",
                List.of(type));
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
        assertGpuMatchesCpu(
                createEmptyPage(List.of(BIGINT)),
                List.of(BIGINT),
                new GpuSum(0, BIGINT, INT64),
                "sum",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSumGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, 1, 10);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(BIGINT),
                new GpuSum(0, BIGINT, INT64),
                "sum",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @MethodSource("sumSupportedTypes")
    void testSumForAllTypes(Type type)
    {
        Block block = createInputBlockForSum(type, 100);
        Type outputType = sumOutputType(type);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(type),
                new GpuSum(0, outputType, toDType(outputType).orElseThrow()),
                "sum",
                List.of(type));
    }

    @ParameterizedTest
    @MethodSource("sumSupportedTypes")
    void testGroupBySumForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createInputBlockForSum(type, 100);
        Page inputPage = new Page(groupByBlock, valueBlock);
        Type outputType = sumOutputType(type);

        assertGpuGroupByMatchesCpu(
                inputPage,
                BIGINT,
                type,
                new GpuSum(1, outputType, toDType(outputType).orElseThrow()),
                "sum",
                List.of(type));
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
        assertGpuMatchesCpu(
                createEmptyPage(List.of(BIGINT)),
                List.of(BIGINT),
                new GpuMin(0, BIGINT, INT64),
                "min",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMinGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(BIGINT),
                new GpuMin(0, BIGINT, INT64),
                "min",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @MethodSource("allConvertibleTypes")
    void testMinForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(type),
                new GpuMin(0, type, toDType(type).orElseThrow()),
                "min",
                List.of(type));
    }

    @ParameterizedTest
    @MethodSource("allConvertibleTypes")
    void testGroupByMinForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);

        assertGpuGroupByMatchesCpu(
                inputPage,
                BIGINT,
                type,
                new GpuMin(1, type, toDType(type).orElseThrow()),
                "min",
                List.of(type));
    }

    @Test
    void testMaxGlobalEmpty()
    {
        assertGpuMatchesCpu(
                createEmptyPage(List.of(BIGINT)),
                List.of(BIGINT),
                new GpuMax(0, BIGINT, INT64),
                "max",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testMaxGlobal(NullsProvider nullsProvider)
    {
        Block block = createBigintBlock(100, nullsProvider, -1000, 1000);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(BIGINT),
                new GpuMax(0, BIGINT, INT64),
                "max",
                List.of(BIGINT));
    }

    @ParameterizedTest
    @MethodSource("allConvertibleTypes")
    void testMaxForAllTypes(Type type)
    {
        Block block = createBlock(type, 100, RANDOM_NULLS);

        assertGpuMatchesCpu(
                new Page(block),
                List.of(type),
                new GpuMax(0, type, toDType(type).orElseThrow()),
                "max",
                List.of(type));
    }

    @ParameterizedTest
    @MethodSource("allConvertibleTypes")
    void testGroupByMaxForAllTypes(Type type)
    {
        Block groupByBlock = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, RANDOM_NULLS);
        Page inputPage = new Page(groupByBlock, valueBlock);

        assertGpuGroupByMatchesCpu(
                inputPage,
                BIGINT,
                type,
                new GpuMax(1, type, toDType(type).orElseThrow()),
                "max",
                List.of(type));
    }

    static Stream<Type> allConvertibleTypes()
    {
        return Stream.of(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARCHAR);
    }

    static Stream<Type> sumSupportedTypes()
    {
        return Stream.of(BIGINT, DOUBLE);
    }

    private static Type sumOutputType(Type inputType)
    {
        if (inputType == TINYINT || inputType == SMALLINT || inputType == INTEGER || inputType == BIGINT) {
            return BIGINT;
        }
        if (inputType == REAL || inputType == DOUBLE) {
            return DOUBLE;
        }
        throw new IllegalArgumentException("Unsupported type for sum: " + inputType);
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

    private void assertGpuMatchesCpu(
            Page inputPage,
            List<Type> inputTypes,
            GpuAggregateFunction gpuAggregate,
            String cpuFunctionName,
            List<Type> cpuParamTypes)
    {
        Object gpuResult = executeGpuGlobalAggregation(inputPage, inputTypes, gpuAggregate);
        assertAggregation(FUNCTION_RESOLUTION, cpuFunctionName, fromTypes(cpuParamTypes), gpuResult, inputPage);
    }

    private void assertGpuGroupByMatchesCpu(
            Page inputPage,
            Type groupByKeyType,
            Type groupByValueType,
            GpuAggregateFunction gpuAggregate,
            String cpuFunctionName,
            List<Type> cpuParamTypes)
    {
        Map<Object, Object> gpuResult = executeGpuGroupByAggregation(inputPage, groupByKeyType, groupByValueType, gpuAggregate);
        Map<Object, Object> cpuResult = executeCpuGroupByAggregation(inputPage, groupByKeyType, cpuFunctionName, cpuParamTypes);

        assertThat(gpuResult.keySet()).isEqualTo(cpuResult.keySet());
        for (Object groupKey : gpuResult.keySet()) {
            Object gpuValue = gpuResult.get(groupKey);
            Object cpuValue = cpuResult.get(groupKey);
            assertThat(AggregationTestUtils.makeValidityAssertion(cpuValue).apply(gpuValue, cpuValue))
                    .as("Group %s: expected %s but was %s", groupKey, cpuValue, gpuValue)
                    .isTrue();
        }
    }

    private Map<Object, Object> executeCpuGroupByAggregation(
            Page inputPage,
            Type groupByType,
            String cpuFunctionName,
            List<Type> cpuParamTypes)
    {
        Block groupBlock = inputPage.getBlock(GROUP_KEY_CHANNEL);

        if (cpuParamTypes.isEmpty()) {
            // COUNT(*) case - just count rows per group
            Map<Object, Integer> groupCounts = new HashMap<>();
            for (int i = 0; i < inputPage.getPositionCount(); i++) {
                Object groupKey = groupByType.getObjectValue(groupBlock, i);
                groupCounts.merge(groupKey, 1, Integer::sum);
            }
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<Object, Integer> entry : groupCounts.entrySet()) {
                Page groupPage = createBigintNullsPage(entry.getValue());
                result.put(entry.getKey(), computeCpuAggregation(cpuFunctionName, cpuParamTypes, groupPage));
            }
            return result;
        }

        Block valueBlock = inputPage.getBlock(GROUP_VALUE_CHANNEL);
        Type valueType = cpuParamTypes.getFirst();
        Map<Object, BlockBuilder> groupBuilders = new HashMap<>();

        for (int i = 0; i < inputPage.getPositionCount(); i++) {
            Object groupKey = groupByType.getObjectValue(groupBlock, i);
            BlockBuilder builder = groupBuilders.computeIfAbsent(groupKey, _ -> valueType.createBlockBuilder(null, 16));
            if (valueBlock.isNull(i)) {
                builder.appendNull();
            }
            else {
                builder.append(valueBlock.getUnderlyingValueBlock(), valueBlock.getUnderlyingValuePosition(i));
            }
        }

        Map<Object, Object> result = new HashMap<>();
        for (Map.Entry<Object, BlockBuilder> entry : groupBuilders.entrySet()) {
            Page groupPage = new Page(entry.getValue().build());
            result.put(entry.getKey(), computeCpuAggregation(cpuFunctionName, cpuParamTypes, groupPage));
        }
        return result;
    }

    private static Object computeCpuAggregation(String functionName, List<Type> paramTypes, Page inputPage)
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(functionName, fromTypes(paramTypes));
        return AggregationTestUtils.aggregation(function, inputPage);
    }

    private static Page createBigintNullsPage(int positionCount)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            builder.appendNull();
        }
        return new Page(builder.build());
    }

    private Object executeGpuGlobalAggregation(
            Page inputPage,
            List<Type> inputTypes,
            GpuAggregateFunction gpuAggregate)
    {
        List<Page> results = executeGpuAggregation(inputPage, inputTypes, Optional.empty(), List.of(gpuAggregate), true);

        checkState(results.size() == 1, "Expected single result page");
        Page resultPage = results.getFirst();
        checkState(resultPage.getPositionCount() == 1, "Expected single row");
        return getOnlyValue(gpuAggregate.outputType(), resultPage.getBlock(0));
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

    private Map<Object, Object> executeGpuGroupByAggregation(
            Page inputPage,
            Type groupByKeyType,
            Type groupByValueType,
            GpuAggregateFunction gpuAggregate)
    {
        List<Page> pages = executeGpuAggregation(
                inputPage,
                List.of(groupByKeyType, groupByValueType),
                Optional.of(groupByKeyType),
                List.of(gpuAggregate),
                true);

        Map<Object, Object> result = new HashMap<>();

        for (Page page : pages) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Object groupKey = groupByKeyType.getObjectValue(page.getBlock(GROUP_KEY_CHANNEL), position);
                Object groupValue = gpuAggregate.outputType().getObjectValue(page.getBlock(GROUP_VALUE_CHANNEL), position);
                Object previous = result.put(groupKey, groupValue);
                assertThat(previous).as("Duplicate group key in GPU result: %s", groupKey).isNull();
            }
        }

        return result;
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
                    GpuAggregation.Factory factory = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypesBuilder.build(), inputRaw);
                    return factory.create(copyToDevice);
                });
    }
}
