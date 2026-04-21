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
import com.google.common.primitives.Ints;
import io.trino.operator.SimplePageWithPositionComparator;
import io.trino.operator.TopNProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;

final class TestGpuTopNOperator
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void testEmptyInput()
    {
        assertGpuTopNMatchesCpu(
                createEmptyPage(List.of(BIGINT)),
                List.of(BIGINT),
                10,
                new int[] {0},
                List.of(ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @MethodSource("nullsAndSortOrders")
    void testSingleColumn(NullsProvider nullsProvider, SortOrder sortOrder)
    {
        assertGpuTopNMatchesCpu(
                new Page(createBlock(BIGINT, 100, nullsProvider)),
                List.of(BIGINT),
                10,
                new int[] {0},
                List.of(sortOrder));
    }

    @Test
    void testLimitGreaterThanInput()
    {
        assertGpuTopNMatchesCpu(
                new Page(createBlock(BIGINT, 5, NO_NULLS)),
                List.of(BIGINT),
                100,
                new int[] {0},
                List.of(ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @MethodSource("nullsAndTwoSortOrders")
    void testMultipleColumns(NullsProvider nullsProvider, SortOrder firstSortOrder, SortOrder secondSortOrder)
    {
        assertGpuTopNMatchesCpu(
                new Page(
                        createBlock(BIGINT, 100, nullsProvider),
                        createBlock(BIGINT, 100, nullsProvider)),
                List.of(BIGINT, BIGINT),
                10,
                new int[] {0, 1},
                List.of(firstSortOrder, secondSortOrder));
    }

    @Test
    void testSortBySecondColumn()
    {
        assertGpuTopNMatchesCpu(
                new Page(
                        createBlock(BIGINT, 100, NO_NULLS),
                        createBlock(BIGINT, 100, NO_NULLS)),
                List.of(BIGINT, BIGINT),
                10,
                new int[] {1},
                List.of(ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testAllTypes(Type type)
    {
        assertGpuTopNMatchesCpu(
                new Page(createBlock(type, 100, NO_NULLS)),
                List.of(type),
                10,
                new int[] {0},
                List.of(ASC_NULLS_LAST));
    }

    static Stream<Arguments> nullsAndSortOrders()
    {
        return Arrays.stream(NullsProvider.values())
                .flatMap(nulls -> Arrays.stream(SortOrder.values())
                        .map(sort -> Arguments.of(nulls, sort)));
    }

    static Stream<Arguments> nullsAndTwoSortOrders()
    {
        return Arrays.stream(NullsProvider.values())
                .flatMap(nulls -> Arrays.stream(SortOrder.values())
                        .flatMap(sort1 -> Arrays.stream(SortOrder.values())
                                .map(sort2 -> Arguments.of(nulls, sort1, sort2))));
    }

    private static void assertGpuTopNMatchesCpu(Page inputPage, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        List<Page> gpuResults = executeGpuTopN(inputPage, types, limit, sortChannels, sortOrders);
        List<Page> cpuResults = executeCpuTopN(inputPage, types, limit, sortChannels, sortOrders);
        assertSameDataInOrder(gpuResults, cpuResults, types);
    }

    private static Page createEmptyPage(List<Type> types)
    {
        Block[] blocks = types.stream()
                .map(type -> type.createBlockBuilder(null, 0).build())
                .toArray(Block[]::new);
        return new Page(0, blocks);
    }

    private static List<Page> executeCpuTopN(Page inputPage, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = IntStream.of(sortChannels)
                .mapToObj(types::get)
                .toList();

        SimplePageWithPositionComparator comparator = new SimplePageWithPositionComparator(
                sortTypes,
                Ints.asList(sortChannels),
                sortOrders,
                TYPE_OPERATORS);

        TopNProcessor processor = new TopNProcessor(
                newSimpleAggregatedMemoryContext(),
                types,
                limit,
                comparator);

        processor.addInput(inputPage);

        ImmutableList.Builder<Page> results = ImmutableList.builder();
        while (!processor.noMoreOutput()) {
            Page output = processor.getOutput();
            if (output != null) {
                results.add(output);
            }
        }
        return results.build();
    }

    private static List<Page> executeGpuTopN(Page inputPage, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        return executeGpuOperation(
                List.of(inputPage),
                types,
                types,
                copyToDevice -> {
                    GpuTopN.Factory factory = new GpuTopN.Factory(limit, sortChannels, sortOrders);
                    return factory.create(copyToDevice);
                });
    }
}
