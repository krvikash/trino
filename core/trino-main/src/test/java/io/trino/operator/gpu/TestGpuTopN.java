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
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.trino.operator.SimplePageWithPositionComparator;
import io.trino.operator.TopNProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;

final class TestGpuTopN
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

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
    @MethodSource("singleColumnTestCases")
    void testSingleColumn(Type type, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        assertGpuTopNMatchesCpu(
                new Page(createBlock(type, 100, nullsProvider)),
                List.of(type),
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
    @MethodSource("twoColumnsTestCases")
    void testMultipleColumns(NullsProvider nullsProvider, Type firstType, SortOrder firstSortOrder, Type secondType, SortOrder secondSortOrder)
    {
        assertGpuTopNMatchesCpu(
                new Page(
                        createBlock(firstType, 100, nullsProvider),
                        createBlock(secondType, 100, nullsProvider)),
                List.of(firstType, secondType),
                10,
                new int[] {0, 1},
                List.of(firstSortOrder, secondSortOrder));
    }

    @ParameterizedTest
    @MethodSource("singleColumnTestCases")
    void testMultipleBatches(Type type, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(type, TARGET_ROW_COUNT, nullsProvider)),
                new Page(createBlock(type, TARGET_ROW_COUNT, nullsProvider)),
                new Page(createBlock(type, TARGET_ROW_COUNT, nullsProvider)));
        assertGpuTopNMatchesCpu(
                inputPages,
                List.of(type),
                10,
                new int[] {0},
                List.of(sortOrder));
    }

    @Test
    void testMultipleBatchesWithLimitGreaterThanBatchSize()
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, NO_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, NO_NULLS)),
                new Page(createBlock(BIGINT, TARGET_ROW_COUNT, NO_NULLS)));
        assertGpuTopNMatchesCpu(
                inputPages,
                List.of(BIGINT),
                200,
                new int[] {0},
                List.of(ASC_NULLS_LAST));
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
    @EnumSource(SortOrder.class)
    void testFloatingPointEdgeCases(SortOrder sortOrder)
    {
        // cuDF sorts NaN after all real values. Trino uses COMPARISON_UNORDERED_FIRST for
        // NULLS_FIRST sort orders, placing NaN before real values (right after null).
        // NaN bit patterns don't matter here: DoubleType.writeDouble uses doubleToLongBits
        // which normalizes all NaN representations to canonical NaN before they reach the GPU.
        BlockBuilder doubleBuilder = DOUBLE.createFixedSizeBlockBuilder(8);
        DOUBLE.writeDouble(doubleBuilder, -0.0);
        DOUBLE.writeDouble(doubleBuilder, 0.0);
        DOUBLE.writeDouble(doubleBuilder, Double.NaN);
        DOUBLE.writeDouble(doubleBuilder, Double.NaN);
        DOUBLE.writeDouble(doubleBuilder, 1.0);
        DOUBLE.writeDouble(doubleBuilder, Double.NEGATIVE_INFINITY);
        DOUBLE.writeDouble(doubleBuilder, Double.POSITIVE_INFINITY);
        doubleBuilder.appendNull();
        Page doublePage = new Page(doubleBuilder.buildValueBlock());
        // limit=8 exercises full sort; limit=5 exercises truncation interacting with NaN position
        for (int limit : List.of(5, 8)) {
            assertGpuTopNMatchesCpu(
                    doublePage,
                    List.of(DOUBLE),
                    limit,
                    new int[] {0},
                    List.of(sortOrder));
        }

        BlockBuilder realBuilder = REAL.createFixedSizeBlockBuilder(8);
        REAL.writeFloat(realBuilder, -0.0f);
        REAL.writeFloat(realBuilder, 0.0f);
        REAL.writeFloat(realBuilder, Float.NaN);
        REAL.writeFloat(realBuilder, Float.NaN);
        REAL.writeFloat(realBuilder, 1.0f);
        REAL.writeFloat(realBuilder, Float.NEGATIVE_INFINITY);
        REAL.writeFloat(realBuilder, Float.POSITIVE_INFINITY);
        realBuilder.appendNull();
        Page realPage = new Page(realBuilder.buildValueBlock());
        for (int limit : List.of(5, 8)) {
            assertGpuTopNMatchesCpu(
                    realPage,
                    List.of(REAL),
                    limit,
                    new int[] {0},
                    List.of(sortOrder));
        }
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    void testFloatingPointNanAcrossMultipleBatches(SortOrder sortOrder)
    {
        // NaN values spread across separate pages exercise the sort-and-truncate loop:
        // intermediate truncation must not discard NaN values that belong in the final result.
        List<Page> inputPages = List.of(
                new Page(createBlock(DOUBLE, TARGET_ROW_COUNT, NO_NULLS)),
                new Page(createBlock(DOUBLE, TARGET_ROW_COUNT, NO_NULLS)),
                new Page(createBlock(DOUBLE, TARGET_ROW_COUNT, NO_NULLS)));
        assertGpuTopNMatchesCpu(
                inputPages,
                List.of(DOUBLE),
                10,
                new int[] {0},
                List.of(sortOrder));
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    void testFloatingPointNanAsSecondarySortKey(SortOrder sortOrder)
    {
        // Float sort key as secondary column exercises the channel index mapping
        // in the NaN flag insertion logic (flags must be placed before their float column,
        // not before the first sort key).
        BlockBuilder bigintBuilder = BIGINT.createBlockBuilder(null, 10);
        BlockBuilder doubleBuilder = DOUBLE.createBlockBuilder(null, 10);
        // Two groups of BIGINT=1 and BIGINT=2, with NaN/null in both groups
        for (long group : new long[] {1, 2}) {
            BIGINT.writeLong(bigintBuilder, group);
            DOUBLE.writeDouble(doubleBuilder, Double.NaN);
            BIGINT.writeLong(bigintBuilder, group);
            DOUBLE.writeDouble(doubleBuilder, 3.0);
            BIGINT.writeLong(bigintBuilder, group);
            DOUBLE.writeDouble(doubleBuilder, -1.0);
            BIGINT.writeLong(bigintBuilder, group);
            doubleBuilder.appendNull();
            BIGINT.writeLong(bigintBuilder, group);
            DOUBLE.writeDouble(doubleBuilder, Double.POSITIVE_INFINITY);
        }
        assertGpuTopNMatchesCpu(
                new Page(bigintBuilder.build(), doubleBuilder.build()),
                List.of(BIGINT, DOUBLE),
                7,
                new int[] {0, 1},
                List.of(SortOrder.ASC_NULLS_FIRST, sortOrder));
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    void testCharPaddedSemantics(SortOrder sortOrder)
    {
        Type charType = createCharType(2);
        BlockBuilder orderBuilder = charType.createBlockBuilder(null, 4);
        // U+0001 is less than space (U+0020), so naive byte comparison would produce wrong order without proper CHAR padding
        for (String value : List.of("a", "a", "a\u0001", "a\u0001")) {
            charType.writeSlice(orderBuilder, utf8Slice(value));
        }

        assertGpuTopNMatchesCpu(
                List.of(new Page(orderBuilder.build())),
                List.of(charType),
                10,
                new int[] {0},
                List.of(sortOrder));
    }

    static Stream<Object[]> singleColumnTestCases()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_TYPES,
                        ImmutableList.copyOf(NullsProvider.values()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    static Stream<Object[]> twoColumnsTestCases()
    {
        return Lists.cartesianProduct(
                        ImmutableList.copyOf(NullsProvider.values()),
                        TESTED_GPU_TYPES,
                        ImmutableList.copyOf(SortOrder.values()),
                        TESTED_GPU_TYPES,
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    private static void assertGpuTopNMatchesCpu(Page inputPage, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        assertGpuTopNMatchesCpu(List.of(inputPage), types, limit, sortChannels, sortOrders);
    }

    private static void assertGpuTopNMatchesCpu(List<Page> inputPages, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        List<Page> gpuResults = executeGpuTopN(inputPages, types, limit, sortChannels, sortOrders);
        List<Page> cpuResults = executeCpuTopN(inputPages, types, limit, sortChannels, sortOrders);
        assertSameDataInOrder(gpuResults, cpuResults, types);
    }

    private static Page createEmptyPage(List<Type> types)
    {
        Block[] blocks = types.stream()
                .map(type -> type.createBlockBuilder(null, 0).build())
                .toArray(Block[]::new);
        return new Page(0, blocks);
    }

    private static List<Page> executeCpuTopN(List<Page> inputPages, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
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

        inputPages.forEach(processor::addInput);

        ImmutableList.Builder<Page> results = ImmutableList.builder();
        while (!processor.noMoreOutput()) {
            Page output = processor.getOutput();
            if (output != null) {
                results.add(output);
            }
        }
        return results.build();
    }

    private static List<Page> executeGpuTopN(List<Page> inputPages, List<Type> types, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = IntStream.of(sortChannels)
                .mapToObj(types::get)
                .toList();
        return executeGpuOperation(
                inputPages,
                types,
                types,
                (context, copyToDevice) -> {
                    GpuTopN.Factory factory = new GpuTopN.Factory(limit, sortChannels, sortOrders, sortTypes);
                    return factory.create(context, copyToDevice);
                });
    }
}
