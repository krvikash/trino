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
import io.trino.operator.window.FrameInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
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
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.gpu.GpuTestUtils.drainToPages;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.mergePages;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.TESTED_GPU_WINDOW_FUNCTIONS;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.compileWindow;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.createOperation;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindow
{
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduler-%s"));
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
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
    @MethodSource("baseTestCases")
    void testEmptyInput(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(2)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, 0),
                    types,
                    new int[] {0},
                    new int[] {1},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("baseTestCases")
    void testSingleInputRow(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(2)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, 1),
                    types,
                    new int[] {0},
                    new int[] {1},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("baseTestCases")
    void testSinglePartitionKeyAndOrderBy(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(2)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, POSITION_COUNT),
                    types,
                    new int[] {0},
                    new int[] {1},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("baseTestCases")
    void testNoPartitionBy(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(1)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, POSITION_COUNT),
                    types,
                    new int[0],
                    new int[] {0},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("noOrderByTestCases")
    void testNoOrderBy(String function, NullsProvider nullsProvider)
    {
        for (List<Type> types : typesTestCases(1)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, POSITION_COUNT),
                    types,
                    new int[] {0},
                    new int[0],
                    List.of(),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.window.GpuWindowTestUtils#TESTED_GPU_WINDOW_FUNCTIONS")
    void testNoSourceColumns(String function)
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(5)),
                List.of(),
                new int[0],
                new int[0],
                List.of(),
                List.of(function));
    }

    @ParameterizedTest
    @MethodSource("fourColumnsWithNullProviderAndSortOrder")
    void testMultiColumnKeys(String function, NullsProvider nullsProvider, SortOrder firstSortOrder, SortOrder secondSortOrder)
    {
        for (List<Type> types : typesTestCases(4)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 1, POSITION_COUNT),
                    types,
                    new int[] {0, 1},
                    new int[] {2, 3},
                    List.of(firstSortOrder, secondSortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("baseTestCases")
    void testTies(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(2)) {
            Type partitionType = types.get(0);
            Type orderType = types.get(1);

            Block orderByBlock = createBlock(orderType, POSITION_COUNT, nullsProvider);
            Page page = mergePages(
                    List.of(
                            new Page(createBlock(partitionType, POSITION_COUNT, nullsProvider), orderByBlock),
                            new Page(createBlock(partitionType, POSITION_COUNT, nullsProvider), orderByBlock),
                            new Page(createBlock(partitionType, POSITION_COUNT, nullsProvider), orderByBlock)),
                    types);
            assertGpuWindowMatchesCpu(
                    List.of(page),
                    types,
                    new int[] {0},
                    new int[] {1},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @ParameterizedTest
    @MethodSource("multiplePagesTestCases")
    void testMultipleInputPages(String function, NullsProvider nullsProvider, SortOrder sortOrder)
    {
        for (List<Type> types : typesTestCases(2)) {
            assertGpuWindowMatchesCpu(
                    createPages(types, nullsProvider, 3, TARGET_ROW_COUNT),
                    types,
                    new int[] {0},
                    new int[] {1},
                    List.of(sortOrder),
                    List.of(function));
        }
    }

    @Test
    void testMultipleFunctions()
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createBlock(BIGINT, 100, NullsProvider.RANDOM_NULLS),
                        createBlock(BIGINT, 100, NullsProvider.RANDOM_NULLS))),
                List.of(BIGINT, BIGINT),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                TESTED_GPU_WINDOW_FUNCTIONS);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.window.GpuWindowTestUtils#TESTED_GPU_WINDOW_FUNCTIONS")
    void testSortPreservesNonKeyColumns(String function)
    {
        int positionCount = 100;
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createBigintBlock(positionCount, NO_NULLS, 1L, 3L),
                        createBigintBlock(positionCount, NO_NULLS, 10L, 20L),
                        createLongSequenceBlock(0, positionCount))),
                List.of(BIGINT, BIGINT, BIGINT),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(function));
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.window.GpuWindowTestUtils#TESTED_GPU_WINDOW_FUNCTIONS")
    void testCharKeysUsePaddedSemantics(String function)
    {
        Type charType = createCharType(2);
        BlockBuilder charBlockBuilder = charType.createBlockBuilder(null, 4);
        for (String value : List.of("a", "a", "a\u0001", "a\u0001")) {
            charType.writeSlice(charBlockBuilder, utf8Slice(value));
        }

        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createLongsBlock(1L, 1L, 1L, 1L),
                        charBlockBuilder.build())),
                List.of(BIGINT, charType),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(function));

        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        charBlockBuilder.build(),
                        createLongsBlock(1L, 2L, 3L, 4L))),
                List.of(charType, BIGINT),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                List.of(function));
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
                List.of("rank", "dense_rank"));
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
                List.of("rank", "dense_rank"));
    }

    @ParameterizedTest
    @MethodSource("floatingPointTestCases")
    void testOrderByFloatingPointEdgeCases(String function, Type type, SortOrder sortOrder, List<Double> values)
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createLongsBlock(Collections.nCopies(values.size(), 1L)),
                        createFloatingPointBlock(type, values))),
                List.of(BIGINT, type),
                new int[] {0},
                new int[] {1},
                List.of(sortOrder),
                List.of(function));
    }

    @ParameterizedTest
    @MethodSource("floatingPointTestCases")
    void testPartitionByFloatingPointEdgeCases(String function, Type type, SortOrder sortOrder, List<Double> values)
    {
        assertGpuWindowMatchesCpu(
                List.of(new Page(
                        createFloatingPointBlock(type, values),
                        createLongsBlock(LongStream.range(0, values.size()).boxed().toList()))),
                List.of(type, BIGINT),
                new int[] {0},
                new int[] {1},
                List.of(sortOrder),
                List.of(function));
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
                List.of("rank", "rank", "dense_rank", "dense_rank", "row_number", "row_number"));
    }

    static Stream<Object[]> baseTestCases()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_WINDOW_FUNCTIONS,
                        ImmutableList.copyOf(NullsProvider.values()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    static Stream<Object[]> noOrderByTestCases()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_WINDOW_FUNCTIONS,
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    static Stream<Object[]> multiplePagesTestCases()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_WINDOW_FUNCTIONS,
                        ImmutableList.copyOf(NullsProvider.values()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
    }

    static Stream<Object[]> fourColumnsWithNullProviderAndSortOrder()
    {
        return Lists.cartesianProduct(
                        TESTED_GPU_WINDOW_FUNCTIONS,
                        ImmutableList.copyOf(NullsProvider.values()),
                        ImmutableList.copyOf(SortOrder.values()),
                        ImmutableList.copyOf(SortOrder.values()))
                .stream()
                .map(List::toArray);
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

    private static List<List<Type>> typesTestCases(int typesPerTestCase)
    {
        Random random = new Random(42);
        ImmutableList.Builder<List<Type>> builder = ImmutableList.builder();
        for (Type type : TESTED_GPU_TYPES) {
            ImmutableList.Builder<Type> testCase = ImmutableList.builder();
            testCase.add(type);
            for (int i = 1; i < typesPerTestCase; i++) {
                int index = random.nextInt(TESTED_GPU_TYPES.size());
                testCase.add(TESTED_GPU_TYPES.get(index));
            }
            builder.add(testCase.build());
        }
        return builder.build();
    }

    private static List<Page> createPages(List<Type> types, NullsProvider nullsProvider, int pageCount, int positionsPerPage)
    {
        return ImmutableList.copyOf(GpuTestUtils.createPages(types, nullsProvider, pageCount, positionsPerPage, true));
    }

    private static void assertGpuWindowMatchesCpu(
            List<Page> inputPages,
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<String> functions)
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

    private static boolean isUnsupported(List<String> functions, int[] orderChannels, List<Type> sourceTypes)
    {
        return containsType(orderChannels, sourceTypes, VARBINARY) && containsFunction(functions, "rank", "dense_rank");
    }

    private static boolean containsType(int[] orderChannels, List<Type> sourceTypes, Type... types)
    {
        Set<Type> typeSet = ImmutableSet.copyOf(types);
        return Arrays.stream(orderChannels).mapToObj(sourceTypes::get).anyMatch(typeSet::contains);
    }

    private static boolean containsFunction(List<String> functions, String... functionNames)
    {
        Set<String> functionNamesSet = ImmutableSet.copyOf(functionNames);
        return functions.stream().anyMatch(functionNamesSet::contains);
    }

    private static void assertSameWindowDataWithoutOrder(
            List<Page> actual,
            List<Page> expected,
            List<Type> outputTypes,
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<String> functions)
    {
        List<Integer> rowNumberOutputChannels = IntStream.range(0, functions.size())
                .filter(functionIndex -> functions.get(functionIndex).equals("row_number"))
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
            List<String> functions)
    {
        List<Integer> outputChannels = IntStream.range(0, sourceTypes.size())
                .boxed()
                .collect(toImmutableList());
        ImmutableList.Builder<WindowFunctionDefinition> functionDefinitions = ImmutableList.builder();
        for (String name : functions) {
            functionDefinitions.add(windowFunctionDefinition(name));
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

    private static WindowFunctionDefinition windowFunctionDefinition(String name)
    {
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(name, ImmutableList.of());
        FunctionManager functionManager = FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager();
        verify(resolvedFunction.functionKind() == WINDOW, "Unsupported function kind: %s", resolvedFunction.functionKind());
        WindowFunctionSupplier supplier = functionManager.getWindowFunctionSupplier(resolvedFunction);
        return WindowFunctionDefinition.window(supplier, resolvedFunction.signature().getReturnType(), UNBOUNDED_FRAME, false, ImmutableList.of());
    }
}
