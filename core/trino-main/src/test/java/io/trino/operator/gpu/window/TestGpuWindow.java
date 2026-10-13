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
import com.google.common.primitives.Ints;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverContext;
import io.trino.operator.DummySpillerFactory;
import io.trino.operator.TestWindowOperator;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuTestUtils;
import io.trino.operator.gpu.window.GpuWindowCompiler.CompileResult;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.gpu.BufferPages.TARGET_ROW_COUNT;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataWithoutOrder;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.TESTED_GPU_WINDOW_FUNCTIONS;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.compileWindow;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.createOperation;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

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

    static Stream<WindowFunctionTestCase> twoColumns()
    {
        return allTypesAndFunctionsTestCases(2).stream();
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
        return executeGpuOperation(
                inputPages,
                sourceTypes,
                outputTypes,
                (context, source) -> createOperation(context, source, stages));
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
