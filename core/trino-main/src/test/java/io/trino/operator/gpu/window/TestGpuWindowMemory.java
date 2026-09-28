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
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.GpuProject;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.operator.gpu.window.GpuWindowCompiler.CompileResult;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.TESTED_GPU_WINDOW_FUNCTIONS;
import static io.trino.operator.gpu.window.GpuWindowTestUtils.compileWindow;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
final class TestGpuWindowMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int MULTIPLE_PAGES = 4;
    private static final int MEGABYTE = 1024 * 1024;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("singlePartitionKeyAndOrderByTestCases")
    void testSinglePartitionKeyAndOrderBy(WindowFunctionTestCase testCase)
    {
        testWindow(
                testCase,
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @MethodSource("multipleOrderByTestCases")
    void testMultipleOrderBy(WindowFunctionTestCase testCase)
    {
        testWindow(
                testCase,
                new int[] {0},
                new int[] {1, 2},
                List.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                1);
    }

    @ParameterizedTest
    @MethodSource("multiplePartitionByTestCases")
    void testMultiplePartitionBy(WindowFunctionTestCase testCase)
    {
        testWindow(
                testCase,
                new int[] {0, 1},
                new int[] {2},
                List.of(ASC_NULLS_LAST),
                1);
    }

    @ParameterizedTest
    @MethodSource("noPartitionByTestCases")
    void testNoPartitionBy(WindowFunctionTestCase testCase)
    {
        testWindow(
                testCase,
                new int[] {},
                new int[] {0},
                List.of(ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @MethodSource("noOrderByTestCases")
    void testNoOrderBy(WindowFunctionTestCase testCase)
    {
        if ("avg".equals(testCase.function().name())) {
            testWindow(
                    testCase.sourceTypes(),
                    new int[] {0},
                    new int[] {},
                    List.of(),
                    List.of(testCase.function()),
                    1,
                    MEGABYTE);
            testWindow(
                    testCase.sourceTypes(),
                    new int[] {0},
                    new int[] {},
                    List.of(),
                    List.of(testCase.function()),
                    MULTIPLE_PAGES,
                    // Larger margin for a partitioned avg without order by, because its estimate charges one partition per row, an upper bound that
                    // the multi-page input does not reach: every page repeats the same keys, which leaves about a quarter as many partitions as rows.
                    4 * MEGABYTE);
        }
        else {
            testWindow(
                    testCase,
                    new int[] {0},
                    new int[] {},
                    List.of());
        }
    }

    @Test
    void testNoOrderByMultiplePartitionBy()
    {
        // Two partition keys make the unique keys the groupby returns wider than the aggregated column.
        testWindow(
                List.of(BIGINT, BIGINT, createDecimalType(28, 2)),
                new int[] {0, 1},
                new int[] {},
                List.of(),
                List.of(BoundTestedWindowFunction.of("avg", 2)),
                1,
                MEGABYTE);
    }

    @Test
    void testSingleSortKeyWideTable()
    {
        // The gather of a wide table through the sort order exceeds the radix sort working memory of a narrow key
        testWindow(
                List.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT),
                new int[] {},
                new int[] {0},
                List.of(ASC_NULLS_LAST),
                List.of(BoundTestedWindowFunction.of("row_number")));
    }

    @Test
    void testMultipleFunctions()
    {
        ImmutableList.Builder<Type> sourceTypes = ImmutableList.builder();
        sourceTypes.add(BIGINT, BIGINT);
        ImmutableList.Builder<BoundTestedWindowFunction> functions = ImmutableList.builder();
        int nextArgumentChannel = 2;
        for (TestedWindowFunction function : TESTED_GPU_WINDOW_FUNCTIONS) {
            int[] argumentChannels = new int[function.argumentTypes().size()];
            for (int argument = 0; argument < function.argumentTypes().size(); argument++) {
                sourceTypes.add(function.argumentTypes().get(argument));
                argumentChannels[argument] = nextArgumentChannel++;
            }
            functions.add(BoundTestedWindowFunction.of(function, argumentChannels));
        }
        testWindow(
                sourceTypes.build(),
                new int[] {0},
                new int[] {1},
                List.of(ASC_NULLS_LAST),
                functions.build());
    }

    private void testWindow(
            WindowFunctionTestCase testCase,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders)
    {
        testWindow(
                testCase,
                partitionChannels,
                orderChannels,
                orderSortOrders,
                1);
        testWindow(
                testCase,
                partitionChannels,
                orderChannels,
                orderSortOrders,
                MULTIPLE_PAGES);
    }

    private void testWindow(
            WindowFunctionTestCase testCase,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            int pageCount)
    {
        testWindow(
                testCase.sourceTypes(),
                partitionChannels,
                orderChannels,
                orderSortOrders,
                List.of(testCase.function()),
                pageCount,
                MEGABYTE);
    }

    static Stream<WindowFunctionTestCase> singlePartitionKeyAndOrderByTestCases()
    {
        return allTypesAndFunctionsTestCases(
                List.of(
                        List.of(BIGINT, BIGINT),
                        List.of(VARCHAR, VARCHAR)));
    }

    static Stream<WindowFunctionTestCase> multipleOrderByTestCases()
    {
        return allTypesAndFunctionsTestCases(
                List.of(
                        List.of(BIGINT, BIGINT, BIGINT),
                        List.of(BIGINT, VARCHAR, VARCHAR),
                        List.of(BIGINT, VARCHAR, BIGINT)));
    }

    static Stream<WindowFunctionTestCase> multiplePartitionByTestCases()
    {
        return allTypesAndFunctionsTestCases(
                List.of(
                        List.of(BIGINT, BIGINT, BIGINT),
                        List.of(VARCHAR, VARCHAR, BIGINT),
                        List.of(BIGINT, VARCHAR, VARCHAR)));
    }

    static Stream<WindowFunctionTestCase> noPartitionByTestCases()
    {
        return allTypesAndFunctionsTestCases(List.of(List.of(BIGINT)));
    }

    static Stream<WindowFunctionTestCase> noOrderByTestCases()
    {
        return allTypesAndFunctionsTestCases(List.of(List.of(BIGINT)));
    }

    private static Stream<WindowFunctionTestCase> allTypesAndFunctionsTestCases(List<List<Type>> sourceTypes)
    {
        return TESTED_GPU_WINDOW_FUNCTIONS.stream()
                .flatMap(function -> sourceTypes.stream()
                        .map(types -> {
                            // argument channels follow the key channels
                            int[] argumentChannels = IntStream.range(types.size(), types.size() + function.argumentTypes().size()).toArray();
                            List<Type> finalSourceTypes = Stream.concat(types.stream(), function.argumentTypes().stream()).toList();
                            return new WindowFunctionTestCase(BoundTestedWindowFunction.of(function, argumentChannels), finalSourceTypes);
                        }));
    }

    private void testWindow(
            List<Type> inputTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<BoundTestedWindowFunction> functions)
    {
        testWindow(inputTypes, partitionChannels, orderChannels, orderSortOrders, functions, 1, MEGABYTE);
        testWindow(inputTypes, partitionChannels, orderChannels, orderSortOrders, functions, MULTIPLE_PAGES, MEGABYTE);
    }

    private void testWindow(
            List<Type> inputTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<BoundTestedWindowFunction> functions,
            int pageCount,
            long marginBytes)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, marginBytes)) {
            Iterator<Page> inputPages = createPages(inputTypes, NO_NULLS, pageCount, ROWS_PER_PAGE, true);

            CompileResult result = compileWindow(inputTypes, partitionChannels, orderChannels, orderSortOrders, functions);
            List<GpuOperation.Factory> stages = switch (result) {
                case CompileResult.Success success -> success.stages();
                case CompileResult.Failure failure -> throw new IllegalArgumentException(failure.reason());
            };
            try (var source = new SettableGpuOperation();
                    GpuOperation operation = createWindowOperation(context, source, stages, memoryValidation)) {
                boolean finished = false;
                while (!finished) {
                    switch (operation.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), inputTypes));
                                        source.setPending(context.taskMemoryContext().allocate(getClass().getSimpleName(), next.retainedMemory()), next);
                                    }
                                    else {
                                        source.noMoreInput();
                                    }
                                });
                            }
                        }
                        case Data(var memory, var page) -> {
                            try (memory; page) {
                                // result consumed; memory accounting is the only thing under test
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }
        }
    }

    // Builds a pipeline from the compiled stages, ensuring only GpuWindow memory reservations are validated.
    // The post-projection is dropped, and any pre-projection is wrapped to bypass validation.
    private static GpuOperation createWindowOperation(
            GpuOperation.Context context,
            GpuOperation source,
            List<GpuOperation.Factory> stages,
            GpuDeviceMemoryUsageValidation memoryValidation)
    {
        GpuOperation operation = source;
        for (int i = 0; i < stages.size() - 1; i++) {
            operation = stages.get(i).create(context, operation);
            if (operation instanceof GpuProject) {
                operation = new ValidationExemptGpuOperation(operation, memoryValidation);
            }
        }
        assertThat(operation).isInstanceOf(GpuWindow.class);
        return operation;
    }

    private static class ValidationExemptGpuOperation
            implements GpuOperation
    {
        private final GpuOperation delegate;
        private final GpuDeviceMemoryUsageValidation validation;

        ValidationExemptGpuOperation(GpuOperation delegate, GpuDeviceMemoryUsageValidation validation)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.validation = requireNonNull(validation, "validation is null");
        }

        @Override
        public Result execute()
        {
            Result[] result = new Result[1];
            validation.withoutValidation(() -> result[0] = delegate.execute());
            return result[0];
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
