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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.operator.project.InputChannels;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuFilterMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int MULTIPLE_PAGES = 4;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testFilterBigint()
    {
        testFilter(List.of(BIGINT));
    }

    @Test
    void testFilterVarchar()
    {
        testFilter(List.of(BIGINT, VARCHAR));
    }

    private void testFilter(List<Type> types)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(types, NO_NULLS, MULTIPLE_PAGES, ROWS_PER_PAGE, true);

            GpuExpression filterExpression = new GpuExpression()
            {
                @Override
                public ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputs)
                {
                    try (Scalar zero = Scalar.fromLong(0)) {
                        return inputs.getFirst().binaryOp(BinaryOp.GREATER_EQUAL, zero, DType.BOOL8);
                    }
                }

                @Override
                public boolean equals(Object obj)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int hashCode()
                {
                    throw new UnsupportedOperationException();
                }
            };
            CompiledExpression compiledFilter = new CompiledExpression(filterExpression, new InputChannels(0));
            GpuFilter.Factory factory = new GpuFilter.Factory(Optional.of(compiledFilter), Optional.empty(), OptionalDouble.empty());
            try (var source = new SettableGpuOperation();
                    GpuOperation filter = factory.create(context, source)) {
                boolean finished = false;
                while (!finished) {
                    switch (filter.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), types));
                                        source.setPending(context.taskMemoryContext().allocate(getClass().getSimpleName(), next.retainedMemory()), next);
                                    }
                                    else {
                                        source.noMoreInput();
                                    }
                                });
                            }
                        }
                        case Data(AllocatedMemory memory, GpuPage page) -> {
                            try (var closer = UncheckedCloser.create()) {
                                closer.register(memory);
                                closer.register(page);
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }
        }
    }
}
