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

import io.airlift.units.DataSize;
import io.trino.SessionTestUtils;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
class TestGpuOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        driverContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setGpuMemory(DataSize.of(10, DataSize.Unit.MEGABYTE), DataSize.of(10, DataSize.Unit.MEGABYTE))
                .setOffHeapMemory(DataSize.of(10, DataSize.Unit.MEGABYTE), DataSize.of(10, DataSize.Unit.MEGABYTE))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        executor.close();
        scheduledExecutor.shutdownNow();
        scheduledExecutor.close();
    }

    @Test
    void testCloseAll()
            throws Exception
    {
        List<WorkMock> operations = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            operations.add(new WorkMock());
        }

        try (Operator _ = new GpuOperator.Factory(
                1,
                new PlanNodeId("1"),
                List.of(BIGINT),
                operations.stream()
                        .map(WorkMock::singletonFactory)
                        .toList(),
                List.of(BIGINT),
                new GpuExecutionSemaphore(new GpuConfig()))
                .createOperator(driverContext)) {
            for (WorkMock operation : operations) {
                assertThat(operation.closed).isFalse();
            }
        }

        for (WorkMock operation : operations) {
            assertThat(operation.closed).isTrue();
        }
    }

    @Test
    void testDoWork()
            throws Exception
    {
        List<WorkMock> operations = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            operations.add(new WorkMock());
        }

        try (Operator operator = new GpuOperator.Factory(
                1,
                new PlanNodeId("1"),
                List.of(BIGINT),
                operations.stream()
                        .map(WorkMock::singletonFactory)
                        .toList(),
                List.of(BIGINT),
                new GpuExecutionSemaphore(new GpuConfig()))
                .createOperator(driverContext)) {
            verify(operator.needsInput());
            Page input = new Page(createBlock(BIGINT, 10, NO_NULLS));
            operator.addInput(input);
            operator.finish();

            Page output = operator.getOutput();
            int callsToGetOutput = 1;
            while (output == null) {
                assertThat(operator.isFinished()).as("isFinished before returning a page").isFalse();
                output = operator.getOutput();
                callsToGetOutput++;
            }
            // The input data is so small it shouldn't get split into multiple pages
            assertSameDataInOrder(List.of(output), List.of(input), List.of(BIGINT));
            assertThat(callsToGetOutput).isEqualTo(1);

            int callsToFinish = 0;
            while (!operator.isFinished()) {
                assertThat(operator.getOutput()).as("output after returning a page").isNull();
                callsToFinish++;
            }
            assertThat(callsToFinish).isEqualTo(1);
        }
    }

    private static class WorkMock
            implements GpuOperation
    {
        private GpuOperation source;
        private boolean closed;

        @Override
        public Result execute()
        {
            return source.execute();
        }

        @Override
        public void close()
        {
            closed = true;
            source.close();
        }

        private GpuOperation.Factory singletonFactory()
        {
            return new GpuOperation.Factory()
            {
                @Override
                public GpuOperation create(Context context, GpuOperation source)
                {
                    checkState(WorkMock.this.source == null, "Can create only one operator");
                    WorkMock.this.source = requireNonNull(source, "source is null");
                    return WorkMock.this;
                }

                @Override
                public GpuOperation.Factory duplicate()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void noMoreOperators() {}
            };
        }
    }
}
