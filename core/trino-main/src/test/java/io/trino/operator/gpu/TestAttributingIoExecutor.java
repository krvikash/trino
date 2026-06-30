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

import io.trino.operator.OperatorContext;
import io.trino.operator.TestingOperatorContext;
import io.trino.testing.DirectIoExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAttributingIoExecutor
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    void testTaskCpuAttributedToOperator()
            throws Exception
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        AttributingIoExecutor executor = new AttributingIoExecutor(new DirectIoExecutor(), operatorContext);

        long result = executor.submit(() -> {
            long start = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            long sum = 0;
            for (long i = 0; i < 500_000_000L && THREAD_MX_BEAN.getCurrentThreadCpuTime() - start < 10_000_000L; i++) {
                sum += i;
            }
            return sum;
        }).get();

        assertThat(result).isNotNegative();
        if (THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported()) {
            assertThat(operatorContext.getOperatorStats().getGetOutputCpu().roundTo(NANOSECONDS)).isPositive();
        }
    }

    @Test
    void testResultPropagates()
            throws Exception
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        AttributingIoExecutor executor = new AttributingIoExecutor(new DirectIoExecutor(), operatorContext);

        assertThat(executor.submit(() -> "value").get()).isEqualTo("value");
    }

    @Test
    void testExceptionPropagates()
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        AttributingIoExecutor executor = new AttributingIoExecutor(new DirectIoExecutor(), operatorContext);

        CompletableFuture<Object> future = executor.submit(() -> {
            throw new IllegalStateException("boom");
        });
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }
}
