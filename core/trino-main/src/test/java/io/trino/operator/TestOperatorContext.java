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
package io.trino.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestOperatorContext
{
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
    void testOffThreadCpuCountedTowardOutputAndTotalCpu()
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);

        operatorContext.recordOffThreadCpu(3_000_000);
        operatorContext.recordOffThreadCpu(2_000_000);

        assertThat(operatorContext.getCpuNanos()).isEqualTo(5_000_000);
        assertThat(operatorContext.getOperatorStats().getGetOutputCpu().roundTo(NANOSECONDS)).isEqualTo(5_000_000);
    }

    @Test
    void testNegativeOffThreadCpuRejected()
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);

        assertThatThrownBy(() -> operatorContext.recordOffThreadCpu(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
