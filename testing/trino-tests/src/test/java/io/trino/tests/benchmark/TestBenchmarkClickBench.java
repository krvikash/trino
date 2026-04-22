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
package io.trino.tests.benchmark;

import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.IntStream;

import static io.trino.tests.benchmark.BenchmarkClickBench.ExecutionMode.CPU;
import static io.trino.tests.benchmark.BenchmarkClickBench.ExecutionMode.GPU;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Sequential execution to avoid memory pressure from concurrent queries, some queries are memory intensive
public class TestBenchmarkClickBench
{
    private DistributedQueryRunner gpuRunner;
    private DistributedQueryRunner cpuRunner;

    @BeforeAll
    public void setup()
            throws Exception
    {
        gpuRunner = BenchmarkClickBench.setup(GPU, false);
        cpuRunner = BenchmarkClickBench.setup(CPU, false);
    }

    @AfterAll
    public void teardown()
    {
        if (gpuRunner != null) {
            gpuRunner.close();
        }
        if (cpuRunner != null) {
            cpuRunner.close();
        }
    }

    @ParameterizedTest(name = "q{0}")
    @MethodSource("queryNumbers")
    public void testGpuMatchesCpu(int queryNumber)
    {
        String query = BenchmarkClickBench.readQuery(queryNumber);
        assertThat(gpuRunner.execute(gpuRunner.getDefaultSession(), query).getMaterializedRows())
                .isEqualTo(cpuRunner.execute(cpuRunner.getDefaultSession(), query).getMaterializedRows());
    }

    static IntStream queryNumbers()
    {
        return IntStream.rangeClosed(1, 43);
    }
}
