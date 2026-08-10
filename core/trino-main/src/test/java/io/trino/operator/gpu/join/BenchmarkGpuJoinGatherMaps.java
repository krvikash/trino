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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.GatherMap;
import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Table;
import io.trino.spi.gpu.borrow.Own;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 2000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkGpuJoinGatherMaps
{
    @Benchmark
    public GatherMap[] innerJoinWithRowCount(JoinData data)
    {
        long rowCount = data.probeKeys.innerJoinRowCount(data.hashJoin);
        GatherMap[] maps = data.probeKeys.innerJoinGatherMaps(data.hashJoin, rowCount);
        closeMaps(maps);
        return maps;
    }

    @Benchmark
    public GatherMap[] innerJoinWithoutRowCount(JoinData data)
    {
        GatherMap[] maps = data.probeKeys.innerJoinGatherMaps(data.hashJoin);
        closeMaps(maps);
        return maps;
    }

    @Benchmark
    public GatherMap[] leftJoinWithRowCount(JoinData data)
    {
        long rowCount = data.probeKeys.leftJoinRowCount(data.hashJoin);
        GatherMap[] maps = data.probeKeys.leftJoinGatherMaps(data.hashJoin, rowCount);
        closeMaps(maps);
        return maps;
    }

    @Benchmark
    public GatherMap[] leftJoinWithoutRowCount(JoinData data)
    {
        GatherMap[] maps = data.probeKeys.leftJoinGatherMaps(data.hashJoin);
        closeMaps(maps);
        return maps;
    }

    private static void closeMaps(GatherMap[] maps)
    {
        for (GatherMap map : maps) {
            map.close();
        }
    }

    @State(Scope.Benchmark)
    public static class JoinData
    {
        @Param({"1000", "10000", "100000", "1000000"})
        private int probeRows = 1000;

        @Param({"1000", "100000"})
        private int buildRows = 1000;

        @Param({"0.1", "0.5", "1.0"})
        private double matchRatio = 0.5;

        private @Own ColumnVector probeColumn;
        private @Own ColumnVector buildColumn;
        private @Own Table probeKeys;
        private @Own Table buildKeys;
        private @Own HashJoin hashJoin;

        @Setup
        public void setup()
        {
            maybeSetGpuMemoryPoolForTests();

            int keyRange = (int) (buildRows / matchRatio);
            Random random = new Random(42);

            int[] probeValues = new int[probeRows];
            for (int i = 0; i < probeRows; i++) {
                probeValues[i] = random.nextInt(keyRange);
            }

            int[] buildValues = new int[buildRows];
            for (int i = 0; i < buildRows; i++) {
                buildValues[i] = i;
            }

            probeColumn = ColumnVector.fromInts(probeValues);
            buildColumn = ColumnVector.fromInts(buildValues);
            probeKeys = new Table(probeColumn);
            buildKeys = new Table(buildColumn);
            hashJoin = new HashJoin(buildKeys, false);
        }

        @TearDown
        public void tearDown()
        {
            hashJoin.close();
            buildKeys.close();
            probeKeys.close();
            buildColumn.close();
            probeColumn.close();
        }
    }

    @Test
    public void ensureBenchmarkValid()
    {
        JoinData data = new JoinData();
        data.setup();
        try {
            BenchmarkGpuJoinGatherMaps benchmark = new BenchmarkGpuJoinGatherMaps();
            benchmark.innerJoinWithRowCount(data);
            benchmark.innerJoinWithoutRowCount(data);
            benchmark.leftJoinWithRowCount(data);
            benchmark.leftJoinWithoutRowCount(data);
        }
        finally {
            data.tearDown();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkGpuJoinGatherMaps.class)
                .run();
    }
}
