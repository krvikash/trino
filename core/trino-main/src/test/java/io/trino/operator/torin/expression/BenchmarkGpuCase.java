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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.torin.CudaStreams;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.torin.Column.Blocks;
import io.trino.spi.torin.GpuTypeConversion;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.torin.BenchmarkGpuUtils.setGpuMemoryPoolForBenchmarks;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/// Micro benchmark for GPU evaluation of a searched CASE with two WHEN clauses,
/// `CASE WHEN a < 50 THEN 1 WHEN a >= 50 THEN 2 ELSE 3 END`, where `a` is uniform in `[0, 100)`:
/// the first clause matches half of the rows and the second one every row the first leaves.
///
/// Reports time/operation and GPU peak memory usage.
@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkGpuCase
{
    /// Run multiple iterations before final {@link Cuda#DEFAULT_STREAM} sync.
    private static final int REPETITIONS = 20;

    private static final Symbol A = new Symbol(BIGINT, "a");

    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long twoWhenClauses(BenchmarkData data)
    {
        long checksum = 0;
        for (int i = 0; i < REPETITIONS; i++) {
            try (ColumnVector result = data.twoWhenClauses.evaluate(data.positionCount, data.inputColumns, new Bindings())) {
                checksum += result.getRowCount();
            }
        }
        // A single stream sync per invocation instead of per evaluation, so the sync barely counts.
        // Peak device memory is reported separately by RmmPeakMemoryProfiler.
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        return checksum;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"65536", "1048576", "100000000"})
        private int positionCount = 1024;

        private final UncheckedCloser closer = UncheckedCloser.create();

        private List<@Borrow ColumnVector> inputColumns;
        private GpuExpression twoWhenClauses;

        @Setup(Level.Trial)
        public void setup()
        {
            setGpuMemoryPoolForBenchmarks();
            createData();
        }

        private void createData()
        {
            Random random = new Random(42);
            long[] values = new long[positionCount];
            for (int position = 0; position < positionCount; position++) {
                values[position] = random.nextLong(0, 100);
            }
            ColumnVector column = GpuTypeConversion.toGpuMapping(BIGINT).orElseThrow()
                    .toColumn().copyToDevice(new Blocks(ImmutableList.of(new LongArrayBlock(positionCount, Optional.empty(), values))));
            closer.register(column::close);
            inputColumns = ImmutableList.of(column);

            Expression a = A.toSymbolReference();
            Expression expression = new Case(
                    ImmutableList.of(
                            new WhenClause(comparison(ComparisonOperator.LESS_THAN, a, new Constant(BIGINT, 50L)), new Constant(BIGINT, 1L)),
                            new WhenClause(comparison(ComparisonOperator.GREATER_THAN_OR_EQUAL, a, new Constant(BIGINT, 50L)), new Constant(BIGINT, 2L))),
                    new Constant(BIGINT, 3L));
            twoWhenClauses = compileExpression(expression, ImmutableMap.of(A, 0), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new IllegalStateException("Expression is not GPU-compilable: " + expression))
                    .expression();
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            closer.close();
        }
    }

    @Test
    public void ensureBenchmarkValid()
    {
        maybeSetGpuMemoryPoolForTests();
        BenchmarkData data = new BenchmarkData();
        try {
            data.createData();
            new BenchmarkGpuCase().twoWhenClauses(data);
        }
        finally {
            data.tearDown();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkGpuCase.class)
                .withOptions(options -> options.addProfiler(RmmPeakMemoryProfiler.class))
                .run();
    }
}
