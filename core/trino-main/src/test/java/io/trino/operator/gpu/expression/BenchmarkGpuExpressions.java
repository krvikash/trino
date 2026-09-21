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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.gpu.CudaStreams;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.block.Block;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
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
import java.util.Map;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/// Micro benchmark for GPU expression evaluation.
///
/// Reports time/operation and GPU peak memory usage.
@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 2000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkGpuExpressions
{
    /// Run multiple iterations before final {@link Cuda#DEFAULT_STREAM} sync.
    private static final int REPETITIONS = 100;

    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long betweenBigintConstantBounds(BenchmarkData data)
    {
        return evaluate(data.betweenBigintConstantBounds, data.positionCount, ImmutableList.of(data.bigints));
    }

    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long betweenCastToIntegerConstantBounds(BenchmarkData data)
    {
        return evaluate(data.betweenCastToIntegerConstantBounds, data.positionCount, ImmutableList.of(data.bigintsWithinIntRange));
    }

    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long greaterThanOrEqualBigintConstant(BenchmarkData data)
    {
        return evaluate(data.greaterThanOrEqualBigintConstant, data.positionCount, ImmutableList.of(data.bigints));
    }

    private static long evaluate(GpuExpression expression, int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        long checksum = 0;
        for (int i = 0; i < REPETITIONS; i++) {
            try (ColumnVector result = expression.evaluate(positionCount, inputColumns, new Bindings())) {
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
        @Param({"1024", "65536", "1048576"})
        private int positionCount = 1024;

        private final UncheckedCloser closer = UncheckedCloser.create();

        private @Borrow ColumnVector bigints;
        private @Borrow ColumnVector bigintsWithinIntRange;

        private GpuExpression betweenBigintConstantBounds;
        private GpuExpression betweenCastToIntegerConstantBounds;
        private GpuExpression greaterThanOrEqualBigintConstant;

        @Setup(Level.Trial)
        public void setup()
        {
            maybeSetGpuMemoryPoolForTests();

            bigints = bigintColumn(positionCount);
            closer.register(bigints::close);
            bigintsWithinIntRange = bigintColumnWithinIntRange(positionCount);
            closer.register(bigintsWithinIntRange::close);

            Reference value = new Reference(BIGINT, "ref0");
            betweenBigintConstantBounds = compile(
                    between(value, new Constant(BIGINT, 10L), new Constant(BIGINT, 50L)));
            // A narrowing (overflow-checked) cast cannot lower to a cudf AST node, unlike the constant bounds.
            betweenCastToIntegerConstantBounds = compile(
                    between(new Cast(value, INTEGER), new Constant(INTEGER, 10L), new Constant(INTEGER, 50L)));
            greaterThanOrEqualBigintConstant = compile(
                    comparison(ComparisonOperator.GREATER_THAN_OR_EQUAL, value, new Constant(BIGINT, 10L)));
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            closer.close();
        }

        private static GpuExpression compile(Expression expression)
        {
            return compileExpression(expression, layout(BIGINT), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new IllegalStateException("Expression is not GPU-compilable: " + expression))
                    .expression();
        }

        private static ColumnVector bigintColumn(int positionCount)
        {
            Block block = createBlock(BIGINT, positionCount, NullsProvider.NO_NULLS);
            return GpuTypeConversion.toGpuMapping(BIGINT).orElseThrow()
                    .toColumn().copyToDevice(new Blocks(ImmutableList.of(block)));
        }

        private static ColumnVector bigintColumnWithinIntRange(int positionCount)
        {
            Block block = createBigintBlock(positionCount, NullsProvider.NO_NULLS, Integer.MIN_VALUE, Integer.MAX_VALUE);
            return GpuTypeConversion.toGpuMapping(BIGINT).orElseThrow()
                    .toColumn().copyToDevice(new Blocks(ImmutableList.of(block)));
        }

        private static Map<Symbol, Integer> layout(Type type)
        {
            return ImmutableMap.of(new Symbol(type, "ref0"), 0);
        }
    }

    @Test
    public void ensureBenchmarkValid()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        try {
            BenchmarkGpuExpressions benchmark = new BenchmarkGpuExpressions();
            benchmark.betweenBigintConstantBounds(data);
            benchmark.betweenCastToIntegerConstantBounds(data);
            benchmark.greaterThanOrEqualBigintConstant(data);
        }
        finally {
            data.tearDown();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkGpuExpressions.class)
                .withOptions(options -> options.addProfiler(RmmPeakMemoryProfiler.class))
                .run();
    }
}
