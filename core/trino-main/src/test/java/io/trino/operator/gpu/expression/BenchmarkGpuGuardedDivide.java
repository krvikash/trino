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
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.function.OperatorType;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
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
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.gpu.BenchmarkGpuUtils.setGpuMemoryPoolForBenchmarks;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/// Micro benchmark for GPU evaluation of a divide guarded against division by zero,
/// `b <> 0 AND a / b > 100000`, where `selectivity` is the percentage of rows with a non-zero `b`.
///
/// The divide can fail, so the AND evaluates it lazily, only on the rows the guard does not settle.
///
/// Reports time/operation and GPU peak memory usage.
@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkGpuGuardedDivide
{
    /// Run multiple iterations before final {@link Cuda#DEFAULT_STREAM} sync.
    private static final int REPETITIONS = 20;

    private static final Symbol A = new Symbol(BIGINT, "a");
    private static final Symbol B = new Symbol(BIGINT, "b");

    /// `b <> 0 AND a / b > 100000`, where the divide is evaluated over NULL-masked inputs.
    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long divide(BenchmarkData data)
    {
        return evaluate(data.divide, data.positionCount, data.divideInputs);
    }

    /// `b <> 0 AND coalesce(a, 0) / coalesce(b, 0) > 100000`. Coalesce turns NULL inputs back into
    /// values, so NULL-masking cannot keep the divide away from the zero divisors, and it is evaluated
    /// on a filtered copy of the input instead.
    @Benchmark
    @OperationsPerInvocation(REPETITIONS)
    public long divideByCoalesce(BenchmarkData data)
    {
        return evaluate(data.divideByCoalesce, data.positionCount, data.divideByCoalesceInputs);
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
        @Param({"65536", "1048576", "100000000"})
        private int positionCount = 1024;

        @Param({"0", "5", "50", "95", "100"})
        private int selectivity = 50;

        private final UncheckedCloser closer = UncheckedCloser.create();

        private GpuExpression divide;
        private List<@Borrow ColumnVector> divideInputs;
        private GpuExpression divideByCoalesce;
        private List<@Borrow ColumnVector> divideByCoalesceInputs;

        @Setup(Level.Trial)
        public void setup()
        {
            setGpuMemoryPoolForBenchmarks();
            createData();
        }

        private void createData()
        {
            Random random = new Random(42);
            long[] dividends = new long[positionCount];
            long[] divisors = new long[positionCount];
            for (int position = 0; position < positionCount; position++) {
                dividends[position] = random.nextLong(90_000, 10_500_000);
                divisors[position] = random.nextInt(100) < selectivity ? random.nextLong(1, 51) : 0;
            }
            // Layout channels: a, b
            List<@Borrow ColumnVector> columns = ImmutableList.of(register(bigintColumn(dividends)), register(bigintColumn(divisors)));

            Expression a = A.toSymbolReference();
            Expression b = B.toSymbolReference();
            Expression guard = comparison(ComparisonOperator.NOT_EQUAL, b, bigint(0));
            Expression divideAbove = comparison(ComparisonOperator.GREATER_THAN, divide(a, b), bigint(100_000));
            Expression divideByCoalesceAbove = comparison(
                    ComparisonOperator.GREATER_THAN,
                    divide(new Coalesce(a, bigint(0)), new Coalesce(b, bigint(0))),
                    bigint(100_000));
            // Otherwise the two benchmarks would no longer measure different evaluation strategies
            checkState(compile(divideAbove).expression().fallibility() == FALLIBLE_EXCEPT_ALL_NULLS, "divide should be evaluated over NULL-masked inputs");
            checkState(compile(divideByCoalesceAbove).expression().fallibility() == FALLIBLE, "divide by coalesce should be evaluated over filtered inputs");

            CompiledExpression compiledDivide = compile(and(guard, divideAbove));
            divide = compiledDivide.expression();
            divideInputs = inputColumns(compiledDivide, columns);
            CompiledExpression compiledDivideByCoalesce = compile(and(guard, divideByCoalesceAbove));
            divideByCoalesce = compiledDivideByCoalesce.expression();
            divideByCoalesceInputs = inputColumns(compiledDivideByCoalesce, columns);
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            closer.close();
        }

        private ColumnVector register(ColumnVector column)
        {
            closer.register(column::close);
            return column;
        }

        private static ColumnVector bigintColumn(long[] values)
        {
            LongArrayBlock block = new LongArrayBlock(values.length, Optional.empty(), values);
            return GpuTypeConversion.toGpuMapping(BIGINT).orElseThrow()
                    .toColumn().copyToDevice(new Blocks(ImmutableList.of(block)));
        }

        /// The columns `compiled` reads, in the order it numbers its inputs, as an operator would give them.
        private static List<@Borrow ColumnVector> inputColumns(CompiledExpression compiled, List<@Borrow ColumnVector> layoutColumns)
        {
            return compiled.inputChannels().getInputChannels().stream()
                    .map(layoutColumns::get)
                    .collect(toImmutableList());
        }

        private static CompiledExpression compile(Expression expression)
        {
            return compileExpression(expression, layout(), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new IllegalStateException("Expression is not GPU-compilable: " + expression));
        }

        private static Map<Symbol, Integer> layout()
        {
            return ImmutableMap.of(A, 0, B, 1);
        }

        private static Expression and(Expression left, Expression right)
        {
            return new Logical(Logical.Operator.AND, ImmutableList.of(left, right));
        }

        private static Expression divide(Expression dividend, Expression divisor)
        {
            return new Call(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT)), ImmutableList.of(dividend, divisor));
        }

        private static Expression bigint(long value)
        {
            return new Constant(BIGINT, value);
        }
    }

    @Test
    public void ensureBenchmarkValid()
    {
        maybeSetGpuMemoryPoolForTests();
        BenchmarkData data = new BenchmarkData();
        try {
            data.createData();
            BenchmarkGpuGuardedDivide benchmark = new BenchmarkGpuGuardedDivide();
            benchmark.divide(data);
            benchmark.divideByCoalesce(data);
        }
        finally {
            data.tearDown();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkGpuGuardedDivide.class)
                .withOptions(options -> options.addProfiler(RmmPeakMemoryProfiler.class))
                .run();
    }
}
