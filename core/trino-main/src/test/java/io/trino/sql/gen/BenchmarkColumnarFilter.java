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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.TestingIr.between;
import static java.lang.Math.toIntExact;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 15, time = 1)
@Measurement(iterations = 15, time = 1)
public class BenchmarkColumnarFilter
{
    private static final Random RANDOM = new Random(5376453765L);
    private static final long CONSTANT = 8456;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final String COL_0 = "$col_0";
    private static final String COL_1 = "$col_1";

    private PageProcessor compiledProcessor;
    private final List<Page> inputPages = new ArrayList<>();
    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;
    @Param({"0", "10"})
    public int nullsPercentage;
    @Param
    public FilterProvider filterProvider;
    public String dataType = StandardTypes.INTEGER;

    public enum FilterProvider
    {
        BETWEEN {
            @Override
            Expression getExpression(Type type)
            {
                return between(new Reference(type, COL_0),
                        new Constant(type, CONSTANT - 5),
                        new Constant(type, CONSTANT + 5));
            }
        },
        LESS_THAN {
            @Override
            Expression getExpression(Type type)
            {
                return call(
                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(type, type)),
                        new Constant(type, CONSTANT),
                        new Reference(type, COL_0));
            }
        },
        LESS_THAN_TWO_COLUMNS {
            @Override
            Expression getExpression(Type type)
            {
                return call(
                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(type, type)),
                        new Reference(type, COL_0),
                        new Reference(type, COL_1));
            }
        },
        IS_NULL {
            @Override
            Expression getExpression(Type type)
            {
                return new IsNull(new Reference(type, COL_0));
            }
        },
        IS_NOT_NULL {
            @Override
            Expression getExpression(Type type)
            {
                return call(
                        FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                        new IsNull(new Reference(type, COL_0)));
            }
        }
        /**/;

        abstract Expression getExpression(Type type);
    }

    @Setup
    public void setup()
    {
        Type type = getType(dataType);
        Expression filter = filterProvider.getExpression(type);
        List<Symbol> referencedSymbols = SymbolsExtractor.extractUnique(filter).stream()
                .sorted(Comparator.comparing(Symbol::name))
                .toList();
        int channelCount = referencedSymbols.size();
        ImmutableMap.Builder<Symbol, Integer> layoutBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Reference> projections = ImmutableList.builder();
        for (int channel = 0; channel < channelCount; channel++) {
            Symbol symbol = referencedSymbols.get(channel);
            layoutBuilder.put(symbol, channel);
            projections.add(new Reference(symbol.type(), symbol.name()));
        }
        Map<Symbol, Integer> layout = layoutBuilder.buildOrThrow();

        // Mix dictionary and RLE pages alongside the ValueBlock ones so the JIT profile of every
        // per-row call site (block.isNull, block.getInt, block.getUnderlyingValuePosition, ...)
        // sees all three block shapes that occur in real workloads.
        for (int pageCount = 0; pageCount < 20; pageCount++) {
            inputPages.add(buildPage(channelCount, _ -> createValueBlock(8192, nullsPercentage)));
        }
        for (int pageCount = 0; pageCount < 5; pageCount++) {
            inputPages.add(buildPage(channelCount, _ -> createDictionaryBlock(8192, nullsPercentage)));
            inputPages.add(buildPage(channelCount, _ -> createRleBlock(8192)));
        }
        if (nullsPercentage > 0) {
            inputPages.add(buildPage(channelCount, _ -> createRleNullBlock(8192)));
        }

        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        compiledProcessor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        true,
                        false,
                        true,
                        Optional.of(filter),
                        Optional.empty(),
                        projections.build(),
                        layout,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);
    }

    private static Page buildPage(int channelCount, IntFunction<Block> blockForChannel)
    {
        Block[] blocks = new Block[channelCount];
        for (int channel = 0; channel < channelCount; channel++) {
            blocks[channel] = blockForChannel.apply(channel);
        }
        return new Page(blocks[0].getPositionCount(), blocks);
    }

    @Benchmark
    public long evaluateFilter()
    {
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        long outputRows = 0;
        for (Page inputPage : inputPages) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    null,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    SourcePage.create(inputPage));
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
        }
        return outputRows;
    }

    public static void runAllCombinations()
    {
        for (boolean columnarEvaluationEnabled : ImmutableList.of(false, true)) {
            for (FilterProvider filterProvider : FilterProvider.values()) {
                for (String dataType : ImmutableList.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.SMALLINT)) {
                    for (int nullsPercentage : ImmutableList.of(0, 10)) {
                        BenchmarkColumnarFilter benchmark = new BenchmarkColumnarFilter();
                        benchmark.filterProvider = filterProvider;
                        benchmark.dataType = dataType;
                        benchmark.columnarEvaluationEnabled = columnarEvaluationEnabled;
                        benchmark.nullsPercentage = nullsPercentage;
                        benchmark.setup();
                        benchmark.evaluateFilter();
                    }
                }
            }
        }
    }

    private Block createValueBlock(int positions, int nullsPercentage)
    {
        return switch (dataType) {
            case StandardTypes.BIGINT -> createLongsBlock(positions, nullsPercentage);
            case StandardTypes.INTEGER -> createIntsBlock(positions, nullsPercentage);
            case StandardTypes.SMALLINT -> createShortsBlock(positions, nullsPercentage);
            default -> throw new UnsupportedOperationException();
        };
    }

    private Block createDictionaryBlock(int positions, int nullsPercentage)
    {
        Block valueBlock = createValueBlock(positions, nullsPercentage);
        int[] ids = new int[positions];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        return DictionaryBlock.create(positions, valueBlock, ids);
    }

    private Block createRleBlock(int positions)
    {
        Block valueBlock = createValueBlock(1, 0);
        return RunLengthEncodedBlock.create(valueBlock, positions);
    }

    private Block createRleNullBlock(int positions)
    {
        return RunLengthEncodedBlock.create(getType(dataType), null, positions);
    }

    private static Block createShortsBlock(int positionsCount, int nullsPercentage)
    {
        short[] values = new short[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = (short) RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new ShortArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    private static Block createIntsBlock(int positionsCount, int nullsPercentage)
    {
        int[] values = new int[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new IntArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    private static Block createLongsBlock(int positionsCount, int nullsPercentage)
    {
        long[] values = new long[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new LongArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    private static Type getType(String dataType)
    {
        return switch (dataType) {
            case StandardTypes.BIGINT -> BIGINT;
            case StandardTypes.INTEGER -> INTEGER;
            case StandardTypes.SMALLINT -> SMALLINT;
            default -> throw new UnsupportedOperationException();
        };
    }

    static {
        try {
            // pollute the profile
            runAllCombinations();
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    static void main()
            throws Throwable
    {
        benchmark(BenchmarkColumnarFilter.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
