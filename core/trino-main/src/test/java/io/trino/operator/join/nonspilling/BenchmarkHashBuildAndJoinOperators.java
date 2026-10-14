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
package io.trino.operator.join.nonspilling;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.Session;
import io.trino.operator.DriverContext;
import io.trino.operator.JoinPagesIndex;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.TaskContext;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.nonspilling.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.InterpretedHashGenerator.createChannelsHashGenerator;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@Threads(Threads.MAX)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashBuildAndJoinOperators
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");
    private static final NullSafeHashCompiler HASH_COMPILER = new NullSafeHashCompiler(new TypeOperators());
    private static final JoinPagesIndex.TestingFactory PAGES_INDEX_FACTORY = new JoinPagesIndex.TestingFactory(false);

    @State(Scope.Benchmark)
    public static class BuildContext
    {
        protected static final int ROWS_PER_PAGE = 1024;

        @Param({"varchar", "bigint", "all"})
        protected String hashColumns = "bigint";

        @Param({"bigint", "integer"})
        protected String keyType = "bigint";

        @Param({"plain", "mixed"})
        protected String encoding = "plain";

        @Param({"1", "5"})
        protected int buildRowsRepetition = 1;

        @Param({"10", "100", "10000", "100000", "1000000", "8000000"})
        protected int buildRowsNumber = 8_000_000;

        protected ExecutorService executor;
        protected ScheduledExecutorService scheduledExecutor;
        protected List<Page> buildPages;
        protected List<Type> types;
        protected List<Integer> hashChannels;

        @Setup
        public void setup()
        {
            switch (hashColumns) {
                case "varchar" -> hashChannels = Ints.asList(0);
                case "bigint" -> hashChannels = Ints.asList(1);
                case "all" -> hashChannels = Ints.asList(0, 1, 2);
                default -> throw new UnsupportedOperationException(format("Unknown hashColumns value [%s]", hashColumns));
            }
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            initializeBuildPages();
        }

        public Session getSession()
        {
            return TEST_SESSION;
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, getSession(), DataSize.of(2, GIGABYTE));
        }

        public List<Integer> getHashChannels()
        {
            return hashChannels;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public List<Page> getBuildPages()
        {
            return buildPages;
        }

        protected void initializeBuildPages()
        {
            Type keyColumnType = switch (keyType) {
                case "bigint" -> BIGINT;
                case "integer" -> INTEGER;
                default -> throw new UnsupportedOperationException(format("Unknown keyType value [%s]", keyType));
            };
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(ImmutableList.of(VARCHAR, keyColumnType, keyColumnType));

            int maxValue = buildRowsNumber / buildRowsRepetition + 40;
            int rows = 0;
            while (rows < buildRowsNumber) {
                int newRows = Math.min(buildRowsNumber - rows, ROWS_PER_PAGE);
                buildPagesBuilder.addSequencePage(newRows, (rows + 20) % maxValue, (rows + 30) % maxValue, (rows + 40) % maxValue);
                buildPagesBuilder.pageBreak();
                rows += newRows;
            }

            types = buildPagesBuilder.getTypes();
            List<Page> pages = buildPagesBuilder.build();
            buildPages = IntStream.range(0, pages.size())
                    .mapToObj(pageIndex -> encodeBuildPage(pages.get(pageIndex), pageIndex))
                    .collect(toImmutableList());
        }

        private Page encodeBuildPage(Page page, int pageIndex)
        {
            return switch (encoding) {
                case "plain" -> page;
                case "mixed" -> {
                    if (pageIndex % 2 == 0) {
                        yield page;
                    }
                    yield toDictionaryPage(page);
                }
                default -> throw new UnsupportedOperationException(format("Unknown encoding value [%s]", encoding));
            };
        }
    }

    @State(Scope.Benchmark)
    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 1_400_000;

        @Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        @Param({"bigint", "all"})
        protected String outputColumns = "bigint";

        @Param({"1", "16"})
        protected int partitionCount = 1;

        protected List<Page> probePages;
        protected List<Integer> outputChannels;

        protected OperatorFactory joinOperatorFactory;

        @Override
        @Setup
        public void setup()
        {
            super.setup();

            switch (outputColumns) {
                case "varchar" -> outputChannels = Ints.asList(0);
                case "bigint" -> outputChannels = Ints.asList(1);
                case "all" -> outputChannels = Ints.asList(0, 1, 2);
                default -> throw new UnsupportedOperationException(format("Unknown outputColumns value [%s]", hashColumns));
            }

            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = getLookupSourceFactoryManager(this, outputChannels, partitionCount);
            joinOperatorFactory = join(
                    innerJoin(false, false),
                    HASH_JOIN_OPERATOR_ID,
                    TEST_PLAN_NODE_ID,
                    lookupSourceFactory,
                    false,
                    types,
                    hashChannels,
                    Optional.of(outputChannels),
                    HASH_COMPILER,
                    OptionalInt.empty());
            buildHash(this, lookupSourceFactory, outputChannels, partitionCount);
            initializeProbePages();
        }

        public OperatorFactory getJoinOperatorFactory()
        {
            return joinOperatorFactory;
        }

        public List<Page> getProbePages()
        {
            return probePages;
        }

        protected void initializeProbePages()
        {
            RowPagesBuilder probePagesBuilder = rowPagesBuilder(types);

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            int rowsInPage = 0;
            while (remainingRows > 0) {
                double roll = random.nextDouble();

                int columnA = 20 + (remainingRows % buildRowsNumber);
                int columnB = 30 + (remainingRows % buildRowsNumber);
                int columnC = 40 + (remainingRows % buildRowsNumber);

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        columnA *= -1;
                        columnB *= -1;
                        columnC *= -1;
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    if (rowsInPage >= ROWS_PER_PAGE) {
                        probePagesBuilder.pageBreak();
                        rowsInPage = 0;
                    }
                    probePagesBuilder.row(format("%d", columnA), columnB, columnC);
                    --remainingRows;
                    rowsInPage++;
                }
            }
            List<Page> pages = probePagesBuilder.build();
            probePages = IntStream.range(0, pages.size())
                    .mapToObj(pageIndex -> encodeProbePage(pages.get(pageIndex), pageIndex))
                    .collect(toImmutableList());
        }

        private Page encodeProbePage(Page page, int pageIndex)
        {
            return switch (encoding) {
                case "plain" -> page;
                case "mixed" -> switch (pageIndex % 3) {
                    case 0 -> page;
                    case 1 -> toDictionaryPage(page);
                    default -> toRunLengthEncodedPage(page);
                };
                default -> throw new UnsupportedOperationException(format("Unknown encoding value [%s]", encoding));
            };
        }
    }

    private static Page toDictionaryPage(Page page)
    {
        int positionCount = page.getPositionCount();
        int[] ids = IntStream.range(0, positionCount)
                .map(position -> positionCount - 1 - position)
                .toArray();
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = DictionaryBlock.create(positionCount, page.getBlock(channel), ids);
        }
        return new Page(blocks);
    }

    private static Page toRunLengthEncodedPage(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = RunLengthEncodedBlock.create(page.getBlock(channel).getSingleValueBlock(0), page.getPositionCount());
        }
        return new Page(blocks);
    }

    @Benchmark
    public JoinBridgeManager<PartitionedLookupSourceFactory> benchmarkBuildHash(BuildContext buildContext)
    {
        List<Integer> outputChannels = ImmutableList.of(0, 1, 2);
        JoinBridgeManager<PartitionedLookupSourceFactory> joinBridgeManager = getLookupSourceFactoryManager(buildContext, outputChannels, 1);
        buildHash(buildContext, joinBridgeManager, outputChannels, 1);
        return joinBridgeManager;
    }

    private static JoinBridgeManager<PartitionedLookupSourceFactory> getLookupSourceFactoryManager(BuildContext buildContext, List<Integer> outputChannels, int partitionCount)
    {
        PartitionedLookupSourceFactory factory = new PartitionedLookupSourceFactory(
                buildContext.getTypes(),
                outputChannels.stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                buildContext.getHashChannels().stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                partitionCount,
                false,
                HASH_COMPILER);
        return new JoinBridgeManager<>(
                false,
                factory,
                factory.getOutputTypes());
    }

    private static void buildHash(BuildContext buildContext, JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, List<Integer> outputChannels, int partitionCount)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                HASH_BUILD_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                lookupSourceFactoryManager,
                outputChannels,
                buildContext.getHashChannels(),
                Optional.empty(),
                OptionalInt.empty(),
                ImmutableList.of(),
                PAGES_INDEX_FACTORY,
                incrementalLoadFactorHashArraySizeSupplier(buildContext.getSession()));

        Operator[] operators = IntStream.range(0, partitionCount)
                .mapToObj(_ -> buildContext.createTaskContext()
                        .addPipelineContext(0, true, true, partitionCount > 1)
                        .addDriverContext())
                .map(hashBuilderOperatorFactory::createOperator)
                .toArray(Operator[]::new);

        if (partitionCount == 1) {
            for (Page page : buildContext.getBuildPages()) {
                operators[0].addInput(page);
            }
        }
        else {
            PartitionFunction partitionGenerator = new LocalPartitionGenerator(
                    createChannelsHashGenerator(
                            buildContext.getHashChannels().stream()
                                    .map(channel -> buildContext.getTypes().get(channel))
                                    .collect(toImmutableList()),
                            Ints.toArray(buildContext.getHashChannels()),
                            HASH_COMPILER),
                    partitionCount);

            for (Page page : buildContext.getBuildPages()) {
                Page[] partitionedPages = partitionPages(page, buildContext.getTypes(), partitionCount, partitionGenerator);

                for (int i = 0; i < partitionCount; i++) {
                    operators[i].addInput(partitionedPages[i]);
                }
            }
        }

        PartitionedLookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge();
        ListenableFuture<LookupSource> lookupSource = lookupSourceFactory.createLookupSource();
        for (Operator operator : operators) {
            operator.finish();
        }
        if (!lookupSource.isDone()) {
            throw new AssertionError("Expected lookup source provider to be ready");
        }
        getFutureValue(lookupSource).close();
    }

    private static Page[] partitionPages(Page page, List<Type> types, int partitionCount, PartitionFunction partitionGenerator)
    {
        PageBuilder[] builders = new PageBuilder[partitionCount];

        for (int i = 0; i < partitionCount; i++) {
            builders[i] = new PageBuilder(types);
        }

        for (int i = 0; i < page.getPositionCount(); i++) {
            int partition = partitionGenerator.getPartition(page, i);
            appendRow(builders[partition], types, page, i);
        }

        return Arrays.stream(builders)
                .map(PageBuilder::build)
                .toArray(Page[]::new);
    }

    private static void appendRow(PageBuilder pageBuilder, List<Type> types, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < types.size(); channel++) {
            Block block = page.getBlock(channel);
            pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
        }
    }

    @Benchmark
    public List<Page> benchmarkJoinHash(JoinContext joinContext)
            throws Exception
    {
        DriverContext driverContext = joinContext.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator joinOperator = joinContext.getJoinOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = joinContext.getProbePages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !joinOperator.isFinished() && loops < 1_000_000; loops++) {
            if (joinOperator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    joinOperator.addInput(inputPage);
                }
                else if (!finishing) {
                    joinOperator.finish();
                    finishing = true;
                }
            }

            Page outputPage = joinOperator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        joinOperator.close();

        return outputPages.build();
    }

    @Test
    public void testBenchmarkJoinHash()
            throws Exception
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        benchmarkJoinHash(joinContext);
        // ensure that build side hash table is not freed
        List<Page> pages = benchmarkJoinHash(joinContext);

        // assert that there are any rows
        checkState(!pages.isEmpty());
        checkState(pages.getFirst().getPositionCount() > 0);
    }

    @Test
    public void testBenchmarkBuildHash()
    {
        BuildContext buildContext = new BuildContext();
        buildContext.setup();
        benchmarkBuildHash(buildContext);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkHashBuildAndJoinOperators.class).run();
    }
}
