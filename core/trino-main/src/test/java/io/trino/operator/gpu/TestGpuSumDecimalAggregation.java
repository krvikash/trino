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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.aggregation.AggregationTestUtils;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.gpu.GpuProject.Projection;
import io.trino.operator.gpu.aggregation.GpuAggregateFunction;
import io.trino.operator.gpu.aggregation.GpuAggregation;
import io.trino.operator.gpu.aggregation.GpuSum;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GetColumn;
import io.trino.operator.gpu.expression.GpuCombineDecimalStateSumsToDecimal128;
import io.trino.operator.gpu.expression.GpuCombineSumChunksToVarbinary;
import io.trino.operator.gpu.expression.GpuDecimal128AsVarbinary;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.expression.GpuExtractDecimalStateChunk;
import io.trino.operator.gpu.expression.GpuExtractInt32Chunk;
import io.trino.operator.project.InputChannels;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.block.BlockAssertions.getOnlyValue;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end PARTIAL → FINAL roundtrip tests for {@code sum(decimal)} across all four
 * {CPU, GPU} × {CPU, GPU} engine combinations. The CPU PARTIAL leg produces variable-length
 * (8 / 16 / 24 byte) intermediate states; the GPU PARTIAL leg produces uniform 16-byte. The
 * GPU FINAL implementation must accept both, so all four combinations should agree with a
 * pure-CPU reference.
 */
final class TestGpuSumDecimalAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    private enum Engine
    {
        CPU,
        GPU,
    }

    static Stream<Object[]> engineCombinationsAndDecimalTypes()
    {
        DecimalType shortDecimal = createDecimalType(12, 2);
        DecimalType longDecimal = createDecimalType(26, 4);
        ImmutableList.Builder<Object[]> cases = ImmutableList.builder();
        for (Engine partialEngine : Engine.values()) {
            for (Engine finalEngine : Engine.values()) {
                for (DecimalType type : List.of(shortDecimal, longDecimal)) {
                    cases.add(new Object[] {partialEngine, finalEngine, type});
                }
            }
        }
        return cases.build().stream();
    }

    @ParameterizedTest
    @MethodSource("engineCombinationsAndDecimalTypes")
    void testSumDecimalGlobalRoundtrip(Engine partialEngine, Engine finalEngine, DecimalType type)
    {
        Block valueBlock = createBlock(type, 100, NullsProvider.RANDOM_NULLS);
        Page input = new Page(valueBlock);

        Object expected = computeCpuReference(input, type, Optional.empty());

        Block intermediate = runPartialGlobal(input, type, partialEngine);
        Object actual = runFinalGlobal(intermediate, type, finalEngine);

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("engineCombinationsAndDecimalTypes")
    void testSumDecimalGroupByRoundtrip(Engine partialEngine, Engine finalEngine, DecimalType type)
    {
        Block groupKeys = createGroupByBlock(100, 5);
        Block valueBlock = createBlock(type, 100, NullsProvider.RANDOM_NULLS);
        Page input = new Page(groupKeys, valueBlock);

        Map<Long, Object> expected = computeCpuGroupByReference(input, type);

        Page intermediatePage = runPartialGroupBy(input, type, partialEngine);
        Map<Long, Object> actual = runFinalGroupBy(intermediatePage, type, finalEngine);

        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (Long key : expected.keySet()) {
            assertThat(actual.get(key))
                    .as("group %s", key)
                    .isEqualTo(expected.get(key));
        }
    }

    private static Block runCpuPartialGlobal(Page input, DecimalType type)
    {
        TestingAggregationFunction function = sumFunction(type);
        AggregatorFactory partialFactory = function.createAggregatorFactory(PARTIAL, Ints.asList(0), OptionalInt.empty());
        Aggregator aggregator = partialFactory.createAggregator(new AggregationMetrics());
        if (input.getPositionCount() > 0) {
            aggregator.processPage(input);
        }
        return AggregationTestUtils.getIntermediateBlock(function.getIntermediateType(), aggregator);
    }

    private static Object runCpuFinalGlobal(Block intermediate, DecimalType type)
    {
        TestingAggregationFunction function = sumFunction(type);
        AggregatorFactory finalFactory = function.createAggregatorFactory(FINAL, Ints.asList(0), OptionalInt.empty());
        Aggregator aggregator = finalFactory.createAggregator(new AggregationMetrics());
        aggregator.processPage(new Page(intermediate));
        Block finalBlock = AggregationTestUtils.getFinalBlock(function.getFinalType(), aggregator);
        return getOnlyValue(function.getFinalType(), finalBlock);
    }

    private static Page runCpuPartialGroupBy(Page input, DecimalType type)
    {
        TestingAggregationFunction function = sumFunction(type);
        // Aggregator reads its single value column at channel 0 of the page handed to processPage.
        AggregatorFactory partialFactory = function.createAggregatorFactory(PARTIAL, Ints.asList(0), OptionalInt.empty());
        GroupedAggregator aggregator = partialFactory.createGroupedAggregator(new AggregationMetrics());
        Block groupBlock = input.getBlock(0);
        // Map group keys to dense group ids so the grouped aggregator's int[] groupIds parameter
        // matches the [0, groupCount) contract.
        Map<Long, Integer> keyToId = new HashMap<>();
        int[] groupIds = new int[input.getPositionCount()];
        for (int i = 0; i < input.getPositionCount(); i++) {
            long key = BIGINT.getLong(groupBlock, i);
            groupIds[i] = keyToId.computeIfAbsent(key, _ -> keyToId.size());
        }
        aggregator.processPage(keyToId.size(), groupIds, new Page(input.getBlock(1)));

        // Emit one varbinary intermediate per group, ordered by ascending group id.
        long[] keysByGroupId = keysByGroupId(keyToId);
        BlockBuilder keyOut = BIGINT.createBlockBuilder(null, keyToId.size());
        BlockBuilder intermediateOut = function.getIntermediateType().createBlockBuilder(null, keyToId.size());
        for (int id = 0; id < keyToId.size(); id++) {
            BIGINT.writeLong(keyOut, keysByGroupId[id]);
            aggregator.evaluate(id, intermediateOut);
        }
        return new Page(keyOut.build(), intermediateOut.build());
    }

    private static Map<Long, Object> runCpuFinalGroupBy(Page intermediate, DecimalType type)
    {
        TestingAggregationFunction function = sumFunction(type);
        // Aggregator reads its single varbinary column at channel 0 of the page handed to processPage.
        AggregatorFactory finalFactory = function.createAggregatorFactory(FINAL, Ints.asList(0), OptionalInt.empty());
        GroupedAggregator aggregator = finalFactory.createGroupedAggregator(new AggregationMetrics());
        Block keyBlock = intermediate.getBlock(0);
        Map<Long, Integer> keyToId = new HashMap<>();
        int[] groupIds = new int[intermediate.getPositionCount()];
        for (int i = 0; i < intermediate.getPositionCount(); i++) {
            long key = BIGINT.getLong(keyBlock, i);
            groupIds[i] = keyToId.computeIfAbsent(key, _ -> keyToId.size());
        }
        aggregator.processPage(keyToId.size(), groupIds, new Page(intermediate.getBlock(1)));

        long[] keysByGroupId = keysByGroupId(keyToId);
        BlockBuilder out = function.getFinalType().createBlockBuilder(null, keyToId.size());
        for (int id = 0; id < keyToId.size(); id++) {
            aggregator.evaluate(id, out);
        }
        Block finalBlock = out.build();

        Map<Long, Object> result = new HashMap<>();
        for (int id = 0; id < keyToId.size(); id++) {
            result.put(keysByGroupId[id], function.getFinalType().getObjectValue(finalBlock, id));
        }
        return result;
    }

    private static long[] keysByGroupId(Map<Long, Integer> keyToId)
    {
        long[] keysByGroupId = new long[keyToId.size()];
        for (Map.Entry<Long, Integer> e : keyToId.entrySet()) {
            keysByGroupId[e.getValue()] = e.getKey();
        }
        return keysByGroupId;
    }

    private static Block runGpuPartialGlobal(Page input, DecimalType type)
    {
        DType decimal128Type = decimal128Dtype(type);
        List<Page> output = executeGpuOperation(
                List.of(input),
                List.of(type),
                List.of(VARBINARY),
                (context, copyToDevice) -> chainGpuPartialPipeline(context, copyToDevice, type, decimal128Type, /*sourceChannel=*/ 0, /*groupByChannels=*/ new int[0], List.of()));
        return concatBlocks(output, VARBINARY);
    }

    private static Object runGpuFinalGlobal(Block intermediate, DecimalType type)
    {
        DType decimal128Type = decimal128Dtype(type);
        DecimalType outputType = createDecimalType(38, type.getScale());
        List<Page> output = executeGpuOperation(
                List.of(new Page(intermediate)),
                List.of(VARBINARY),
                List.of(outputType),
                (context, copyToDevice) -> chainGpuFinalPipeline(context, copyToDevice, decimal128Type, /*sourceChannel=*/ 0, /*groupByChannels=*/ new int[0], List.of()));
        checkState(output.size() == 1 && output.getFirst().getPositionCount() == 1, "Expected single result row");
        return getOnlyValue(outputType, output.getFirst().getBlock(0));
    }

    private static Page runGpuPartialGroupBy(Page input, DecimalType type)
    {
        DType decimal128Type = decimal128Dtype(type);
        List<Page> output = executeGpuOperation(
                List.of(input),
                List.of(BIGINT, type),
                List.of(BIGINT, VARBINARY),
                (context, copyToDevice) -> chainGpuPartialPipeline(context, copyToDevice, type, decimal128Type, /*sourceChannel=*/ 1, /*groupByChannels=*/ new int[] {0}, List.of(BIGINT)));
        return concatPages(output, List.of(BIGINT, VARBINARY));
    }

    private static Map<Long, Object> runGpuFinalGroupBy(Page intermediate, DecimalType type)
    {
        DType decimal128Type = decimal128Dtype(type);
        DecimalType outputType = createDecimalType(38, type.getScale());
        List<Page> output = executeGpuOperation(
                List.of(intermediate),
                List.of(BIGINT, VARBINARY),
                List.of(BIGINT, outputType),
                (context, copyToDevice) -> chainGpuFinalPipeline(context, copyToDevice, decimal128Type, /*sourceChannel=*/ 1, /*groupByChannels=*/ new int[] {0}, List.of(BIGINT)));
        Map<Long, Object> result = new HashMap<>();
        for (Page page : output) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                long key = BIGINT.getLong(page.getBlock(0), i);
                Object previous = result.put(key, outputType.getObjectValue(page.getBlock(1), i));
                checkState(previous == null, "Duplicate group key %s in GPU result", key);
            }
        }
        return result;
    }

    private static GpuOperation chainGpuPartialPipeline(
            GpuOperation.Context context,
            GpuOperation source,
            DecimalType inputDecimalType,
            DType decimal128Type,
            int sourceChannel,
            int[] groupByChannels,
            List<Type> groupByTypes)
    {
        // PARTIAL pipeline: pre-projection (chunk extraction or upcast) → SUM → post-projection
        // (combine to varbinary). Mirrors GpuAggregationCompiler.shortDecimalSumPartial /
        // longDecimalSumPartial.
        int sourceColumnCount = groupByChannels.length + 1;
        int derivedStart = sourceColumnCount;

        ImmutableList.Builder<Projection> preProjections = ImmutableList.builder();
        for (int i = 0; i < sourceColumnCount; i++) {
            preProjections.add(new Projection.PassThrough(i));
        }
        if (inputDecimalType.isShort()) {
            // Cast decimal(p,s) → DECIMAL128(s) so SUM accumulates with 128-bit width.
            preProjections.add(new Projection.Gpu(new CompiledExpression(
                    new GpuShortDecimalToDecimal128(decimal128Type),
                    new InputChannels(List.of(sourceChannel)))));
        }
        else {
            preProjections.add(new Projection.Gpu(chunkExpression(sourceChannel, 0, DType.UINT32)));
            preProjections.add(new Projection.Gpu(chunkExpression(sourceChannel, 1, DType.UINT32)));
            preProjections.add(new Projection.Gpu(chunkExpression(sourceChannel, 2, DType.UINT32)));
            preProjections.add(new Projection.Gpu(chunkExpression(sourceChannel, 3, DType.INT32)));
        }

        // GpuAggregation expects derived-channel inputs for the aggregates.
        List<GpuAggregateFunction> aggregates;
        if (inputDecimalType.isShort()) {
            aggregates = List.of(new GpuSum(derivedStart, createDecimalType(38, inputDecimalType.getScale()), decimal128Type));
        }
        else {
            aggregates = List.of(
                    new GpuSum(derivedStart, BIGINT, DType.INT64),
                    new GpuSum(derivedStart + 1, BIGINT, DType.INT64),
                    new GpuSum(derivedStart + 2, BIGINT, DType.INT64),
                    new GpuSum(derivedStart + 3, BIGINT, DType.INT64));
        }

        // Aggregation result layout: groupKeys..., aggSlots...
        int aggResultStart = groupByChannels.length;

        ImmutableList.Builder<Projection> postProjections = ImmutableList.builder();
        for (int i = 0; i < groupByChannels.length; i++) {
            postProjections.add(new Projection.PassThrough(i));
        }
        if (inputDecimalType.isShort()) {
            postProjections.add(new Projection.Gpu(new CompiledExpression(
                    new GpuDecimal128AsVarbinary(),
                    new InputChannels(List.of(aggResultStart)))));
        }
        else {
            postProjections.add(new Projection.Gpu(new CompiledExpression(
                    new GpuCombineSumChunksToVarbinary(decimal128Type),
                    new InputChannels(List.of(aggResultStart, aggResultStart + 1, aggResultStart + 2, aggResultStart + 3)))));
        }

        List<Projection> preProjectionList = preProjections.build();
        GpuOperation op = new GpuProject.Factory(preProjectionList).create(context, source);
        op = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypes, /*inputRaw=*/ true, /*compactionThresholdBytes=*/ 1, preProjectionList.size()).create(context, op);
        op = new GpuProject.Factory(postProjections.build()).create(context, op);
        return op;
    }

    private static GpuOperation chainGpuFinalPipeline(
            GpuOperation.Context context,
            GpuOperation source,
            DType decimal128Type,
            int sourceChannel,
            int[] groupByChannels,
            List<Type> groupByTypes)
    {
        // FINAL pipeline: pre-projection (5 component extractions) → 5 INT64 SUMs → post-
        // projection (recombine to DECIMAL128). Mirrors GpuAggregationCompiler.decimalSumFinal.
        int sourceColumnCount = groupByChannels.length + 1;
        int derivedStart = sourceColumnCount;

        ImmutableList.Builder<Projection> preProjections = ImmutableList.builder();
        for (int i = 0; i < sourceColumnCount; i++) {
            preProjections.add(new Projection.PassThrough(i));
        }
        for (int component = 0; component < GpuExtractDecimalStateChunk.COMPONENT_COUNT; component++) {
            preProjections.add(new Projection.Gpu(new CompiledExpression(
                    new GpuExtractDecimalStateChunk(new GetColumn(0), component),
                    new InputChannels(List.of(sourceChannel)))));
        }

        List<GpuAggregateFunction> aggregates = List.of(
                new GpuSum(derivedStart, BIGINT, DType.INT64),
                new GpuSum(derivedStart + 1, BIGINT, DType.INT64),
                new GpuSum(derivedStart + 2, BIGINT, DType.INT64),
                new GpuSum(derivedStart + 3, BIGINT, DType.INT64),
                new GpuSum(derivedStart + 4, BIGINT, DType.INT64));

        int aggResultStart = groupByChannels.length;

        ImmutableList.Builder<Projection> postProjections = ImmutableList.builder();
        for (int i = 0; i < groupByChannels.length; i++) {
            postProjections.add(new Projection.PassThrough(i));
        }
        postProjections.add(new Projection.Gpu(new CompiledExpression(
                new GpuCombineDecimalStateSumsToDecimal128(decimal128Type),
                new InputChannels(List.of(
                        aggResultStart,
                        aggResultStart + 1,
                        aggResultStart + 2,
                        aggResultStart + 3,
                        aggResultStart + 4)))));

        List<Projection> preProjectionList = preProjections.build();
        GpuOperation op = new GpuProject.Factory(preProjectionList).create(context, source);
        op = new GpuAggregation.Factory(aggregates, groupByChannels, groupByTypes, /*inputRaw=*/ false, /*compactionThresholdBytes=*/ 1, preProjectionList.size()).create(context, op);
        op = new GpuProject.Factory(postProjections.build()).create(context, op);
        return op;
    }

    private static CompiledExpression chunkExpression(int sourceChannel, int chunkIdx, DType chunkType)
    {
        return new CompiledExpression(
                new GpuExtractInt32Chunk(new GetColumn(0), chunkIdx, chunkType),
                new InputChannels(List.of(sourceChannel)));
    }

    /**
     * Inline shim around cuDF's {@code castTo(DECIMAL128)} for short-decimal columns. Mirrors the
     * compiled cast that {@link io.trino.operator.gpu.aggregation.GpuAggregationCompiler}'s
     * {@code shortDecimalSumPartial} emits as the first pre-projection.
     */
    private static final class GpuShortDecimalToDecimal128
            extends GpuExpression
    {
        private final DType decimal128Type;

        private GpuShortDecimalToDecimal128(DType decimal128Type)
        {
            this.decimal128Type = decimal128Type;
        }

        @Override
        public ColumnVector evaluate(int positionCount, List<ColumnVector> inputColumns)
        {
            checkState(inputColumns.size() == 1, "Expected exactly one input column, got %s", inputColumns.size());
            return inputColumns.getFirst().castTo(decimal128Type);
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof GpuShortDecimalToDecimal128 other && decimal128Type.equals(other.decimal128Type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getClass(), decimal128Type);
        }
    }

    private static Block runPartialGlobal(Page input, DecimalType type, Engine engine)
    {
        return switch (engine) {
            case CPU -> runCpuPartialGlobal(input, type);
            case GPU -> runGpuPartialGlobal(input, type);
        };
    }

    private static Object runFinalGlobal(Block intermediate, DecimalType type, Engine engine)
    {
        return switch (engine) {
            case CPU -> runCpuFinalGlobal(intermediate, type);
            case GPU -> runGpuFinalGlobal(intermediate, type);
        };
    }

    private static Page runPartialGroupBy(Page input, DecimalType type, Engine engine)
    {
        return switch (engine) {
            case CPU -> runCpuPartialGroupBy(input, type);
            case GPU -> runGpuPartialGroupBy(input, type);
        };
    }

    private static Map<Long, Object> runFinalGroupBy(Page intermediate, DecimalType type, Engine engine)
    {
        return switch (engine) {
            case CPU -> runCpuFinalGroupBy(intermediate, type);
            case GPU -> runGpuFinalGroupBy(intermediate, type);
        };
    }

    private static Object computeCpuReference(Page input, DecimalType type, Optional<Block> ignored)
    {
        TestingAggregationFunction function = sumFunction(type);
        return AggregationTestUtils.aggregation(function, input);
    }

    private static Map<Long, Object> computeCpuGroupByReference(Page input, DecimalType type)
    {
        Block groupBlock = input.getBlock(0);
        Block valueBlock = input.getBlock(1);

        Map<Long, BlockBuilder> groupBuilders = new HashMap<>();
        for (int i = 0; i < input.getPositionCount(); i++) {
            long groupKey = BIGINT.getLong(groupBlock, i);
            BlockBuilder builder = groupBuilders.computeIfAbsent(groupKey, _ -> type.createBlockBuilder(null, 16));
            if (valueBlock.isNull(i)) {
                builder.appendNull();
            }
            else {
                builder.append(valueBlock.getUnderlyingValueBlock(), valueBlock.getUnderlyingValuePosition(i));
            }
        }
        Map<Long, Object> result = new HashMap<>();
        TestingAggregationFunction function = sumFunction(type);
        for (Map.Entry<Long, BlockBuilder> entry : groupBuilders.entrySet()) {
            result.put(entry.getKey(), AggregationTestUtils.aggregation(function, new Page(entry.getValue().build())));
        }
        return result;
    }

    private static TestingAggregationFunction sumFunction(DecimalType type)
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(type));
    }

    private static DType decimal128Dtype(DecimalType type)
    {
        return DType.create(DType.DTypeEnum.DECIMAL128, -type.getScale());
    }

    private static Block createGroupByBlock(int positionsCount, int numGroups)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(builder, i % numGroups);
        }
        return builder.build();
    }

    private static Block concatBlocks(List<Page> pages, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, pages.stream().mapToInt(Page::getPositionCount).sum());
        for (Page page : pages) {
            Block block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    builder.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(i));
                }
            }
        }
        return builder.build();
    }

    private static Page concatPages(List<Page> pages, List<Type> types)
    {
        int total = pages.stream().mapToInt(Page::getPositionCount).sum();
        Block[] blocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            BlockBuilder builder = types.get(channel).createBlockBuilder(null, total);
            for (Page page : pages) {
                Block block = page.getBlock(channel);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        builder.appendNull();
                    }
                    else {
                        builder.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(i));
                    }
                }
            }
            blocks[channel] = builder.build();
        }
        return new Page(total, blocks);
    }
}
