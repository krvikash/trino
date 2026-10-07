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
package io.trino.operator.gpu.window;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.GroupByScanAggregation;
import ai.rapids.cudf.ScanAggregation;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuProject;
import io.trino.operator.gpu.GpuProject.Projection;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GetColumn;
import io.trino.operator.gpu.expression.GpuBinaryExpression;
import io.trino.operator.gpu.expression.GpuCast;
import io.trino.operator.gpu.expression.GpuConstant;
import io.trino.operator.gpu.expression.GpuDivideRoundToScale;
import io.trino.operator.gpu.expression.GpuDivideSumChunksRoundToScale;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.expression.GpuExtractInt32Chunk;
import io.trino.operator.gpu.expression.GpuIf;
import io.trino.operator.gpu.expression.GpuIsNull;
import io.trino.operator.gpu.expression.GpuNot;
import io.trino.operator.gpu.expression.GpuStruct;
import io.trino.operator.gpu.window.GpuWindow.WindowFrame;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.WindowNode;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class GpuWindowCompiler
{
    private GpuWindowCompiler() {}

    private static final GpuExpression ONE = new GpuConstant(BIGINT, Optional.of(1L));
    private static final CompiledExpression CAST_SCAN_RESULT_TO_BIGINT = new CompiledExpression(new GpuCast(new GetColumn(0), DType.INT64), new InputChannels(0));
    private static final CompiledExpression PASSTHROUGH = new CompiledExpression(new GetColumn(0), new InputChannels(0));
    // cuDF's rank/dense_rank scans return size_type (int32_t) per aggregation.hpp:
    // "All rank methods except AVERAGE method and percentage!=NONE returns size_type column."
    private static final int RANK_SCAN_OUTPUT_BYTES_PER_ROW = DType.INT32.getSizeInBytes();
    // cuDF's sort_groupby_helper (sort_helper.cu) allocates three shared INT32 columns: _key_sorted_order, _group_offsets, _group_labels (3 * INT32_BYTES).
    // Grouped rank scan additionally allocates a temporary gather_map (INT32) used during scatter (scan.cpp).
    private static final int RANK_GROUPED_SCAN_WORKING_BYTES_PER_ROW = 4 * DType.INT32.getSizeInBytes();
    private static final int ROW_NUMBER_SCAN_OUTPUT_BYTES_PER_ROW = DType.INT64.getSizeInBytes();
    // cuDF's sort_groupby_helper (sort_helper.cu) allocates three shared INT32 columns: _key_sorted_order, _group_offsets, _group_labels.
    private static final int ROW_NUMBER_GROUPED_SCAN_WORKING_BYTES_PER_ROW = 3 * DType.INT32.getSizeInBytes();
    // cuDF's DECIMAL128 sum wraps around on overflow instead of failing. A cuDF column holds at most 2^31 - 1 rows,
    // so a sum of precision-28 values stays below (2^31 - 1) * 10^28 < 2^127, while precision 29 could exceed it
    private static final int MAX_DECIMAL128_SUM_AVG_PRECISION = 28;

    public sealed interface CompileResult
    {
        record Success(List<GpuOperation.Factory> stages, List<Type> outputTypes)
                implements CompileResult
        {
            public Success
            {
                stages = ImmutableList.copyOf(requireNonNull(stages, "stages is null"));
                outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            }
        }

        record Failure(String reason)
                implements CompileResult
        {
            public Failure
            {
                requireNonNull(reason, "reason is null");
            }
        }
    }

    public static CompileResult compile(WindowNode node, Map<Symbol, Integer> sourceLayout, List<Type> sourceTypes)
    {
        for (Type type : sourceTypes) {
            if (!isConvertible(type)) {
                return new CompileResult.Failure("unsupported source column type: " + type);
            }
        }

        List<Symbol> partitionBySymbols = node.getPartitionBy();
        int[] partitionChannels = new int[partitionBySymbols.size()];
        ImmutableList.Builder<Type> partitionTypesBuilder = ImmutableList.builder();
        for (int i = 0; i < partitionBySymbols.size(); i++) {
            Symbol symbol = partitionBySymbols.get(i);
            partitionChannels[i] = verifyNotNull(sourceLayout.get(symbol), "channel for partition symbol %s is not in source layout", symbol);
            partitionTypesBuilder.add(symbol.type());
        }

        int[] orderChannels;
        ImmutableList.Builder<SortOrder> orderSortOrdersBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> orderTypesBuilder = ImmutableList.builder();
        Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();
        if (orderingScheme.isPresent()) {
            List<Symbol> orderBySymbols = orderingScheme.get().orderBy();
            orderChannels = new int[orderBySymbols.size()];
            for (int i = 0; i < orderBySymbols.size(); i++) {
                Symbol symbol = orderBySymbols.get(i);
                orderChannels[i] = verifyNotNull(sourceLayout.get(symbol), "channel for order symbol %s is not in source layout", symbol);
                orderSortOrdersBuilder.add(orderingScheme.get().ordering(symbol));
                orderTypesBuilder.add(symbol.type());
            }
        }
        else {
            orderChannels = new int[0];
        }
        List<SortOrder> orderSortOrders = orderSortOrdersBuilder.build();
        List<Type> orderTypes = orderTypesBuilder.build();
        Optional<GpuExpression> peerKey = peerKey(orderChannels);

        ImmutableList.Builder<CompiledWindowFunction> functionsBuilder = ImmutableList.builder();
        for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
            WindowNode.Function function = entry.getValue();
            ResolvedFunction resolvedFunction = function.getResolvedFunction();

            if (!isBuiltinFunctionName(resolvedFunction.name())) {
                return new CompileResult.Failure("non-builtin function: " + resolvedFunction.name());
            }
            if (function.getOrderingScheme().isPresent()) {
                return new CompileResult.Failure("function with ordering: " + resolvedFunction.name());
            }
            if (function.isDistinct()) {
                return new CompileResult.Failure("distinct function: " + resolvedFunction.name());
            }
            if (function.isIgnoreNulls()) {
                return new CompileResult.Failure("ignore nulls function: " + resolvedFunction.name());
            }

            Optional<CompiledWindowFunction> compiledFunction = switch (resolvedFunction.functionKind()) {
                case WINDOW -> compileWindowFunction(resolvedFunction, function, peerKey, orderTypes);
                case AGGREGATE -> {
                    Optional<WindowFrame> frame = compileFrame(function.getFrame());
                    if (frame.isEmpty()) {
                        yield Optional.empty();
                    }
                    yield compileAggregateFunction(resolvedFunction, function, frame.get(), sourceLayout);
                }
                default -> Optional.empty();
            };
            if (compiledFunction.isEmpty()) {
                return new CompileResult.Failure("unsupported window function: " + resolvedFunction.name());
            }
            functionsBuilder.add(compiledFunction.orElseThrow());
        }
        List<CompiledWindowFunction> functions = functionsBuilder.build();

        ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();
        outputTypes.addAll(sourceTypes);
        for (CompiledWindowFunction function : functions) {
            outputTypes.add(function.outputType());
        }

        return new CompileResult.Success(
                buildPipeline(
                        sourceTypes.size(),
                        partitionTypesBuilder.build(),
                        partitionChannels,
                        orderTypes,
                        orderChannels,
                        orderSortOrders,
                        functions),
                outputTypes.build());
    }

    private static Optional<CompiledWindowFunction> compileWindowFunction(
            ResolvedFunction resolvedFunction,
            WindowNode.Function function,
            Optional<GpuExpression> peerKey,
            List<Type> orderTypes)
    {
        String functionName = resolvedFunction.name().functionName();
        return switch (functionName) {
            case "row_number" -> rowNumber(function);
            case "rank" -> rank(function, peerKey, orderTypes);
            case "dense_rank" -> denseRank(function, peerKey, orderTypes);
            default -> Optional.empty();
        };
    }

    private static Optional<WindowFrame> compileFrame(WindowNode.Frame frame)
    {
        if (frame.equals(WindowNode.Frame.DEFAULT_FRAME)) {
            return Optional.of(WindowFrame.DEFAULT);
        }
        return Optional.empty();
    }

    private static Optional<CompiledWindowFunction> compileAggregateFunction(
            ResolvedFunction resolvedFunction,
            WindowNode.Function function,
            WindowFrame frame,
            Map<Symbol, Integer> sourceLayout)
    {
        String functionName = resolvedFunction.name().functionName();
        return switch (functionName) {
            case "avg" -> compileAvgDecimal(function, frame, sourceLayout);
            default -> Optional.empty();
        };
    }

    private static Optional<CompiledWindowFunction> compileAvgDecimal(WindowNode.Function function, WindowFrame frame, Map<Symbol, Integer> sourceLayout)
    {
        List<Expression> arguments = function.getArguments();
        if (arguments.size() != 1) {
            return Optional.empty();
        }
        if (!(arguments.getFirst() instanceof Reference reference)) {
            return Optional.empty();
        }

        Symbol symbol = Symbol.from(reference);
        if (!(symbol.type() instanceof DecimalType decimalType)) {
            return Optional.empty();
        }

        int sourceChannel = verifyNotNull(sourceLayout.get(symbol), "channel for symbol %s is not in source layout", symbol);
        if (decimalType.getPrecision() > MAX_DECIMAL128_SUM_AVG_PRECISION) {
            return Optional.of(avgChunkedSumDecimal(sourceChannel, decimalType, frame));
        }
        // Trino scale s means unscaled / 10^s; cuDF scale convention is unscaled * 10^scale, so negate
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, -decimalType.getScale());

        GpuExpression sumInput;
        if (decimalType.isShort()) {
            sumInput = new GpuCast(new GetColumn(sourceChannel), decimal128Type);
        }
        else {
            sumInput = new GetColumn(sourceChannel);
        }

        GpuExpression divideExpression = new GpuDivideRoundToScale(new GetColumn(0), new GetColumn(1));
        GpuExpression postExpression;
        if (decimalType.isShort()) {
            DType decimal64Type = DType.create(DType.DTypeEnum.DECIMAL64, -decimalType.getScale());
            postExpression = new GpuCast(divideExpression, decimal64Type);
        }
        else {
            postExpression = divideExpression;
        }
        CompiledExpression postProjection = new CompiledExpression(
                // count == 0 ? null : (sum / count)
                nullIfZero(new GetColumn(1), postExpression, decimalType),
                new InputChannels(ImmutableList.of(0, 1)));

        return Optional.of(new CompiledWindowFunction(
                ImmutableList.of(
                        new PrefixSumDifferenceComputation(sumInput, frame),
                        new PrefixSumDifferenceComputation(oneIfNotNull(sourceChannel), frame)), // count non-null values
                postProjection,
                decimalType));
    }

    private static CompiledWindowFunction avgChunkedSumDecimal(int sourceChannel, DecimalType decimalType, WindowFrame frame)
    {
        // Sum the four 32-bit chunks of the DECIMAL128 values separately, as sum(decimal) does. Chunks 0..2 are
        // unsigned, chunk 3 carries the sign of the value.
        ImmutableList.Builder<WindowComputation> computations = ImmutableList.builder();
        ImmutableList.Builder<GpuExpression> chunkSums = ImmutableList.builder();
        for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
            DType chunkType = chunkIndex < 3 ? DType.UINT32 : DType.INT32;
            GpuExpression chunk = new GpuExtractInt32Chunk(new GetColumn(sourceChannel), chunkIndex, chunkType);
            computations.add(new PrefixSumDifferenceComputation(new GpuCast(chunk, DType.INT64), frame));
            chunkSums.add(new GetColumn(chunkIndex));
        }
        computations.add(new PrefixSumDifferenceComputation(oneIfNotNull(sourceChannel), frame)); // count non-null values

        CompiledExpression postProjection = new CompiledExpression(
                // count == 0 ? null : (sum / count)
                nullIfZero(new GetColumn(4), new GpuDivideSumChunksRoundToScale(chunkSums.build(), new GetColumn(4), decimalType), decimalType),
                new InputChannels(ImmutableList.of(0, 1, 2, 3, 4)));
        return new CompiledWindowFunction(computations.build(), postProjection, decimalType);
    }

    // INT64 1 where the value is not null and 0 where it is, so its sum over a frame counts the non-null values
    private static GpuExpression oneIfNotNull(int sourceChannel)
    {
        return new GpuCast(new GpuNot(new GpuIsNull(new GetColumn(sourceChannel))), DType.INT64);
    }

    // Null where the non-negative input is zero, and the value elsewhere, which is not evaluated where the input is zero
    private static GpuExpression nullIfZero(GpuExpression input, GpuExpression value, Type type)
    {
        return new GpuIf(
                new GpuBinaryExpression(input, new GpuConstant(BIGINT, Optional.of(0L)), GREATER, DType.BOOL8),
                value,
                new GpuConstant(type, Optional.empty()),
                type);
    }

    private static Optional<GpuExpression> peerKey(int[] orderChannels)
    {
        if (orderChannels.length == 0) {
            return Optional.empty();
        }
        if (orderChannels.length == 1) {
            return Optional.of(new GetColumn(orderChannels[0]));
        }
        ImmutableList.Builder<GpuExpression> fields = ImmutableList.builderWithExpectedSize(orderChannels.length);
        for (int orderChannel : orderChannels) {
            fields.add(new GetColumn(orderChannel));
        }
        return Optional.of(new GpuStruct(fields.build()));
    }

    private static List<GpuOperation.Factory> buildPipeline(
            int sourceColumnCount,
            List<Type> partitionType,
            int[] partitionChannels,
            List<Type> orderType,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<CompiledWindowFunction> functions)
    {
        ImmutableList.Builder<Projection> preProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> postProjections = ImmutableList.builder();
        InputChannels allSourceChannels = allChannels(sourceColumnCount);

        for (int i = 0; i < sourceColumnCount; i++) {
            preProjections.add(new Projection.PassThrough(i));
            postProjections.add(new Projection.PassThrough(i));
        }

        Map<GpuExpression, Integer> projectedInputChannels = sourceExpressionChannels(sourceColumnCount);
        Map<GpuWindow.WindowComputation, Integer> channelBoundComputationIndexes = new LinkedHashMap<>();
        Map<CompiledWindowFunction, int[]> outputIndexes = new IdentityHashMap<>();
        ImmutableList.Builder<GpuWindow.WindowComputation> computations = ImmutableList.builder();
        int currentChannel = sourceColumnCount;

        for (CompiledWindowFunction function : functions) {
            int[] computationIndexes = new int[function.computations().size()];
            for (int i = 0; i < function.computations().size(); i++) {
                WindowComputation computation = function.computations().get(i);
                Integer projectedChannel = projectedInputChannels.get(computation.input());
                if (projectedChannel == null) {
                    projectedChannel = currentChannel++;
                    projectedInputChannels.put(computation.input(), projectedChannel);
                    preProjections.add(new Projection.Gpu(new CompiledExpression(computation.input(), allSourceChannels)));
                }

                GpuWindow.WindowComputation channelBoundComputation = computation.bindToChannel(projectedChannel);
                Integer computationIndex = channelBoundComputationIndexes.get(channelBoundComputation);
                if (computationIndex == null) {
                    computationIndex = channelBoundComputationIndexes.size();
                    channelBoundComputationIndexes.put(channelBoundComputation, computationIndex);
                    computations.add(channelBoundComputation);
                }
                computationIndexes[i] = computationIndex;
            }
            outputIndexes.put(function, computationIndexes);
        }

        ImmutableList.Builder<GpuOperation.Factory> stages = ImmutableList.builder();
        if (currentChannel > sourceColumnCount) {
            stages.add(new GpuProject.Factory(preProjections.build()));
        }
        stages.add(new GpuWindow.Factory(
                partitionType,
                partitionChannels,
                orderType,
                orderChannels,
                orderSortOrders,
                computations.build()));
        int firstResultChannel = currentChannel;
        for (CompiledWindowFunction function : functions) {
            int[] computationIndexes = verifyNotNull(outputIndexes.get(function), "output indexes are missing");
            postProjections.add(new Projection.Gpu(rebind(function.postProjection(), firstResultChannel, computationIndexes)));
        }
        stages.add(new GpuProject.Factory(postProjections.build()));
        return stages.build();
    }

    private static CompiledExpression rebind(CompiledExpression expression, int firstInputChannel, int[] computationIndexes)
    {
        return new CompiledExpression(
                expression.expression(),
                new InputChannels(expression.inputChannels().getInputChannels().stream()
                        .map(channel -> firstInputChannel + computationIndexes[channel])
                        .toList()));
    }

    private static Map<GpuExpression, Integer> sourceExpressionChannels(int sourceColumnCount)
    {
        Map<GpuExpression, Integer> channels = new LinkedHashMap<>();
        for (int channel = 0; channel < sourceColumnCount; channel++) {
            channels.put(new GetColumn(channel), channel);
        }
        return channels;
    }

    private static InputChannels allChannels(int count)
    {
        ImmutableList.Builder<Integer> channels = ImmutableList.builderWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            channels.add(i);
        }
        return new InputChannels(channels.build());
    }

    private record CompiledWindowFunction(
            List<WindowComputation> computations,
            CompiledExpression postProjection,
            Type outputType)
    {
        public CompiledWindowFunction
        {
            checkArgument(!computations.isEmpty(), "computations is empty");
            computations = ImmutableList.copyOf(computations);
            requireNonNull(postProjection, "postProjection is null");
            requireNonNull(outputType, "outputType is null");
        }
    }

    private static Optional<CompiledWindowFunction> rank(WindowNode.Function function, Optional<GpuExpression> peerKey, List<Type> orderTypes)
    {
        if (hasArgumentsOrNonBigintResult(function)) {
            return Optional.empty();
        }
        if (orderTypes.stream().anyMatch(type -> type instanceof VarbinaryType)) {
            // VARBINARY maps to cuDF list columns, which grouped rank scan rejects.
            return Optional.empty();
        }
        return Optional.of(new CompiledWindowFunction(
                ImmutableList.of(
                        new ScanComputation(
                                peerKey.orElse(ONE),
                                ScanAggregation.rank(),
                                GroupByScanAggregation.rank(),
                                RANK_SCAN_OUTPUT_BYTES_PER_ROW,
                                RANK_GROUPED_SCAN_WORKING_BYTES_PER_ROW)),
                CAST_SCAN_RESULT_TO_BIGINT,
                BIGINT));
    }

    private static Optional<CompiledWindowFunction> denseRank(WindowNode.Function function, Optional<GpuExpression> peerKey, List<Type> orderTypes)
    {
        if (hasArgumentsOrNonBigintResult(function)) {
            return Optional.empty();
        }
        if (orderTypes.stream().anyMatch(type -> type instanceof VarbinaryType)) {
            // VARBINARY maps to cuDF list columns, which grouped rank scan rejects.
            return Optional.empty();
        }
        return Optional.of(new CompiledWindowFunction(
                ImmutableList.of(
                        new ScanComputation(
                                peerKey.orElse(ONE),
                                ScanAggregation.denseRank(),
                                GroupByScanAggregation.denseRank(),
                                RANK_SCAN_OUTPUT_BYTES_PER_ROW,
                                RANK_GROUPED_SCAN_WORKING_BYTES_PER_ROW)),
                CAST_SCAN_RESULT_TO_BIGINT,
                BIGINT));
    }

    private static Optional<CompiledWindowFunction> rowNumber(WindowNode.Function function)
    {
        if (hasArgumentsOrNonBigintResult(function)) {
            return Optional.empty();
        }
        return Optional.of(new CompiledWindowFunction(
                ImmutableList.of(
                        new ScanComputation(
                                ONE,
                                ScanAggregation.sum(),
                                GroupByScanAggregation.sum(),
                                ROW_NUMBER_SCAN_OUTPUT_BYTES_PER_ROW,
                                ROW_NUMBER_GROUPED_SCAN_WORKING_BYTES_PER_ROW)),
                PASSTHROUGH,
                BIGINT));
    }

    private static boolean hasArgumentsOrNonBigintResult(WindowNode.Function function)
    {
        return !function.getArguments().isEmpty() || function.getResolvedFunction().signature().getReturnType() != BIGINT;
    }

    private sealed interface WindowComputation
    {
        GpuExpression input();

        GpuWindow.WindowComputation bindToChannel(int inputChannel);
    }

    private record ScanComputation(
            GpuExpression input,
            ScanAggregation scan,
            GroupByScanAggregation groupedScan,
            int scanOutputBytesPerRow,
            int groupedScanWorkingBytesPerRow)
            implements WindowComputation
    {
        public ScanComputation
        {
            requireNonNull(input, "input is null");
            requireNonNull(scan, "scan is null");
            requireNonNull(groupedScan, "groupedScan is null");
            checkArgument(scanOutputBytesPerRow > 0, "scanOutputBytesPerRow must be positive");
            checkArgument(groupedScanWorkingBytesPerRow >= 0, "groupedScanWorkingBytesPerRow is negative");
        }

        @Override
        public GpuWindow.WindowComputation bindToChannel(int inputChannel)
        {
            return new GpuWindow.ScanComputation(inputChannel, scan, groupedScan, scanOutputBytesPerRow, groupedScanWorkingBytesPerRow);
        }
    }

    private record PrefixSumDifferenceComputation(GpuExpression input, WindowFrame frame)
            implements WindowComputation
    {
        public PrefixSumDifferenceComputation
        {
            requireNonNull(input, "input is null");
            requireNonNull(frame, "frame is null");
        }

        @Override
        public GpuWindow.WindowComputation bindToChannel(int inputChannel)
        {
            return new GpuWindow.PrefixSumDifferenceComputation(inputChannel, frame);
        }
    }
}
