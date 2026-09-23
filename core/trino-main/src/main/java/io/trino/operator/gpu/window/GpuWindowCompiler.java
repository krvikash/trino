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
import io.trino.operator.gpu.expression.GpuCast;
import io.trino.operator.gpu.expression.GpuConstant;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.expression.GpuStruct;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.WindowNode;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

            if (resolvedFunction.functionKind() != FunctionKind.WINDOW) {
                return new CompileResult.Failure("non-window function: " + resolvedFunction.name());
            }
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
            if (!function.getFrame().equals(WindowNode.Frame.DEFAULT_FRAME)) {
                return new CompileResult.Failure("non-default frame: " + resolvedFunction.name());
            }

            String functionName = resolvedFunction.name().functionName();
            Optional<CompiledWindowFunction> compiledFunction = compileWindowFunction(resolvedFunction, function, peerKey, orderTypes);
            if (compiledFunction.isEmpty()) {
                return new CompileResult.Failure("unsupported window function: " + functionName);
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
}
