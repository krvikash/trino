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
package io.trino.operator.gpu.aggregation;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuProject;
import io.trino.operator.gpu.GpuProject.Projection;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GetColumn;
import io.trino.operator.gpu.expression.GpuCast;
import io.trino.operator.gpu.expression.GpuCombineDecimalStateSumsToDecimal128;
import io.trino.operator.gpu.expression.GpuCombineSumChunksToVarbinary;
import io.trino.operator.gpu.expression.GpuDecimal128AsVarbinary;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.expression.GpuExtractDecimalStateChunk;
import io.trino.operator.gpu.expression.GpuExtractInt32Chunk;
import io.trino.operator.gpu.expression.GpuPackAvgDecimalState;
import io.trino.operator.project.InputChannels;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static ai.rapids.cudf.DType.FLOAT64;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.operator.gpu.SignatureFormatter.formatAggregation;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

public final class GpuAggregationCompiler
{
    private GpuAggregationCompiler() {}

    private static final Logger log = Logger.get(GpuAggregationCompiler.class);

    public static AggregationCompileResult compile(AggregationNode node, Map<Symbol, Integer> sourceLayout, DataSize compactionThreshold)
    {
        return compile(node, sourceLayout, compactionThreshold.toBytes());
    }

    @VisibleForTesting
    public static AggregationCompileResult compile(AggregationNode node, Map<Symbol, Integer> sourceLayout, long compactionThresholdBytes)
    {
        Step step = node.getStep();

        if (node.getGroupingSetCount() > 1) {
            log.debug("Could not compile aggregation with %s grouping sets for GPU execution", node.getGroupingSetCount());
            return new AggregationCompileResult.Failure("multiple grouping sets");
        }

        List<Symbol> groupingKeys = node.getGroupingKeys();
        ImmutableList.Builder<Type> groupByTypesBuilder = ImmutableList.builder();
        int[] groupByChannels = new int[groupingKeys.size()];

        for (int i = 0; i < groupingKeys.size(); i++) {
            Symbol symbol = groupingKeys.get(i);
            Type type = symbol.type();
            if (!isConvertible(type)) {
                return new AggregationCompileResult.Failure("grouping key type: " + type);
            }
            groupByTypesBuilder.add(type);
            groupByChannels[i] = verifyNotNull(sourceLayout.get(symbol), "channel for symbol %s is not in source layout", symbol);
        }
        List<Type> groupByTypes = groupByTypesBuilder.build();

        List<AggregateCompilation> compilations = new ArrayList<>();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Symbol outputSymbol = entry.getKey();
            Aggregation aggregation = entry.getValue();
            Optional<AggregateCompilation> compiled = compileAggregation(outputSymbol, aggregation, sourceLayout, step);
            if (compiled.isEmpty()) {
                log.debug(
                        "Could not compile aggregation function %s at step %s with filter=%s mask=%s distinct=%s ordered=%s",
                        aggregation.getResolvedFunction().signature(),
                        step,
                        aggregation.getFilter().isPresent(),
                        aggregation.getMask().isPresent(),
                        aggregation.isDistinct(),
                        aggregation.getOrderingScheme().isPresent());
                return new AggregationCompileResult.Failure(formatAggregation(aggregation));
            }
            compiled.ifPresent(compilation -> verify(
                    compilation.outputType().equals(outputSymbol.type()),
                    "Expected compiled %s %s aggregate to produce %s but got %s",
                    aggregation.getResolvedFunction().name(),
                    step,
                    outputSymbol.type(),
                    compilation.outputType()));
            compilations.add(compiled.get());
        }

        return buildPipeline(sourceLayout.size(), groupByChannels, groupByTypes, compilations, step, compactionThresholdBytes);
    }

    /**
     * Wires the per-aggregate compilations into a (pre-projection, aggregation, post-projection)
     * pipeline. The pre-projection is always emitted and produces the grouping keys followed by one
     * derived column per distinct aggregate input expression; source columns that neither a key nor
     * an aggregate reads are dropped. Aggregate inputs are deduplicated across aggregates by value
     * equality, so an expression requested more than once is computed once.
     */
    private static AggregationCompileResult buildPipeline(
            int sourceColumnCount,
            int[] groupByChannels,
            List<Type> groupByTypes,
            List<AggregateCompilation> compilations,
            Step step,
            long compactionThresholdBytes)
    {
        InputChannels allSourceChannels = allChannels(sourceColumnCount);

        ImmutableList.Builder<Projection> preProjections = ImmutableList.builder();
        ImmutableList.Builder<GpuAggregateFunction> aggregates = ImmutableList.builder();
        ImmutableList.Builder<Projection> postProjections = ImmutableList.builder();
        ImmutableList.Builder<Type> postProjectionTypes = ImmutableList.builder();

        // Pre-projection starts with the grouping keys (channels 0..g-1). The aggregation output
        // carries the keys first too, so the post-projection passes them straight through.
        int groupByCount = groupByChannels.length;
        int[] aggregationGroupByChannels = new int[groupByCount];
        for (int i = 0; i < groupByCount; i++) {
            preProjections.add(new Projection.PassThrough(groupByChannels[i]));
            aggregationGroupByChannels[i] = i;
            postProjections.add(new Projection.PassThrough(i));
            postProjectionTypes.add(groupByTypes.get(i));
        }

        // Aggregate input columns follow the keys and are deduplicated by expression value equality.
        Map<GpuExpression, Integer> derivedChannels = new HashMap<>();
        int currentChannel = groupByCount;
        int currentAggChannel = groupByCount;
        for (AggregateCompilation compilation : compilations) {
            int aggChannel = currentAggChannel;
            int slotCount = compilation.slots().size();
            for (AggregateSlot slot : compilation.slots()) {
                int inputChannel = -1;
                if (slot.input().isPresent()) {
                    GpuExpression inputExpression = slot.input().get();
                    Integer derivedChannel = derivedChannels.get(inputExpression);
                    if (derivedChannel == null) {
                        derivedChannel = currentChannel++;
                        derivedChannels.put(inputExpression, derivedChannel);
                        preProjections.add(new Projection.Gpu(new CompiledExpression(inputExpression, allSourceChannels)));
                    }
                    inputChannel = derivedChannel;
                }
                // Input-less aggregates (e.g. count(*)) ignore the channel; -1 makes a stray read fail loudly.
                aggregates.add(slot.factory().build(inputChannel));
            }
            currentAggChannel += slotCount;

            if (compilation.postProjection().isPresent()) {
                postProjections.add(new Projection.Gpu(compilation.postProjection().get().rebind(aggChannel, slotCount)));
            }
            else {
                verify(slotCount == 1, "Aggregate without post-projection must have exactly one slot, got %s", slotCount);
                postProjections.add(new Projection.PassThrough(aggChannel));
            }
            postProjectionTypes.add(compilation.outputType());
        }

        List<GpuOperation.Factory> stages = new ArrayList<>();
        stages.add(new GpuProject.Factory(preProjections.build()));
        stages.add(new GpuAggregation.Factory(
                aggregates.build(),
                aggregationGroupByChannels,
                groupByTypes,
                step.isInputRaw(),
                step.isOutputPartial(),
                compactionThresholdBytes,
                currentChannel));
        stages.add(new GpuProject.Factory(postProjections.build()));

        return new AggregationCompileResult.Success(stages, postProjectionTypes.build());
    }

    /**
     * Compile a single aggregation entry into a per-aggregate plan: the pre-projection columns
     * it needs, the cuDF aggregations to run, and the post-projection that reshapes the slot
     * results into the aggregate's declared output column.
     */
    private static Optional<AggregateCompilation> compileAggregation(Symbol outputSymbol, Aggregation aggregation, Map<Symbol, Integer> sourceLayout, Step step)
    {
        BoundSignature signature = aggregation.getResolvedFunction().signature();
        if (!isBuiltinFunctionName(signature.getName())) {
            return Optional.empty();
        }
        String name = signature.getName().functionName();

        if (aggregation.isDistinct() || aggregation.getFilter().isPresent() || aggregation.getOrderingScheme().isPresent()) {
            // No DISTINCT, FILTER, or ORDER BY support yet
            return Optional.empty();
        }

        OptionalInt maskChannel = aggregation.getMask()
                .map(mask -> OptionalInt.of(verifyNotNull(sourceLayout.get(mask), "channel for mask symbol %s is not in source layout", mask)))
                .orElse(OptionalInt.empty());

        List<Expression> arguments = aggregation.getArguments();
        Type outputType = outputSymbol.type();

        return switch (name) {
            // For count, all steps produce the same type
            case "count" -> compileCount(arguments, sourceLayout, outputType, maskChannel);
            // For currently supported sum, all steps produce the same type
            case "sum" -> compileSum(arguments, sourceLayout, step, outputType, signature.getReturnType(), maskChannel);
            // For min and max, all steps produce the same type
            case "min", "bool_and" -> compileMinMax(arguments, sourceLayout, outputType, GpuMin::new, maskChannel);
            case "max", "bool_or" -> compileMinMax(arguments, sourceLayout, outputType, GpuMax::new, maskChannel);
            case "avg" -> compileAvg(arguments, sourceLayout, step, outputType, maskChannel);
            case "any_value" -> compileAnyValue(arguments, sourceLayout, outputType, maskChannel);
            default -> Optional.empty();
        };
    }

    private static Optional<AggregateCompilation> compileCount(List<Expression> arguments, Map<Symbol, Integer> sourceLayout, Type outputType, OptionalInt maskChannel)
    {
        return toDType(outputType)
                .flatMap(dType -> {
                    if (arguments.isEmpty()) {
                        if (maskChannel.isPresent()) {
                            int mask = maskChannel.getAsInt();
                            return Optional.of(AggregateCompilation.single(
                                    outputType,
                                    maskExpression(mask, mask),
                                    channel -> new GpuCountNonNull(channel, outputType, dType)));
                        }
                        return Optional.of(new AggregateCompilation(
                                outputType,
                                ImmutableList.of(AggregateSlot.noInput(_ -> new GpuCountAll(outputType, dType))),
                                Optional.empty()));
                    }
                    return getSingleColumnReference(arguments, sourceLayout)
                            .filter(column -> isConvertible(column.type()))
                            .map(column -> {
                                GpuExpression input = maskChannel.isPresent()
                                        ? maskExpression(maskChannel.getAsInt(), column.channel())
                                        : new GetColumn(column.channel());
                                return AggregateCompilation.single(outputType, input, channel -> new GpuCountNonNull(channel, outputType, dType));
                            });
                });
    }

    private static Optional<AggregateCompilation> compileSum(
            List<Expression> arguments,
            Map<Symbol, Integer> sourceLayout,
            Step step,
            Type outputType,
            Type finalStepOutputType,
            OptionalInt maskChannel)
    {
        Optional<ColumnReference> column = getSingleColumnReference(arguments, sourceLayout);
        if (column.isEmpty()) {
            return Optional.empty();
        }

        Type argumentType = column.get().type();
        return switch (argumentType) {
            case BigintType _ -> toDType(argumentType).map(dType -> {
                GpuExpression input = maskChannel.isPresent()
                        ? maskExpression(maskChannel.getAsInt(), column.get().channel())
                        : new GetColumn(column.get().channel());
                return AggregateCompilation.single(outputType, input, channel -> new GpuSum(channel, outputType, dType));
            });

            case RealType _, DoubleType _ -> {
                verify(outputType == REAL || outputType == DOUBLE, "Unexpected outputType for sum with argument type %s: %s", argumentType, outputType);
                verify(!step.isOutputPartial() || outputType == DOUBLE, "Unexpected outputType for sum with argument type %s at step %s: %s", argumentType, step, outputType);

                GpuExpression input = new GetColumn(column.get().channel());
                if (maskChannel.isPresent()) {
                    input = new Mask(new GetColumn(maskChannel.getAsInt()), input);
                }
                // sum(REAL) and sum(DOUBLE) both use DOUBLE as accumulator type
                if (step.isInputRaw() && argumentType == REAL) {
                    input = new GpuCast(input, FLOAT64);
                }

                Optional<PostProjection> postProjection = Optional.empty();
                // for sum(REAL), the final result type is REAL
                if (outputType == REAL) {
                    postProjection = Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuCast(new GetColumn(0), DType.FLOAT32),
                            new InputChannels(channels[0]))));
                }

                yield Optional.of(new AggregateCompilation(
                        outputType,
                        ImmutableList.of(AggregateSlot.of(input, channel -> new GpuSum(channel, outputType, FLOAT64))),
                        postProjection));
            }

            case DecimalType _ -> {
                // Decimal sum uses pre-projections for chunk extraction; composing with mask would require two-stage pre-projections which is not supported yet.
                if (maskChannel.isPresent()) {
                    yield Optional.empty();
                }
                // sum(decimal(p,s)) → decimal(38,s) with a VARBINARY-serialized intermediate.
                // Decimal-typed argument means PARTIAL (decimal → VARBINARY) or SINGLE (decimal → decimal).
                yield switch (step) {
                    case PARTIAL -> compileSumDecimalPartial(column.get(), outputType, (DecimalType) finalStepOutputType);
                    case SINGLE -> Optional.empty(); // TODO: chunked sum + final reduce
                    case FINAL, INTERMEDIATE -> throw new IllegalStateException(
                            "decimal argument unexpected at sum(decimal) step " + step);
                };
            }

            default -> {
                // Decimal FINAL/INTERMEDIATE uses chunk extraction pre-projections; composing with mask would require two-stage pre-projections which is not supported yet.
                if (maskChannel.isPresent()) {
                    yield Optional.empty();
                }
                // FINAL / INTERMEDIATE of sum(decimal) read the VARBINARY intermediate state, not
                // the original decimal argument. Recognize by the function return type.
                yield (column.get().type().equals(VARBINARY) && finalStepOutputType instanceof DecimalType decimalReturn)
                        ? switch (step) {
                    case FINAL -> compileSumDecimalFinal(column.get(), outputType, decimalReturn);
                    case INTERMEDIATE -> Optional.empty(); // TODO: deserialize, sum, re-serialize
                    case PARTIAL, SINGLE -> Optional.empty();
                }
                        : Optional.empty();
            }
        };
    }

    private static Optional<AggregateCompilation> compileSumDecimalPartial(ColumnReference column, Type outputType, DecimalType finalStepOutputType)
    {
        DecimalType argumentType = (DecimalType) column.type();
        verify(outputType.equals(VARBINARY), "sum(decimal) PARTIAL output symbol must be VARBINARY, got %s", outputType);
        // Trino sum(decimal(p,s)) always returns decimal(38, s) — DECIMAL128 in cuDF.
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, -finalStepOutputType.getScale());

        if (argumentType.isShort()) {
            return Optional.of(AggregateCompilation.shortDecimalSumPartial(column.channel(), argumentType, decimal128Type));
        }
        return Optional.of(AggregateCompilation.longDecimalSumPartial(column.channel(), decimal128Type));
    }

    private static Optional<AggregateCompilation> compileSumDecimalFinal(ColumnReference column, Type outputType, DecimalType finalStepOutputType)
    {
        verify(column.type().equals(VARBINARY), "sum(decimal) FINAL input must be VARBINARY, got %s", column.type());
        verify(outputType.equals(finalStepOutputType),
                "sum(decimal) FINAL output type must equal function return type, got %s vs %s",
                outputType,
                finalStepOutputType);
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, -finalStepOutputType.getScale());
        return Optional.of(AggregateCompilation.decimalSumFinal(column.channel(), decimal128Type, finalStepOutputType));
    }

    private static Optional<AggregateCompilation> compileAvg(List<Expression> arguments, Map<Symbol, Integer> sourceLayout, Step step, Type outputType, OptionalInt maskChannel)
    {
        if (step != Step.PARTIAL) {
            return Optional.empty();
        }

        // avg(decimal) PARTIAL uses pre-projections; composing with mask would require two-stage pre-projections which is not supported yet.
        if (maskChannel.isPresent()) {
            return Optional.empty();
        }

        Optional<ColumnReference> column = getSingleColumnReference(arguments, sourceLayout);
        if (column.isEmpty()) {
            return Optional.empty();
        }

        Type argumentType = column.get().type();
        if (!(argumentType instanceof DecimalType decimalType) || !decimalType.isShort()) {
            return Optional.empty();
        }

        verify(outputType.equals(VARBINARY), "avg(decimal) PARTIAL output symbol must be VARBINARY, got %s", outputType);
        return compileAvgShortDecimalPartial(column.get(), decimalType);
    }

    private static Optional<AggregateCompilation> compileAvgShortDecimalPartial(ColumnReference column, DecimalType inputDecimalType)
    {
        int negatedScale = -inputDecimalType.getScale();
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, negatedScale);
        GpuExpression cast = new GpuCast(new GetColumn(column.channel()), decimal128Type);
        GpuExpression passthrough = new GetColumn(column.channel());

        Type sumOutputType = decimalSumOutputType(inputDecimalType);

        return Optional.of(new AggregateCompilation(
                VARBINARY,
                ImmutableList.of(
                        AggregateSlot.of(cast, channel -> new GpuSum(channel, sumOutputType, decimal128Type)),
                        AggregateSlot.of(passthrough, channel -> new GpuCountNonNull(channel, BigintType.BIGINT, DType.INT64))),
                Optional.of(new PostProjection(channels -> new CompiledExpression(
                        new GpuPackAvgDecimalState(),
                        new InputChannels(ImmutableList.of(channels[0], channels[1])))))));
    }

    private static Optional<AggregateCompilation> compileMinMax(
            List<Expression> arguments,
            Map<Symbol, Integer> sourceLayout,
            Type returnType,
            MinMaxFactory factory,
            OptionalInt maskChannel)
    {
        return getSingleColumnReference(arguments, sourceLayout)
                .flatMap(column -> {
                    switch (column.type()) {
                        case BooleanType _,
                             TinyintType _, SmallintType _, IntegerType _, BigintType _,
                             RealType _, DoubleType _,
                             DecimalType _,
                             CharType _, VarcharType _, DateType _ -> {
                            // cudf comparison semantics for carrier DType match those of Trino Type
                        }
                        case TimestampType timestampType when timestampType.getPrecision() <= 9 -> {
                            // cudf comparison semantics for carrier DType match those of Trino Type
                        }
                        default -> {
                            return Optional.empty();
                        }
                    }
                    return toDType(returnType).map(dType -> {
                        GpuExpression input = maskChannel.isPresent()
                                ? maskExpression(maskChannel.getAsInt(), column.channel())
                                : new GetColumn(column.channel());
                        return AggregateCompilation.single(returnType, input, channel -> factory.create(channel, returnType, dType));
                    });
                });
    }

    private static Optional<AggregateCompilation> compileAnyValue(
            List<Expression> arguments,
            Map<Symbol, Integer> sourceLayout,
            Type returnType,
            OptionalInt maskChannel)
    {
        return getSingleColumnReference(arguments, sourceLayout)
                .flatMap(column -> toDType(returnType)
                        .map(dType -> {
                            GpuExpression input = maskChannel.isPresent()
                                    ? maskExpression(maskChannel.getAsInt(), column.channel())
                                    : new GetColumn(column.channel());
                            return AggregateCompilation.single(returnType, input, channel -> new GpuAnyValue(channel, returnType, dType));
                        }));
    }

    private static Optional<ColumnReference> getSingleColumnReference(List<Expression> arguments, Map<Symbol, Integer> sourceLayout)
    {
        if (arguments.size() != 1) {
            return Optional.empty();
        }
        if (!(arguments.getFirst() instanceof Reference reference)) {
            throw new IllegalArgumentException("Expected Reference but got: " + arguments.getFirst().getClass().getSimpleName());
        }

        Symbol symbol = Symbol.from(reference);
        int channel = verifyNotNull(sourceLayout.get(symbol), "channel for symbol %s is not in source layout", symbol);

        return Optional.of(new ColumnReference(channel, symbol.type()));
    }

    private record ColumnReference(int channel, Type type)
    {
        public ColumnReference
        {
            requireNonNull(type, "type is null");
        }
    }

    @FunctionalInterface
    private interface MinMaxFactory
    {
        GpuAggregateFunction create(int channel, Type outputType, DType outputDType);
    }

    public sealed interface AggregationCompileResult
    {
        record Success(List<GpuOperation.Factory> stages, List<Type> finalOutputTypes)
                implements AggregationCompileResult
        {
            public Success
            {
                stages = ImmutableList.copyOf(requireNonNull(stages, "stages is null"));
                finalOutputTypes = ImmutableList.copyOf(requireNonNull(finalOutputTypes, "finalOutputTypes is null"));
            }
        }

        record Failure(String reason)
                implements AggregationCompileResult
        {
            public Failure
            {
                requireNonNull(reason, "reason is null");
            }
        }
    }

    /**
     * Per-aggregate compilation result. Captures everything needed to wire one logical aggregate
     * into the pre-projection / aggregation / post-projection pipeline.
     *
     * @param outputType the Trino type the aggregate's output column carries (the AggregationNode's
     *         symbol type for this aggregate — the *intermediate* type for PARTIAL, or
     *         the return type for SINGLE/FINAL)
     * @param slots one or more aggregate slots — multiple for chunked aggregations like long-decimal
     *         sum (4 INT64 sums of UINT32/INT32 chunks). Each slot declares the expression producing
     *         its input column (empty for input-less aggregates such as count(*)) and a factory that
     *         builds the {@link GpuAggregateFunction} once the input's channel is resolved.
     * @param postProjection optional reshape from N slot result columns back to one output column
     *         — e.g. reassembling four chunk sums into a 16-byte LIST&lt;INT8&gt;.
     */
    private record AggregateCompilation(
            Type outputType,
            List<AggregateSlot> slots,
            Optional<PostProjection> postProjection)
    {
        AggregateCompilation
        {
            requireNonNull(outputType, "outputType is null");
            slots = List.copyOf(slots);
            requireNonNull(postProjection, "postProjection is null");
            verify(!slots.isEmpty(), "slots is empty");
            // The pipeline builder pass-throughs the single aggregate slot when no post-projection
            // is provided, so multi-slot compilations must declare one.
            verify(postProjection.isPresent() || slots.size() == 1,
                    "Aggregate without post-projection must have exactly one slot, got %s",
                    slots.size());
        }

        static AggregateCompilation single(Type outputType, GpuExpression input, AggregateSlotFactory factory)
        {
            return new AggregateCompilation(outputType, List.of(AggregateSlot.of(input, factory)), Optional.empty());
        }

        static AggregateCompilation shortDecimalSumPartial(int sourceChannel, DecimalType inputDecimalType, DType decimal128Type)
        {
            // Pre-projection: cast DECIMAL64(s) → DECIMAL128(s) so the SUM accumulates with
            // 128-bit width. Sum(short_decimal) cannot overflow DECIMAL128: max input
            // magnitude < 10^18, max DECIMAL128 ~ 10^38, max rows per cuDF batch ~ 2^31.
            int negatedScale = -inputDecimalType.getScale();
            verify(decimal128Type.getScale() == negatedScale,
                    "decimal128Type must have the same scale as the input, got %s vs %s",
                    decimal128Type.getScale(),
                    negatedScale);
            GpuExpression cast = new GpuCast(new GetColumn(sourceChannel), decimal128Type);
            Type sumOutputType = decimalSumOutputType(inputDecimalType);
            return new AggregateCompilation(
                    VARBINARY,
                    List.of(AggregateSlot.of(cast, channel -> new GpuSum(channel, sumOutputType, decimal128Type))),
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuDecimal128AsVarbinary(),
                            new InputChannels(List.of(channels[0]))))));
        }

        static AggregateCompilation longDecimalSumPartial(int sourceChannel, DType decimal128Type)
        {
            // Pre-projection: extract the four 32-bit chunks of the DECIMAL128 input. Chunks
            // 0..2 are unsigned, chunk 3 carries the sign of the value.
            List<AggregateSlot> slots = List.of(
                    chunkSlot(sourceChannel, 0, DType.UINT32),
                    chunkSlot(sourceChannel, 1, DType.UINT32),
                    chunkSlot(sourceChannel, 2, DType.UINT32),
                    chunkSlot(sourceChannel, 3, DType.INT32));

            return new AggregateCompilation(
                    VARBINARY,
                    slots,
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuCombineSumChunksToVarbinary(decimal128Type),
                            new InputChannels(List.of(channels[0], channels[1], channels[2], channels[3]))))));
        }

        private static AggregateSlot chunkSlot(int sourceChannel, int chunkIdx, DType chunkType)
        {
            // cuDF's grouped SUM on UINT32/INT32 widens to INT64; combineInt64SumChunks accepts any
            // 8-byte type for chunks 0..2 and requires INT64 for chunk 3, so INT64 across the board
            // satisfies it. The slot result columns are consumed by the post-projection and never
            // copied to a Trino block, so the Trino type only has to match the cuDF dtype.
            return AggregateSlot.of(
                    new GpuExtractInt32Chunk(new GetColumn(sourceChannel), chunkIdx, chunkType),
                    channel -> new GpuSum(channel, BigintType.BIGINT, DType.INT64));
        }

        static AggregateCompilation decimalSumFinal(int sourceChannel, DType decimal128Type, Type outputType)
        {
            // Pre-projection: unpack the VARBINARY intermediate state of sum(decimal) into 5 fixed-
            // width components — 4 INT32 chunks of the 128-bit running sum, plus the running
            // overflow long. Variable-length input (8/16/24 byte rows from CPU PARTIAL or uniform
            // 16-byte from GPU PARTIAL) is handled inside GpuExtractDecimalStateChunk.
            List<AggregateSlot> slots = List.of(
                    stateComponentSlot(sourceChannel, 0),
                    stateComponentSlot(sourceChannel, 1),
                    stateComponentSlot(sourceChannel, 2),
                    stateComponentSlot(sourceChannel, 3),
                    stateComponentSlot(sourceChannel, 4));

            return new AggregateCompilation(
                    outputType,
                    slots,
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuCombineDecimalStateSumsToDecimal128(decimal128Type),
                            new InputChannels(List.of(channels[0], channels[1], channels[2], channels[3], channels[4]))))));
        }

        private static AggregateSlot stateComponentSlot(int sourceChannel, int componentIdx)
        {
            // 5 INT64 sums, mirroring chunkSlot — cuDF SUM widens UINT32/INT32 inputs to INT64 with
            // the right (zero/sign) extension. The 5th sum accumulates the per-state overflow long.
            return AggregateSlot.of(
                    new GpuExtractDecimalStateChunk(new GetColumn(sourceChannel), componentIdx),
                    channel -> new GpuSum(channel, BigintType.BIGINT, DType.INT64));
        }
    }

    /**
     * One aggregate slot: the expression producing its input column (empty for input-less aggregates
     * such as count(*)) paired with a factory that builds the {@link GpuAggregateFunction} once the
     * input's channel in the pre-projection output is resolved.
     */
    private record AggregateSlot(Optional<GpuExpression> input, AggregateSlotFactory factory)
    {
        AggregateSlot
        {
            requireNonNull(input, "input is null");
            requireNonNull(factory, "factory is null");
        }

        static AggregateSlot of(GpuExpression input, AggregateSlotFactory factory)
        {
            return new AggregateSlot(Optional.of(input), factory);
        }

        static AggregateSlot noInput(AggregateSlotFactory factory)
        {
            return new AggregateSlot(Optional.empty(), factory);
        }
    }

    /**
     * Builds a single aggregate once its input channel in the pre-projection output is known.
     * Input-less aggregates (e.g. count(*)) ignore the argument.
     */
    @FunctionalInterface
    private interface AggregateSlotFactory
    {
        GpuAggregateFunction build(int inputChannel);
    }

    /**
     * Builder for the post-projection {@link CompiledExpression} of a multi-slot aggregate. The
     * compiler doesn't know the absolute aggregation result channels until the layout is fixed,
     * so each per-aggregate compilation hands back a closure that takes the resolved channels.
     */
    private record PostProjection(ChannelToExpression buildExpression)
    {
        CompiledExpression rebind(int firstSlotChannel, int slotCount)
        {
            int[] channels = new int[slotCount];
            for (int i = 0; i < slotCount; i++) {
                channels[i] = firstSlotChannel + i;
            }
            return buildExpression.build(channels);
        }
    }

    @FunctionalInterface
    private interface ChannelToExpression
    {
        CompiledExpression build(int[] channels);
    }

    private static Type decimalSumOutputType(DecimalType input)
    {
        // Mirrors Trino's signature: sum(decimal(p, s)) returns decimal(38, s).
        return DecimalType.createDecimalType(38, input.getScale());
    }

    private static GpuExpression maskExpression(int maskChannel, int valueChannel)
    {
        return new Mask(new GetColumn(maskChannel), new GetColumn(valueChannel));
    }

    private static InputChannels allChannels(int count)
    {
        ImmutableList.Builder<Integer> channels = ImmutableList.builderWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            channels.add(i);
        }
        return new InputChannels(channels.build());
    }

    private static final class Mask
            extends GpuExpression
    {
        private final GpuExpression mask;
        private final GpuExpression value;

        public Mask(GpuExpression mask, GpuExpression value)
        {
            this.mask = requireNonNull(mask, "mask is null");
            this.value = requireNonNull(value, "value is null");
        }

        @Override
        public ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
        {
            try (ColumnVector maskVector = mask.evaluate(positionCount, inputColumns);
                    ColumnVector valueVector = value.evaluate(positionCount, inputColumns)) {
                return mask(maskVector, valueVector);
            }
        }

        private static @Move ColumnVector mask(@Borrow ColumnVector mask, @Borrow ColumnVector value)
        {
            checkArgument(mask.getType() == DType.BOOL8, "Unexpected mask type: %s", mask.getType());
            try (Scalar nullScalar = Scalar.fromNull(value.getType())) {
                return mask.ifElse(value, nullScalar);
            }
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof Mask other
                    && mask.equals(other.mask)
                    && value.equals(other.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getClass(), mask, value);
        }
    }
}
