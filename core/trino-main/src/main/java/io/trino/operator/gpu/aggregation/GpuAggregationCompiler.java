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
import io.trino.operator.gpu.expression.GetOnlyColumn;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static ai.rapids.cudf.DType.FLOAT64;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.Lists.newArrayList;
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
     * Wires the per-aggregate compilations into a pipeline of (optional pre-projection,
     * aggregation, optional post-projection). Pre-/post-projection are emitted only if at
     * least one aggregate needs them — i.e. the existing single-stage behavior is preserved
     * for queries without sum(decimal).
     */
    private static AggregationCompileResult buildPipeline(
            int sourceColumnCount,
            int[] groupByChannels,
            List<Type> groupByTypes,
            List<AggregateCompilation> compilations,
            Step step,
            long compactionThresholdBytes)
    {
        boolean needsPipeline = compilations.stream().anyMatch(c -> !c.preProjection().isEmpty() || c.postProjection().isPresent());

        if (!needsPipeline) {
            ImmutableList.Builder<GpuAggregateFunction> aggregates = ImmutableList.builder();
            for (AggregateCompilation compilation : compilations) {
                // Simple compilations have no preProjection, so their factories ignore the channel
                // and return the pre-bound aggregate; the value below is unused.
                aggregates.add(compilation.aggregates().getFirst().build(0));
            }
            GpuAggregation.Factory aggregation = new GpuAggregation.Factory(
                    aggregates.build(),
                    groupByChannels,
                    groupByTypes,
                    step.isInputRaw(),
                    compactionThresholdBytes,
                    sourceColumnCount);
            return new AggregationCompileResult.Success(List.of(aggregation), aggregation.getOutputTypes());
        }

        // Pre-projection: pass through all source columns, then append derived columns (chunks /
        // upcasts) for each aggregate that needs them. The aggregation's input channels for those
        // aggregates point to the appended columns.
        ImmutableList.Builder<Projection> preProjections = ImmutableList.builder();
        for (int i = 0; i < sourceColumnCount; i++) {
            preProjections.add(new Projection.PassThrough(i));
        }
        // Aggregates with input channels rewired to point at appended pre-projection columns when
        // a derived input is present; left unchanged otherwise.
        ImmutableList.Builder<GpuAggregateFunction> aggregates = ImmutableList.builder();
        // Post-projection: pass-through group keys, then either pass-through or run the
        // per-aggregate post-projection on its slot results.
        ImmutableList.Builder<Projection> postProjections = ImmutableList.builder();
        ImmutableList.Builder<Type> postProjectionTypes = ImmutableList.builder();
        for (int i = 0; i < groupByChannels.length; i++) {
            postProjections.add(new Projection.PassThrough(i));
            postProjectionTypes.add(groupByTypes.get(i));
        }

        int currentDerivedChannel = sourceColumnCount;
        int currentAggChannel = groupByChannels.length;
        for (AggregateCompilation compilation : compilations) {
            int derivedStart = currentDerivedChannel;
            for (CompiledExpression derivedExpression : compilation.preProjection()) {
                preProjections.add(new Projection.Gpu(derivedExpression));
            }
            currentDerivedChannel += compilation.preProjection().size();

            int aggChannel = currentAggChannel;
            int slotCount = compilation.aggregates().size();
            for (int slot = 0; slot < slotCount; slot++) {
                // For simple compilations (empty preProjection) the factory ignores the channel
                // and returns the pre-bound aggregate; otherwise it wires up the appended
                // pre-projection column.
                aggregates.add(compilation.aggregates().get(slot).build(derivedStart + slot));
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
        if (currentDerivedChannel != sourceColumnCount) {
            stages.add(new GpuProject.Factory(preProjections.build()));
        }
        stages.add(new GpuAggregation.Factory(
                aggregates.build(),
                groupByChannels,
                groupByTypes,
                step.isInputRaw(),
                compactionThresholdBytes,
                currentDerivedChannel));
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
                            return Optional.of(new AggregateCompilation(
                                    outputType,
                                    ImmutableList.of(maskExpression(mask, mask)),
                                    ImmutableList.of(channel -> new GpuCountNonNull(channel, outputType, dType)),
                                    Optional.empty()));
                        }
                        return Optional.of(AggregateCompilation.simple(outputType, new GpuCountAll(outputType, dType)));
                    }
                    return getSingleColumnReference(arguments, sourceLayout)
                            .filter(column -> isConvertible(column.type()))
                            .map(column -> {
                                if (maskChannel.isPresent()) {
                                    return new AggregateCompilation(
                                            outputType,
                                            ImmutableList.of(maskExpression(maskChannel.getAsInt(), column.channel())),
                                            ImmutableList.of(channel -> new GpuCountNonNull(channel, outputType, dType)),
                                            Optional.empty());
                                }
                                return AggregateCompilation.simple(outputType, new GpuCountNonNull(column.channel(), outputType, dType));
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
                if (maskChannel.isPresent()) {
                    return new AggregateCompilation(
                            outputType,
                            ImmutableList.of(maskExpression(maskChannel.getAsInt(), column.get().channel())),
                            ImmutableList.of(channel -> new GpuSum(channel, outputType, dType)),
                            Optional.empty());
                }
                return AggregateCompilation.simple(outputType, new GpuSum(column.get().channel(), outputType, dType));
            });

            case RealType _, DoubleType _ -> {
                verify(outputType == REAL || outputType == DOUBLE, "Unexpected outputType for sum with argument type %s: %s", argumentType, outputType);
                verify(!step.isOutputPartial() || outputType == DOUBLE, "Unexpected outputType for sum with argument type %s at step %s: %s", argumentType, step, outputType);

                List<Integer> inputChannels = newArrayList(column.get().channel());
                GpuExpression input = new GetColumn(0);
                if (maskChannel.isPresent()) {
                    inputChannels.add(maskChannel.getAsInt());
                    input = new Mask(new GetColumn(1), input);
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
                        ImmutableList.of(new CompiledExpression(input, new InputChannels(inputChannels))),
                        ImmutableList.of(channel -> new GpuSum(channel, outputType, FLOAT64)),
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
        CompiledExpression cast = new CompiledExpression(
                new GpuCast(new GetOnlyColumn(), decimal128Type),
                new InputChannels(ImmutableList.of(column.channel())));
        CompiledExpression passthrough = new CompiledExpression(
                new GetOnlyColumn(),
                new InputChannels(ImmutableList.of(column.channel())));

        Type sumOutputType = decimalSumOutputType(inputDecimalType);

        return Optional.of(new AggregateCompilation(
                VARBINARY,
                ImmutableList.of(cast, passthrough),
                ImmutableList.of(
                        channel -> new GpuSum(channel, sumOutputType, decimal128Type),
                        channel -> new GpuCountNonNull(channel, BigintType.BIGINT, DType.INT64)),
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
                        if (maskChannel.isPresent()) {
                            return new AggregateCompilation(
                                    returnType,
                                    ImmutableList.of(maskExpression(maskChannel.getAsInt(), column.channel())),
                                    ImmutableList.of(channel -> factory.create(channel, returnType, dType)),
                                    Optional.empty());
                        }
                        return AggregateCompilation.simple(returnType, factory.create(column.channel(), returnType, dType));
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
                            if (maskChannel.isPresent()) {
                                return new AggregateCompilation(
                                        returnType,
                                        ImmutableList.of(maskExpression(maskChannel.getAsInt(), column.channel())),
                                        ImmutableList.of(channel -> new GpuAnyValue(channel, returnType, dType)),
                                        Optional.empty());
                            }
                            return AggregateCompilation.simple(returnType, new GpuAnyValue(column.channel(), returnType, dType));
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
     * into the optional pre-projection / aggregation / post-projection pipeline.
     *
     * @param outputType the Trino type the aggregate's output column carries (the AggregationNode's
     *         symbol type for this aggregate — the *intermediate* type for PARTIAL, or
     *         the return type for SINGLE/FINAL)
     * @param preProjection compiled expressions appended to the pre-projection stage as derived
     *         input columns (e.g. the four chunks for long-decimal sum). Empty for
     *         simple aggregates that read the source column directly.
     * @param aggregates one or more aggregate slot factories — multiple for chunked aggregations
     *         like long-decimal sum (4 INT64 sums of UINT32/INT32 chunks). Each factory
     *         receives the resolved input channel (the appended derived column when
     *         {@code preProjection} is non-empty; the source channel otherwise) and
     *         returns the configured {@link GpuAggregateFunction}.
     * @param postProjection optional reshape from N slot result columns back to one output column
     *         — e.g. reassembling four chunk sums into a 16-byte LIST&lt;INT8&gt;.
     */
    private record AggregateCompilation(
            Type outputType,
            List<CompiledExpression> preProjection,
            List<AggregateSlotFactory> aggregates,
            Optional<PostProjection> postProjection)
    {
        AggregateCompilation
        {
            requireNonNull(outputType, "outputType is null");
            preProjection = List.copyOf(preProjection);
            aggregates = List.copyOf(aggregates);
            requireNonNull(postProjection, "postProjection is null");
            verify(!aggregates.isEmpty(), "aggregates is empty");
            // The pipeline builder routes aggregate i's input channel to pre-projection column i
            // (see GpuAggregationCompiler#buildPipeline), so a non-empty pre-projection must have
            // one derived column per aggregate slot.
            verify(preProjection.isEmpty() || preProjection.size() == aggregates.size(),
                    "preProjection size %s must match aggregates size %s when non-empty",
                    preProjection.size(),
                    aggregates.size());
            // The pipeline builder pass-throughs the single aggregate slot when no post-projection
            // is provided, so multi-slot compilations must declare one.
            verify(postProjection.isPresent() || aggregates.size() == 1,
                    "Aggregate without post-projection must have exactly one slot, got %s",
                    aggregates.size());
        }

        static AggregateCompilation simple(Type outputType, GpuAggregateFunction aggregate)
        {
            return new AggregateCompilation(outputType, List.of(), List.of(_ -> aggregate), Optional.empty());
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
            CompiledExpression cast = new CompiledExpression(
                    new GpuCast(new GetOnlyColumn(), decimal128Type),
                    new InputChannels(List.of(sourceChannel)));
            Type sumOutputType = decimalSumOutputType(inputDecimalType);
            return new AggregateCompilation(
                    VARBINARY,
                    List.of(cast),
                    List.of(channel -> new GpuSum(channel, sumOutputType, decimal128Type)),
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuDecimal128AsVarbinary(),
                            new InputChannels(List.of(channels[0]))))));
        }

        static AggregateCompilation longDecimalSumPartial(int sourceChannel, DType decimal128Type)
        {
            // Pre-projection: extract the four 32-bit chunks of the DECIMAL128 input. Chunks
            // 0..2 are unsigned, chunk 3 carries the sign of the value.
            List<CompiledExpression> chunks = List.of(
                    chunkExpression(sourceChannel, 0, DType.UINT32),
                    chunkExpression(sourceChannel, 1, DType.UINT32),
                    chunkExpression(sourceChannel, 2, DType.UINT32),
                    chunkExpression(sourceChannel, 3, DType.INT32));

            // Aggregation: 4 INT64 sums (BIGINT in Trino terms). cuDF's grouped SUM on UINT32/INT32
            // widens to INT64; combineInt64SumChunks accepts any 8-byte type for chunks 0..2 and
            // requires INT64 for chunk 3, so INT64 across the board satisfies it. The slot result
            // columns are consumed by the post-projection and never copied to a Trino block, so the
            // Trino type only has to match the cuDF dtype.
            AggregateSlotFactory chunkSum = channel -> new GpuSum(channel, BigintType.BIGINT, DType.INT64);
            List<AggregateSlotFactory> sums = List.of(chunkSum, chunkSum, chunkSum, chunkSum);

            return new AggregateCompilation(
                    VARBINARY,
                    chunks,
                    sums,
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuCombineSumChunksToVarbinary(decimal128Type),
                            new InputChannels(List.of(channels[0], channels[1], channels[2], channels[3]))))));
        }

        private static CompiledExpression chunkExpression(int sourceChannel, int chunkIdx, DType chunkType)
        {
            return new CompiledExpression(
                    new GpuExtractInt32Chunk(chunkIdx, chunkType),
                    new InputChannels(List.of(sourceChannel)));
        }

        static AggregateCompilation decimalSumFinal(int sourceChannel, DType decimal128Type, Type outputType)
        {
            // Pre-projection: unpack the VARBINARY intermediate state of sum(decimal) into 5 fixed-
            // width components — 4 INT32 chunks of the 128-bit running sum, plus the running
            // overflow long. Variable-length input (8/16/24 byte rows from CPU PARTIAL or uniform
            // 16-byte from GPU PARTIAL) is handled inside GpuExtractDecimalStateChunk.
            List<CompiledExpression> components = List.of(
                    decimalStateComponentExpression(sourceChannel, 0),
                    decimalStateComponentExpression(sourceChannel, 1),
                    decimalStateComponentExpression(sourceChannel, 2),
                    decimalStateComponentExpression(sourceChannel, 3),
                    decimalStateComponentExpression(sourceChannel, 4));

            // Aggregation: 5 INT64 sums, mirroring longDecimalSumPartial — cuDF SUM widens
            // UINT32/INT32 inputs to INT64 with the right (zero/sign) extension. The 5th sum
            // accumulates the per-state overflow long.
            AggregateSlotFactory chunkSum = channel -> new GpuSum(channel, BigintType.BIGINT, DType.INT64);
            List<AggregateSlotFactory> sums = List.of(chunkSum, chunkSum, chunkSum, chunkSum, chunkSum);

            return new AggregateCompilation(
                    outputType,
                    components,
                    sums,
                    Optional.of(new PostProjection(channels -> new CompiledExpression(
                            new GpuCombineDecimalStateSumsToDecimal128(decimal128Type),
                            new InputChannels(List.of(channels[0], channels[1], channels[2], channels[3], channels[4]))))));
        }

        private static CompiledExpression decimalStateComponentExpression(int sourceChannel, int componentIdx)
        {
            return new CompiledExpression(
                    new GpuExtractDecimalStateChunk(componentIdx),
                    new InputChannels(List.of(sourceChannel)));
        }
    }

    /**
     * Builds a single aggregate slot once its input channel is known. For simple aggregates the
     * channel is encoded at compile time and the factory ignores its argument; for chunked-sum
     * aggregates the factory wires the supplied pre-projection channel into a fresh
     * {@link GpuAggregateFunction}.
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

    private static CompiledExpression maskExpression(int maskChannel, int valueChannel)
    {
        return new CompiledExpression(
                new Mask(new GetColumn(0), new GetColumn(1)),
                new InputChannels(ImmutableList.of(maskChannel, valueChannel)));
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
