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

import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static java.util.Objects.requireNonNull;

public final class GpuAggregationCompiler
{
    private GpuAggregationCompiler() {}

    private static final Logger log = Logger.get(GpuAggregationCompiler.class);

    public static Optional<GpuAggregation.Factory> compile(AggregationNode node, Map<Symbol, Integer> sourceLayout)
    {
        Step step = node.getStep();

        if (node.getGroupingSetCount() > 1) {
            log.debug("Could not compile aggregation with %s grouping sets", node.getGroupingSetCount());
            // GROUPING SETS are not supported yet
            return Optional.empty();
        }

        List<Symbol> groupingKeys = node.getGroupingKeys();
        ImmutableList.Builder<Type> groupByTypesBuilder = ImmutableList.builder();
        int[] groupByChannels = new int[groupingKeys.size()];

        for (int i = 0; i < groupingKeys.size(); i++) {
            Symbol symbol = groupingKeys.get(i);
            Type type = symbol.type();
            if (!isConvertible(type)) {
                return Optional.empty();
            }
            groupByTypesBuilder.add(type);
            groupByChannels[i] = verifyNotNull(sourceLayout.get(symbol), "channel for symbol %s is not in source layout", symbol);
        }

        ImmutableList.Builder<GpuAggregateFunction> aggregates = ImmutableList.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Symbol outputSymbol = entry.getKey();
            Aggregation aggregation = entry.getValue();
            Optional<GpuAggregateFunction> compiled = compileAggregation(outputSymbol, aggregation, sourceLayout);
            if (compiled.isEmpty()) {
                log.debug(
                        "Could not compile aggregation function %s with filter=%s mask=%s distinct=%s ordered=%s",
                        aggregation.getResolvedFunction().signature(),
                        aggregation.getFilter().isPresent(),
                        aggregation.getMask().isPresent(),
                        aggregation.isDistinct(),
                        aggregation.getOrderingScheme().isPresent());
                return Optional.empty();
            }
            compiled.ifPresent(aggregateFunction -> verify(
                    aggregateFunction.outputType().equals(outputSymbol.type()),
                    "Expected compiled %s %s aggregate to produce %s but got %s",
                    aggregation.getResolvedFunction().name(),
                    step,
                    outputSymbol.type(),
                    aggregateFunction.outputType()));
            aggregates.add(compiled.get());
        }

        return Optional.of(new GpuAggregation.Factory(
                aggregates.build(),
                groupByChannels,
                groupByTypesBuilder.build(),
                step.isInputRaw()));
    }

    private static Optional<GpuAggregateFunction> compileAggregation(Symbol outputSymbol, Aggregation aggregation, Map<Symbol, Integer> sourceLayout)
    {
        BoundSignature signature = aggregation.getResolvedFunction().signature();
        if (!isBuiltinFunctionName(signature.getName())) {
            return Optional.empty();
        }
        String name = signature.getName().functionName();

        if (aggregation.isDistinct() || aggregation.getFilter().isPresent() || aggregation.getOrderingScheme().isPresent() || aggregation.getMask().isPresent()) {
            // No DISTINCT, FILTER, ORDER BY, or MASK support yet
            return Optional.empty();
        }

        List<Expression> arguments = aggregation.getArguments();
        Type outputType = outputSymbol.type();

        return switch (name) {
            // For count, all steps produce the same type
            case "count" -> compileCount(arguments, sourceLayout, outputType);
            // For currently supported sum, all steps produce the same type
            case "sum" -> compileSum(arguments, sourceLayout, outputType);
            // For min and max, all steps produce the same type
            case "min" -> compileMinMax(arguments, sourceLayout, outputType, GpuMin::new);
            case "max" -> compileMinMax(arguments, sourceLayout, outputType, GpuMax::new);
            default -> Optional.empty();
        };
    }

    private static Optional<GpuAggregateFunction> compileCount(List<Expression> arguments, Map<Symbol, Integer> sourceLayout, Type outputType)
    {
        return toDType(outputType)
                .flatMap(dType -> {
                    if (arguments.isEmpty()) {
                        // COUNT(*) - count all rows
                        return Optional.of(new GpuCountAll(outputType, dType));
                    }
                    // COUNT(column) - count non-nulls
                    return getSingleColumnReference(arguments, sourceLayout)
                            .filter(column -> isConvertible(column.type()))
                            .map(column -> new GpuCountNonNull(column.channel(), outputType, dType));
                });
    }

    private static Optional<GpuAggregateFunction> compileSum(List<Expression> arguments, Map<Symbol, Integer> sourceLayout, Type outputType)
    {
        Optional<ColumnReference> column = getSingleColumnReference(arguments, sourceLayout);
        if (column.isEmpty()) {
            return Optional.empty();
        }

        Type argumentType = column.get().type();
        return switch (argumentType) {
            // For bigint, double and real, argument type == intermediate type == return type, so all 4 Steps share the same shape
            // cuDF SUM yields the correct total whether the rows are raw values (PARTIAL/SINGLE) or already-summed partials (INTERMEDIATE/FINAL).
            case BigintType _, DoubleType _, RealType _ -> toDType(argumentType).map(dType -> new GpuSum(column.get().channel(), outputType, dType));
            default -> Optional.empty();
        };
    }

    private static Optional<GpuAggregateFunction> compileMinMax(List<Expression> arguments, Map<Symbol, Integer> sourceLayout, Type outputType, MinMaxFactory factory)
    {
        return getSingleColumnReference(arguments, sourceLayout)
                .flatMap(column -> toDType(outputType)
                        .map(dType -> factory.create(column.channel(), outputType, dType)));
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
}
