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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cache.NonEvictableCache;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.Accumulator;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregationMask;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.isPushAggregationIntoValuesEnabled;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.values;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates a single global aggregation over a ValuesNode at planning time
 * and replaces the subtree with a constant ValuesNode containing the computed results.
 * <p>
 * Transforms:
 * <pre>{@code
 * - Aggregation (global, SINGLE)
 *     agg_result <- agg(x) [mask(m)]
 *   - Values (constant rows)
 * }</pre>
 * Into:
 * <pre>{@code
 * - Values (single row with computed aggregation results)
 * }</pre>
 */
public class PushAggregationIntoValues
        implements Rule<AggregationNode>
{
    private static final Capture<ValuesNode> VALUES = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushAggregationIntoValues::isSupportedAggregation)
            .with(source().matching(values()
                    .matching(PushAggregationIntoValues::isSupportedValues)
                    .capturedAs(VALUES)));

    private final PlannerContext plannerContext;
    private final NonEvictableCache<AccumulatorFactoryKey, AccumulatorFactory> accumulatorFactoryCache;

    public PushAggregationIntoValues(PlannerContext plannerContext, NonEvictableCache<AccumulatorFactoryKey, AccumulatorFactory> accumulatorFactoryCache)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accumulatorFactoryCache = requireNonNull(accumulatorFactoryCache, "accumulatorFactoryCache is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationIntoValuesEnabled(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ValuesNode valuesNode = captures.get(VALUES);
        List<Symbol> valuesOutputSymbols = valuesNode.getOutputSymbols();
        List<Expression> rows = valuesNode.getRows().orElse(ImmutableList.of());

        // Reject non-deterministic rows or rows with unresolved correlations
        if (!rows.isEmpty()) {
            if (rows.stream().anyMatch(row -> !isDeterministic(row))) {
                return Result.empty();
            }
            if (!extractUnique(rows).isEmpty()) {
                return Result.empty();
            }
        }

        // Validate mask symbols reference ValuesNode outputs
        for (Aggregation agg : node.getAggregations().values()) {
            if (agg.getMask().isPresent() && !valuesOutputSymbols.contains(agg.getMask().get())) {
                return Result.empty();
            }
        }

        ImmutableList.Builder<Expression> resultItems = ImmutableList.builder();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            Aggregation agg = node.getAggregations().get(outputSymbol);
            resultItems.add(evaluateAggregation(agg, valuesOutputSymbols, rows));
        }

        return Result.ofPlanNode(new ValuesNode(
                node.getId(),
                node.getOutputSymbols(),
                ImmutableList.of(new Row(resultItems.build()))));
    }

    private Constant evaluateAggregation(Aggregation agg, List<Symbol> valuesOutputSymbols, List<Expression> rows)
    {
        ResolvedFunction resolvedFunction = agg.getResolvedFunction();

        AccumulatorFactory factory = uncheckedCacheGet(
                accumulatorFactoryCache,
                new AccumulatorFactoryKey(resolvedFunction.functionId(), resolvedFunction.signature()),
                () -> {
                    AggregationImplementation implementation = plannerContext.getFunctionManager()
                            .getAggregationImplementation(resolvedFunction);
                    return generateAccumulatorFactory(
                            resolvedFunction.signature(),
                            implementation,
                            resolvedFunction.functionNullability(),
                            false);
                });

        Accumulator accumulator = factory.createAccumulator(ImmutableList.of());

        if (!rows.isEmpty()) {
            Page page = buildPage(agg, valuesOutputSymbols, rows);
            Optional<Block> maskBlock = buildMaskBlock(agg, valuesOutputSymbols, rows);
            AggregationMask mask = factory.createAggregationMaskBuilder().buildAggregationMask(page, maskBlock);
            accumulator.addInput(page, mask);
        }

        Type resultType = resolvedFunction.signature().getReturnType();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, 1);
        accumulator.evaluateFinal(resultBuilder);
        Block resultBlock = resultBuilder.build();
        return new Constant(resultType, readNativeValue(resultType, resultBlock, 0));
    }

    private static Page buildPage(Aggregation agg, List<Symbol> valuesOutputSymbols, List<Expression> rows)
    {
        List<Expression> arguments = agg.getArguments();
        if (arguments.isEmpty()) {
            return new Page(rows.size());
        }

        List<Type> argumentTypes = agg.getResolvedFunction().signature().getArgumentTypes();
        Block[] blocks = new Block[arguments.size()];
        for (int col = 0; col < arguments.size(); col++) {
            int columnIndex = valuesOutputSymbols.indexOf(Symbol.from(arguments.get(col)));
            blocks[col] = buildColumn(argumentTypes.get(col), columnIndex, rows);
        }

        return new Page(rows.size(), blocks);
    }

    private static Optional<Block> buildMaskBlock(Aggregation agg, List<Symbol> valuesOutputSymbols, List<Expression> rows)
    {
        if (agg.getMask().isEmpty()) {
            return Optional.empty();
        }
        int maskColumnIndex = valuesOutputSymbols.indexOf(agg.getMask().get());
        return Optional.of(buildColumn(BOOLEAN, maskColumnIndex, rows));
    }

    private static Block buildColumn(Type type, int columnIndex, List<Expression> rows)
    {
        BlockBuilder builder = type.createBlockBuilder(null, rows.size());
        for (Expression row : rows) {
            writeNativeValue(type, builder, extractValue(row, columnIndex));
        }
        return builder.build();
    }

    private static Object extractValue(Expression rowExpression, int columnIndex)
    {
        verify(rowExpression instanceof Row, "Unexpected expression type in ValuesNode: %s", rowExpression.getClass().getSimpleName());
        Row row = (Row) rowExpression;
        Expression item = row.items().get(columnIndex);
        verify(item instanceof Constant, "Expected Constant in ValuesNode row, got: %s", item.getClass().getSimpleName());
        return ((Constant) item).value();
    }

    private static boolean isSupportedAggregation(AggregationNode node)
    {
        if (node.getStep() != AggregationNode.Step.SINGLE) {
            return false;
        }
        if (!node.hasSingleGlobalAggregation()) {
            return false;
        }
        if (node.getGroupIdSymbol().isPresent()) {
            return false;
        }
        for (Aggregation agg : node.getAggregations().values()) {
            if (agg.isDistinct() ||
                    agg.getOrderingScheme().isPresent() ||
                    agg.getFilter().isPresent() ||
                    agg.getArguments().stream().anyMatch(Lambda.class::isInstance)) {
                return false;
            }
        }
        return true;
    }

    // Do not optimize if any row is not a Row instance, because we cannot
    // decompose non-Row expressions into per-column constants.
    private static boolean isSupportedValues(ValuesNode valuesNode)
    {
        return valuesNode.getRows().isPresent() &&
                valuesNode.getRows().get().stream().allMatch(Row.class::isInstance);
    }

    public record AccumulatorFactoryKey(FunctionId functionId, BoundSignature signature) {}
}
