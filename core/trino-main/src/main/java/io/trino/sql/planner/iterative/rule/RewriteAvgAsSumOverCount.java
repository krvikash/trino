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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.isGpuExecutionEnabled;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites {@code avg(x)} into {@code sum(CAST(x AS double)) / CAST(count(x) AS double)}
 * so that the GPU execution path can handle avg using the already-supported sum and count
 * aggregates.
 * <p>
 * The input is cast to double before summing to match the semantics of the built-in avg,
 * which accumulates the sum as a double to avoid integer overflow.
 * <p>
 * Only rewrites avg over {@code BIGINT}, {@code DOUBLE}, and {@code REAL} inputs — the types
 * where casting to double is semantically equivalent to what the CPU-based avg does.
 * <p>
 * The rule matches SINGLE-step aggregations because all aggregations initially enter the
 * optimizer as SINGLE. After this rule replaces avg with sum and count, the subsequent
 * {@code AddExchanges} phase splits them into PARTIAL/FINAL as usual — standard primitives
 * that need no special handling. Once the rule fires, no avg aggregation remains in the plan
 * at any step.
 */
public class RewriteAvgAsSumOverCount
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName AVG_NAME = builtinFunctionName("avg");
    private static final Set<Type> SUPPORTED_INPUT_TYPES = Set.of(BIGINT, DOUBLE, REAL);

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(SINGLE));

    private final PlannerContext plannerContext;

    public RewriteAvgAsSumOverCount(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isGpuExecutionEnabled(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<Symbol, Aggregation> newAggregations = new LinkedHashMap<>(node.getAggregations());
        Assignments.Builder childProjections = Assignments.builder();
        Assignments.Builder outerProjections = Assignments.builder()
                .putIdentities(node.getGroupingKeys());
        boolean rewritten = false;

        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Symbol outputSymbol = entry.getKey();
            Aggregation aggregation = entry.getValue();

            if (!isRewritableAvg(aggregation) || !SUPPORTED_INPUT_TYPES.contains(aggregation.getArguments().getFirst().type())) {
                outerProjections.putIdentity(outputSymbol);
                continue;
            }

            Expression argument = aggregation.getArguments().getFirst();
            Type avgOutputType = aggregation.getResolvedFunction().signature().getReturnType();

            // Cast input to DOUBLE before summing to avoid integer overflow.
            // SimplifyRedundantCast will remove the cast when the input is already DOUBLE.
            Symbol castSymbol = context.getSymbolAllocator().newSymbol("avg_input", DOUBLE);
            childProjections.put(castSymbol, new Cast(argument, DOUBLE));
            Reference sumCountInput = castSymbol.toSymbolReference();

            ResolvedFunction sumFunction = plannerContext.getMetadata()
                    .resolveBuiltinFunction("sum", TypeSignatureProvider.fromTypes(DOUBLE));
            Symbol sumSymbol = context.getSymbolAllocator().newSymbol("sum", sumFunction.signature().getReturnType());
            newAggregations.put(sumSymbol, new Aggregation(
                    sumFunction,
                    ImmutableList.of(sumCountInput),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));

            ResolvedFunction countFunction = plannerContext.getMetadata()
                    .resolveBuiltinFunction("count", TypeSignatureProvider.fromTypes(DOUBLE));
            Symbol countSymbol = context.getSymbolAllocator().newSymbol("count", countFunction.signature().getReturnType());
            newAggregations.put(countSymbol, new Aggregation(
                    countFunction,
                    ImmutableList.of(sumCountInput),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));

            newAggregations.remove(outputSymbol);

            ResolvedFunction divide = plannerContext.getMetadata()
                    .resolveOperator(OperatorType.DIVIDE, ImmutableList.of(DOUBLE, DOUBLE));
            Expression divisionResult = new Call(divide, ImmutableList.of(
                    sumSymbol.toSymbolReference(),
                    new Cast(countSymbol.toSymbolReference(), DOUBLE)));
            outerProjections.put(outputSymbol, maybeCast(divisionResult, avgOutputType));

            rewritten = true;
        }

        if (!rewritten) {
            return Result.empty();
        }

        for (Symbol symbol : node.getSource().getOutputSymbols()) {
            childProjections.putIdentity(symbol);
        }
        PlanNode source = new ProjectNode(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                childProjections.build());

        AggregationNode newAggregation = AggregationNode.builderFrom(node)
                .setSource(source)
                .setAggregations(newAggregations)
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregation,
                outerProjections.build()));
    }

    private static boolean isRewritableAvg(Aggregation aggregation)
    {
        // Skip DISTINCT, FILTER, ORDER BY, and MASK — the GPU aggregation path does not support
        // them (see GpuAggregationCompiler#compileAggregation). Additionally, rewriting
        // avg(DISTINCT x) is unsafe: the BIGINT-to-DOUBLE cast is lossy for values beyond 2^53,
        // so DISTINCT would operate on fewer unique values after casting.
        return aggregation.getResolvedFunction().signature().getName().equals(AVG_NAME)
                && !aggregation.isDistinct()
                && aggregation.getFilter().isEmpty()
                && aggregation.getOrderingScheme().isEmpty()
                && aggregation.getMask().isEmpty()
                && aggregation.getArguments().size() == 1;
    }

    private static Expression maybeCast(Expression expression, Type targetType)
    {
        if (expression.type().equals(targetType)) {
            return expression;
        }
        return new Cast(expression, targetType);
    }
}
