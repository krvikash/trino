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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.isRewriteSumWithLiteralEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * Rule that rewrites {@code sum(column ± literal)} into
 * {@code sum(column) ± literal * count(column)}, moving the literal shift out
 * of the per-row aggregation.
 *
 * <p>Transforms, shown for a narrow column {@code s : SMALLINT}:
 * <pre> {@code
 * - Aggregation[key]: out = sum(aggr)
 *   - Project:
 *       aggr = CAST(CAST(s AS INTEGER) + 1 AS BIGINT)
 *     - source
 * }</pre>
 * into:
 * <pre> {@code
 * - Project:
 *       out = sum_s + CAST(1 AS BIGINT) * count_s
 *   - Aggregation[key]: sum_s = sum(cast_s), count_s = count(s)
 *     - Project:
 *         cast_s = CAST(s AS BIGINT)
 *         s
 *       - source
 * }</pre>
 *
 * <p>The rule applies only when the column is TINYINT or SMALLINT and the
 * inner arithmetic runs in INTEGER or BIGINT (the shape Trino's coercion
 * pipeline emits for a narrow column combined with an INTEGER or BIGINT
 * literal). Under these constraints {@code sum(col)} cannot overflow BIGINT
 * for any realistic row count (&gt; 2.8×10^14 rows for SMALLINT,
 * &gt; 7×10^16 for TINYINT), and the rewritten aggregation produces the
 * same value as the original. The rewrite may succeed where the original
 * would have overflowed per-row in the narrower arithmetic type; accepted
 * as benign.
 *
 * <p>BIGINT and INTEGER columns are excluded because {@code sum(col)} over
 * a wider column type can overflow where the original's per-row-shifted
 * accumulator (which keeps intermediate values smaller) would have stayed
 * in range. Reliable per-column min/max statistics would let us prove
 * safety for those types and extend coverage — see TODO in
 * {@code tryMatch}.
 *
 * <p>This rule was introduced for ClickBench Q29, which issues 90
 * {@code SUM(ResolutionWidth + k)} clauses over a single SMALLINT column.
 *
 * <p>Multiple matches on the same column collapse to a single
 * {@code sum(column)} + {@code count(column)} pair: the rewrite deduplicates
 * the emitted aggregates (and any coercion-cast input) across matches so the
 * rewritten aggregation stays small regardless of how many shifted-sum
 * clauses fed into it.
 */
public class RewriteSumWithLiteralAsSumAndCount
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName SUM_NAME = builtinFunctionName("sum");
    private static final CatalogSchemaFunctionName ADD_NAME = builtinFunctionName(OperatorType.ADD);
    private static final CatalogSchemaFunctionName SUBTRACT_NAME = builtinFunctionName(OperatorType.SUBTRACT);

    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(SINGLE))
            .with(source().matching(project().capturedAs(CHILD)));

    private final PlannerContext plannerContext;

    public RewriteSumWithLiteralAsSumAndCount(PlannerContext plannerContext)
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
        return isRewriteSumWithLiteralEnabled(session);
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        ProjectNode childProject = captures.get(CHILD);

        Map<Symbol, Match> matchByOutputSymbol = new LinkedHashMap<>();
        for (Entry<Symbol, Aggregation> entry : parent.getAggregations().entrySet()) {
            tryMatch(entry.getValue(), childProject.getAssignments())
                    .ifPresent(match -> matchByOutputSymbol.put(entry.getKey(), match));
        }

        // Only rewrite when at least one column has multiple matches. For a
        // single match the extra count aggregate costs more than the per-row
        // add it saves (empirically verified on ClickBench hits, single-match
        // case is a net slowdown).
        Map<Symbol, Long> matchesPerColumn = matchByOutputSymbol.values().stream()
                .collect(groupingBy(Match::column, counting()));
        matchByOutputSymbol.values().removeIf(match -> matchesPerColumn.get(match.column()) < 2);

        if (matchByOutputSymbol.isEmpty()) {
            return Result.empty();
        }

        // count(column) reads the original column directly; ensure it is exposed
        // by the child project (which may only have emitted the now-obsolete
        // bound arithmetic expression).
        Set<Symbol> childOutputs = childProject.getAssignments().outputs();
        Assignments.Builder newChildAssignments = Assignments.builder()
                .putAll(childProject.getAssignments());
        for (Match match : matchByOutputSymbol.values()) {
            if (!childOutputs.contains(match.column())) {
                newChildAssignments.putIdentity(match.column());
            }
        }

        AggregateAllocator allocator = new AggregateAllocator(context, newChildAssignments);
        Assignments.Builder outerAssignments = Assignments.builder()
                .putIdentities(parent.getGroupingKeys());
        for (Entry<Symbol, Aggregation> entry : parent.getAggregations().entrySet()) {
            Symbol outputSymbol = entry.getKey();
            Aggregation aggregation = entry.getValue();
            Match match = matchByOutputSymbol.get(outputSymbol);
            if (match == null) {
                allocator.addUnchanged(outputSymbol, aggregation);
                outerAssignments.putIdentity(outputSymbol);
                continue;
            }
            Symbol sumInput = allocator.sumInputFor(match);
            Symbol sumSymbol = allocator.sumAggregateFor(sumInput);
            Symbol countSymbol = allocator.countAggregateFor(match.column());
            outerAssignments.put(outputSymbol, buildRewrittenExpression(sumSymbol, countSymbol, match));
        }

        ProjectNode newChildProject = new ProjectNode(
                context.getIdAllocator().getNextId(),
                childProject.getSource(),
                newChildAssignments.build());

        AggregationNode newAggregation = AggregationNode.builderFrom(parent)
                .setSource(newChildProject)
                .setAggregations(allocator.aggregations())
                .setPreGroupedSymbols(ImmutableList.of())
                .build();

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregation,
                outerAssignments.build()));
    }

    private Expression buildRewrittenExpression(Symbol sumSymbol, Symbol countSymbol, Match match)
    {
        Expression literal = match.literal().type().equals(BIGINT)
                ? match.literal()
                : new Cast(match.literal(), BIGINT);
        ResolvedFunction multiply = plannerContext.getMetadata().resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
        Expression literalTimesCount = new Call(multiply, ImmutableList.of(literal, countSymbol.toSymbolReference()));
        ResolvedFunction outerOperator = plannerContext.getMetadata().resolveOperator(
                match.isSubtract() ? OperatorType.SUBTRACT : OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
        Expression sumReference = sumSymbol.toSymbolReference();
        return match.columnOnLeft()
                ? new Call(outerOperator, ImmutableList.of(sumReference, literalTimesCount))
                : new Call(outerOperator, ImmutableList.of(literalTimesCount, sumReference));
    }

    /**
     * Inspects a single aggregate entry together with the assignments of its
     * child Project. Returns a {@link Match} describing the matched column,
     * literal, operator, and orientation when the shape fits one of the
     * accepted forms; otherwise {@link Optional#empty()}.
     */
    static Optional<Match> tryMatch(Aggregation aggregation, Assignments sourceAssignments)
    {
        if (!aggregation.getResolvedFunction().signature().getName().equals(SUM_NAME)) {
            return Optional.empty();
        }
        if (aggregation.isDistinct()) {
            return Optional.empty();
        }
        if (aggregation.getFilter().isPresent()) {
            return Optional.empty();
        }
        if (aggregation.getOrderingScheme().isPresent()) {
            return Optional.empty();
        }
        if (aggregation.getMask().isPresent()) {
            return Optional.empty();
        }
        if (aggregation.getArguments().size() != 1) {
            return Optional.empty();
        }
        if (!(aggregation.getArguments().get(0) instanceof Reference reference)) {
            return Optional.empty();
        }

        Expression boundExpression = sourceAssignments.get(Symbol.from(reference));
        if (boundExpression == null) {
            return Optional.empty();
        }
        // Require the integer-sum path: sum(bigint) with a bigint-typed input.
        // Non-integer sums (double, decimal, real) flow through a different
        // signature and aren't covered by the safety argument below.
        if (!boundExpression.type().equals(BIGINT)) {
            return Optional.empty();
        }
        if (boundExpression instanceof Cast cast) {
            boundExpression = cast.expression();
        }
        if (!(boundExpression instanceof Call call)) {
            return Optional.empty();
        }
        // Require the inner arithmetic to run in INTEGER or BIGINT (never in a
        // narrower type), so per-row evaluation cannot throw
        // NUMERIC_VALUE_OUT_OF_RANGE in a type where the rewrite would
        // silently succeed.
        Type callType = call.type();
        if (!(callType instanceof IntegerType || callType instanceof BigintType)) {
            return Optional.empty();
        }

        boolean isSubtract;
        CatalogSchemaFunctionName functionName = call.function().name();
        if (functionName.equals(ADD_NAME)) {
            isSubtract = false;
        }
        else if (functionName.equals(SUBTRACT_NAME)) {
            isSubtract = true;
        }
        else {
            return Optional.empty();
        }

        if (call.arguments().size() != 2) {
            return Optional.empty();
        }
        Expression leftArgument = call.arguments().get(0);
        Expression rightArgument = call.arguments().get(1);

        Symbol column;
        Constant literal;
        boolean columnOnLeft;
        Optional<Symbol> leftColumn = columnFromOperand(leftArgument);
        Optional<Symbol> rightColumn = columnFromOperand(rightArgument);
        if (leftColumn.isPresent() && rightArgument instanceof Constant rightLiteral) {
            column = leftColumn.get();
            literal = rightLiteral;
            columnOnLeft = true;
        }
        else if (leftArgument instanceof Constant leftLiteral && rightColumn.isPresent()) {
            column = rightColumn.get();
            literal = leftLiteral;
            columnOnLeft = false;
        }
        else {
            return Optional.empty();
        }

        if (literal.value() == null) {
            return Optional.empty();
        }
        // Restrict to TINYINT/SMALLINT columns: the type's value range bounds
        // |sum(col)| by type_max × N, so the rewrite's running accumulator
        // cannot overflow BIGINT for any realistic row count, which is what
        // makes the rewrite strictly behaviour-preserving (see class javadoc).
        //
        // TODO: extend to INTEGER and BIGINT columns when reliable per-column
        // min/max statistics are available — they would let us prove that
        // sum(col) stays within BIGINT range for a given query, lifting the
        // type-only bound used here.
        if (!(column.type() instanceof TinyintType || column.type() instanceof SmallintType)) {
            return Optional.empty();
        }

        return Optional.of(new Match(column, literal, columnOnLeft, isSubtract));
    }

    /**
     * The column operand of the arithmetic can be either a plain
     * {@link Reference} or a {@code Reference} wrapped in a widening cast within
     * the integer family (e.g. {@code CAST(smallint_col AS integer)}, inserted
     * by analyzer coercion when the {@code +} operator requires the column to
     * match the literal's type). The widening is lossless, so the underlying
     * column symbol can be carried through into the rewritten aggregate.
     */
    private static Optional<Symbol> columnFromOperand(Expression expression)
    {
        if (expression instanceof Reference reference) {
            return Optional.of(Symbol.from(reference));
        }
        if (expression instanceof Cast cast
                && cast.expression() instanceof Reference reference
                && isLosslessIntegerWidening(reference.type(), cast.type())) {
            return Optional.of(Symbol.from(reference));
        }
        return Optional.empty();
    }

    private static boolean isLosslessIntegerWidening(Type source, Type target)
    {
        if (source instanceof TinyintType) {
            return target instanceof SmallintType || target instanceof IntegerType || target instanceof BigintType;
        }
        if (source instanceof SmallintType) {
            return target instanceof IntegerType || target instanceof BigintType;
        }
        if (source instanceof IntegerType) {
            return target instanceof BigintType;
        }
        return false;
    }

    record Match(Symbol column, Constant literal, boolean columnOnLeft, boolean isSubtract) {}

    /**
     * Collects the new aggregate list while sharing cast inputs, sum aggregate
     * outputs, and count aggregate outputs across matches that target the same
     * column. With many {@code sum(col + k_i)} clauses over the same column
     * this collapses the rewritten aggregation into one {@code sum(col)} +
     * one {@code count(col)} instead of one pair per clause.
     */
    private final class AggregateAllocator
    {
        private final Context context;
        private final Assignments.Builder childAssignments;
        private final Map<Symbol, Aggregation> newAggregations = new LinkedHashMap<>();
        private final Map<Symbol, Symbol> sumInputs = new HashMap<>();
        private final Map<Symbol, Symbol> sumAggregates = new HashMap<>();
        private final Map<Symbol, Symbol> countAggregates = new HashMap<>();

        AggregateAllocator(Context context, Assignments.Builder childAssignments)
        {
            this.context = context;
            this.childAssignments = childAssignments;
        }

        Symbol sumInputFor(Match match)
        {
            return sumInputs.computeIfAbsent(match.column(), column -> {
                Symbol cast = context.getSymbolAllocator().newSymbol(column.name(), BIGINT);
                childAssignments.put(cast, new Cast(column.toSymbolReference(), BIGINT));
                return cast;
            });
        }

        Symbol sumAggregateFor(Symbol sumInput)
        {
            return sumAggregates.computeIfAbsent(sumInput, input -> {
                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("sum", TypeSignatureProvider.fromTypes(input.type()));
                Symbol symbol = context.getSymbolAllocator().newSymbol("sum", function.signature().getReturnType());
                newAggregations.put(symbol, new Aggregation(
                        function,
                        ImmutableList.of(input.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
                return symbol;
            });
        }

        Symbol countAggregateFor(Symbol column)
        {
            return countAggregates.computeIfAbsent(column, col -> {
                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("count", TypeSignatureProvider.fromTypes(col.type()));
                Symbol symbol = context.getSymbolAllocator().newSymbol("count", function.signature().getReturnType());
                newAggregations.put(symbol, new Aggregation(
                        function,
                        ImmutableList.of(col.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
                return symbol;
            });
        }

        void addUnchanged(Symbol outputSymbol, Aggregation aggregation)
        {
            newAggregations.put(outputSymbol, aggregation);
        }

        Map<Symbol, Aggregation> aggregations()
        {
            return newAggregations;
        }
    }
}
