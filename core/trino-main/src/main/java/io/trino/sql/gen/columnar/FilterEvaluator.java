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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.function.FunctionKind.BATCH;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.sql.gen.columnar.AndFilterEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.gen.columnar.DynamicPageFilter.DynamicFilterEvaluator;
import static io.trino.sql.gen.columnar.OrFilterEvaluator.createOrExpressionEvaluator;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.type.UnknownType.UNKNOWN;

/**
 * Used by PageProcessor to evaluate filter expression on input Page.
 * <p>
 * Implementations handle dictionary aware processing through {@link DictionaryAwareColumnarFilter}.
 */
public sealed interface FilterEvaluator
        permits AndFilterEvaluator,
                ColumnarFilterEvaluator,
                ColumnarFilterEvaluatorWithProjectedArguments,
                DynamicFilterEvaluator,
                OrFilterEvaluator,
                PageFilterEvaluator,
                SelectAllEvaluator,
                SelectNoneEvaluator
{
    Logger log = Logger.get(FilterEvaluator.class);

    SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page);

    record SelectionResult(SelectedPositions selectedPositions, long filterTimeNanos) {}

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterEvaluationEnabled,
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            boolean filterReorderingEnabled,
            Optional<Expression> filter,
            Map<Symbol, Integer> layout,
            ColumnarFilterCompiler columnarFilterCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            Optional<String> classNameSuffix)
    {
        if (columnarFilterEvaluationEnabled && filter.isPresent()) {
            return createColumnarFilterEvaluator(
                    columnarFilterSubexpressionEvaluationEnabled,
                    isDebugOutputEnabled,
                    filterReorderingEnabled,
                    filter.get(),
                    layout,
                    columnarFilterCompiler,
                    pageFunctionCompiler,
                    classNameSuffix);
        }
        return Optional.empty();
    }

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            boolean filterReorderingEnabled,
            Expression expression,
            Map<Symbol, Integer> layout,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Optional<String> classNameSuffix)
    {
        return switch (expression) {
            case Constant constant when constant.value() instanceof Boolean booleanValue -> booleanValue ? Optional.of(SelectAllEvaluator::new) : Optional.of(SelectNoneEvaluator::new);
            case Comparison comparison when comparison.operator() == Comparison.Operator.NOT_EQUAL -> {
                // Lower NOT_EQUAL to NOT(EQUAL) so it goes through the same Call sub-expression evaluation
                // path that handled it in the old RowExpression IR (where NOT_EQUAL was translated to
                // Call($not, Call(EQUAL, ...)) by SqlToRowExpressionTranslator).
                ResolvedFunction notFunction = compiler.getMetadata().resolveBuiltinFunction("$not", fromTypes(BOOLEAN));
                Call wrapped = new Call(notFunction, ImmutableList.of(new Comparison(Comparison.Operator.EQUAL, comparison.left(), comparison.right())));
                yield createCallExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, wrapped, layout, classNameSuffix);
            }
            case Comparison comparison -> createComparisonExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, comparison, layout, classNameSuffix);
            case Call call -> {
                if (call.function().functionKind() == BATCH) {
                    // Batch functions are not supported in columnar filter evaluation
                    yield Optional.empty();
                }
                if (isNotExpression(call) && call.arguments().getFirst() instanceof IsNull isNull) {
                    // "not(is_null(reference))" is handled explicitly as it is easy.
                    yield createIsNotNullExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, call, isNull, layout, classNameSuffix);
                }
                yield createCallExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, call, layout, classNameSuffix);
            }
            case IsNull isNull -> createIsNullExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, isNull, layout, classNameSuffix);
            case Logical logical when logical.operator() == Logical.Operator.AND -> createAndExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, filterReorderingEnabled, compiler, pageFunctionCompiler, logical, layout, classNameSuffix);
            case Logical logical when logical.operator() == Logical.Operator.OR -> createOrExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, filterReorderingEnabled, compiler, pageFunctionCompiler, logical, layout, classNameSuffix);
            case Between between -> createBetweenEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, filterReorderingEnabled, compiler, pageFunctionCompiler, between, layout, classNameSuffix);
            case In in -> createInExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, in, layout, classNameSuffix);
            default -> Optional.empty();
        };
    }

    static boolean isNotExpression(Call call)
    {
        CatalogSchemaFunctionName functionName = call.function().name();
        return isBuiltinFunctionName(functionName) && functionName.functionName().equals("$not");
    }

    // Reordering can expose a term that may fail to rows that an earlier term would have filtered out,
    // changing SQL short-circuit semantics. Only reorder when every term is guaranteed not to fail.
    static boolean isReorderingSafe(PlannerContext plannerContext, List<Expression> terms)
    {
        return terms.stream().noneMatch(term -> mayFail(plannerContext, term));
    }

    private static Optional<Supplier<FilterEvaluator>> createBetweenEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            boolean filterReorderingEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Between between,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        // When the min and max arguments of a BETWEEN expression are both constants, evaluating them inline is cheaper than AND-ing subexpressions.
        // Sub-expression value is projected onto a synthesized channel so the inline filter sees a Reference.
        if (between.min() instanceof Constant && between.max() instanceof Constant) {
            return createReferenceValueFilterEvaluator(
                    columnarFilterSubexpressionEvaluationEnabled,
                    isDebugOutputEnabled,
                    compiler,
                    pageFunctionCompiler,
                    between.value(),
                    value -> new Between(value, between.min(), between.max()),
                    layout,
                    classNameSuffix);
        }
        // AND decomposition references the value twice; require a Reference so the value is evaluated once.
        Expression valueExpression = between.value();
        if (!(valueExpression instanceof Reference)) {
            return Optional.empty();
        }
        ResolvedFunction lessThanOrEqual = compiler.getMetadata().resolveOperator(
                LESS_THAN_OR_EQUAL,
                ImmutableList.of(valueExpression.type(), valueExpression.type()));
        return createAndExpressionEvaluator(
                columnarFilterSubexpressionEvaluationEnabled,
                isDebugOutputEnabled,
                filterReorderingEnabled,
                compiler,
                pageFunctionCompiler,
                new Logical(
                        Logical.Operator.AND,
                        ImmutableList.of(
                                call(lessThanOrEqual, between.min(), valueExpression),
                                call(lessThanOrEqual, valueExpression, between.max()))),
                layout,
                classNameSuffix);
    }

    /**
     * Builds an evaluator for filters whose value must come from a {@link Reference}
     * (IS_NULL, IS_NOT_NULL, IN, BETWEEN — their compiled columnar filters read the value
     * off a real input channel). If {@code value} is already a Reference, the filter compiles
     * directly. Otherwise, when sub-expression evaluation is enabled, {@code value} is
     * compiled as a page projection and {@code buildFilter} is invoked with the projected
     * reference; the resulting evaluator wraps the filter with
     * {@link ColumnarFilterEvaluatorWithProjectedArguments}.
     */
    private static Optional<Supplier<FilterEvaluator>> createReferenceValueFilterEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Expression value,
            Function<Expression, Expression> buildFilter,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        if (value instanceof Reference) {
            return compiler.generateFilter(buildFilter.apply(value), layout)
                    .map(supplier -> () -> createDictionaryAwareEvaluator(supplier.get()));
        }
        if (!columnarFilterSubexpressionEvaluationEnabled) {
            return Optional.empty();
        }
        Optional<Supplier<PageProjection>> projection = compileProjection(pageFunctionCompiler, value, layout, classNameSuffix);
        if (projection.isEmpty()) {
            return Optional.empty();
        }
        Symbol projectedSymbol = new Symbol(value.type(), "$projected_0");
        Expression rewrittenFilter = buildFilter.apply(new Reference(value.type(), projectedSymbol.name()));
        return compiler.generateFilter(rewrittenFilter, ImmutableMap.of(projectedSymbol, 0))
                .map(supplier -> () -> new ColumnarFilterEvaluatorWithProjectedArguments(
                        new DebugContext(ImmutableList.of(value), layout, rewrittenFilter.toString(), isDebugOutputEnabled),
                        ImmutableList.of(projection.get().get()),
                        createDictionaryAwareEvaluator(supplier.get())));
    }

    private static Optional<Supplier<FilterEvaluator>> createInExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            In in,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        checkArgument(!in.valueList().isEmpty(), "valueList must not be empty");
        return createReferenceValueFilterEvaluator(
                columnarFilterSubexpressionEvaluationEnabled,
                isDebugOutputEnabled,
                compiler,
                pageFunctionCompiler,
                in.value(),
                value -> new In(value, in.valueList()),
                layout,
                classNameSuffix);
    }

    private static Optional<Supplier<FilterEvaluator>> createCallExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Call call,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        if (!extractLambdaExpressions(call).isEmpty()) {
            // not supported
            return Optional.empty();
        }
        Optional<ProjectedArguments> projected = projectFilterArguments(columnarFilterSubexpressionEvaluationEnabled, pageFunctionCompiler, call.arguments(), layout, classNameSuffix);
        if (projected.isEmpty()) {
            return Optional.empty();
        }
        ProjectedArguments arguments = projected.get();
        Call rewrittenCall = new Call(call.function(), arguments.rewrittenArguments());
        return toFilterEvaluator(compiler.generateFilter(rewrittenCall, arguments.rewrittenLayout()), arguments, isDeterministic(call), layout, rewrittenCall, isDebugOutputEnabled);
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNotNullExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Call call,
            IsNull isNull,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        checkArgument(isNotExpression(call), "call %s should be not", call);
        checkArgument(call.arguments().size() == 1);
        Type argumentType = isNull.value().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);
        return createReferenceValueFilterEvaluator(
                columnarFilterSubexpressionEvaluationEnabled,
                isDebugOutputEnabled,
                compiler,
                pageFunctionCompiler,
                isNull.value(),
                value -> new Call(call.function(), ImmutableList.of(new IsNull(value))),
                layout,
                classNameSuffix);
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNullExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            IsNull isNull,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        Type argumentType = isNull.value().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);
        return createReferenceValueFilterEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, isNull.value(), IsNull::new, layout, classNameSuffix);
    }

    private static Optional<Supplier<FilterEvaluator>> createComparisonExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Comparison comparison,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        Optional<ProjectedArguments> projected = projectFilterArguments(columnarFilterSubexpressionEvaluationEnabled, pageFunctionCompiler, ImmutableList.of(comparison.left(), comparison.right()), layout, classNameSuffix);
        if (projected.isEmpty()) {
            return Optional.empty();
        }
        ProjectedArguments arguments = projected.get();
        Comparison rewrittenComparison = new Comparison(comparison.operator(), arguments.rewrittenArguments().get(0), arguments.rewrittenArguments().get(1));
        // comparison operators are always deterministic
        return toFilterEvaluator(compiler.generateFilter(rewrittenComparison, arguments.rewrittenLayout()), arguments, true, layout, rewrittenComparison, isDebugOutputEnabled);
    }

    private static FilterEvaluator createDictionaryAwareEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1, "filter should have 1 input channel");
        return new ColumnarFilterEvaluator(new DictionaryAwareColumnarFilter(filter));
    }

    private static Optional<Supplier<PageProjection>> compileProjection(PageFunctionCompiler compiler, Expression expression, Map<Symbol, Integer> layout, Optional<String> classNameSuffix)
    {
        try {
            return Optional.of(compiler.compileProjection(expression, layout, classNameSuffix));
        }
        catch (Throwable t) {
            if (getCausalChain(t).stream().anyMatch(cause -> cause instanceof UnsupportedOperationException)) {
                log.debug("Unsupported sub-expression for columnar evaluation %s, %s", expression, t);
            }
            else {
                log.warn("Failed to compile sub-expression %s for columnar evaluation, %s", expression, t);
            }
            return Optional.empty();
        }
    }

    /**
     * Holds the result of rewriting a filter's arguments for sub-expression evaluation:
     * non-trivial arguments (anything other than {@link Reference} or {@link Constant})
     * are compiled as page projections and replaced with synthesized {@code $projected_N}
     * references; constants pass through unchanged.
     *
     * <p>{@link #rewrittenArguments} is the argument list to feed back into the parent
     * filter expression (e.g. {@link Call} or {@link Comparison}). {@link #rewrittenLayout}
     * is the layout to compile the rewritten filter against. {@link #argumentProjections}
     * and {@link #argumentExpressions} are empty when no projection was needed.
     */
    record ProjectedArguments(
            List<Expression> rewrittenArguments,
            Map<Symbol, Integer> rewrittenLayout,
            List<Supplier<PageProjection>> argumentProjections,
            List<Expression> argumentExpressions)
    {
        boolean hasProjections()
        {
            return !argumentProjections.isEmpty();
        }
    }

    private static Optional<ProjectedArguments> projectFilterArguments(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            PageFunctionCompiler pageFunctionCompiler,
            List<Expression> arguments,
            Map<Symbol, Integer> layout,
            Optional<String> classNameSuffix)
    {
        long intermediateProjectChannels = arguments.stream()
                .filter(argument -> !(argument instanceof Constant || argument instanceof Reference))
                .count();
        if (intermediateProjectChannels == 0) {
            return Optional.of(new ProjectedArguments(arguments, layout, ImmutableList.of(), ImmutableList.of()));
        }
        if (!columnarFilterSubexpressionEvaluationEnabled) {
            return Optional.empty();
        }

        ImmutableList.Builder<Supplier<PageProjection>> argumentProjectionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Expression> argumentExpressionsBuilder = ImmutableList.builder();
        for (Expression argument : arguments) {
            if (argument instanceof Constant) {
                // Constant is handled directly in the filter evaluation
                continue;
            }
            Optional<Supplier<PageProjection>> projection = compileProjection(pageFunctionCompiler, argument, layout, classNameSuffix);
            if (projection.isEmpty()) {
                return Optional.empty();
            }
            argumentProjectionsBuilder.add(projection.get());
            argumentExpressionsBuilder.add(argument);
        }

        ImmutableList.Builder<Expression> rewrittenArgumentsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Integer> rewrittenLayoutBuilder = ImmutableMap.builder();
        int argumentIndex = 0;
        for (Expression argument : arguments) {
            if (argument instanceof Constant) {
                rewrittenArgumentsBuilder.add(argument);
            }
            else {
                String name = "$projected_" + argumentIndex;
                Symbol symbol = new Symbol(argument.type(), name);
                rewrittenArgumentsBuilder.add(new Reference(argument.type(), name));
                rewrittenLayoutBuilder.put(symbol, argumentIndex);
                argumentIndex++;
            }
        }

        return Optional.of(new ProjectedArguments(
                rewrittenArgumentsBuilder.build(),
                rewrittenLayoutBuilder.buildOrThrow(),
                argumentProjectionsBuilder.build(),
                argumentExpressionsBuilder.build()));
    }

    private static Optional<Supplier<FilterEvaluator>> toFilterEvaluator(
            Optional<Supplier<ColumnarFilter>> compiledFilter,
            ProjectedArguments arguments,
            boolean isDeterministic,
            Map<Symbol, Integer> debugLayout,
            Expression rewrittenExpression,
            boolean isDebugOutputEnabled)
    {
        return compiledFilter.map(filterSupplier -> () -> {
            ColumnarFilter filter = filterSupplier.get();
            FilterEvaluator evaluator = filter.getInputChannels().size() == 1 && isDeterministic ? createDictionaryAwareEvaluator(filter) : new ColumnarFilterEvaluator(filter);
            if (!arguments.hasProjections()) {
                return evaluator;
            }
            DebugContext debugContext = new DebugContext(arguments.argumentExpressions(), debugLayout, rewrittenExpression.toString(), isDebugOutputEnabled);
            return new ColumnarFilterEvaluatorWithProjectedArguments(
                    debugContext,
                    arguments.argumentProjections().stream().map(Supplier::get).collect(toImmutableList()),
                    evaluator);
        });
    }
}
