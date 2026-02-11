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
import io.airlift.log.Logger;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.function.FunctionKind.BATCH;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.sql.gen.columnar.AndFilterEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.gen.columnar.DynamicPageFilter.DynamicFilterEvaluator;
import static io.trino.sql.gen.columnar.OrFilterEvaluator.createOrExpressionEvaluator;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.type.UnknownType.UNKNOWN;

/**
 * Used by PageProcessor to evaluate filter expression on input Page.
 * <p>
 * Implementations handle dictionary aware processing through {@link DictionaryAwareColumnarFilter}.
 */
public sealed interface FilterEvaluator
        permits
        AndFilterEvaluator,
        ColumnarFilterEvaluator,
        ColumnarFilterEvaluatorWithProjectedArguments,
        OrFilterEvaluator,
        PageFilterEvaluator,
        SelectAllEvaluator,
        SelectNoneEvaluator,
        DynamicFilterEvaluator
{
    Logger log = Logger.get(FilterEvaluator.class);

    SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page);

    record SelectionResult(SelectedPositions selectedPositions, long filterTimeNanos) {}

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterEvaluationEnabled,
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            Optional<RowExpression> filter,
            ColumnarFilterCompiler columnarFilterCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            Optional<String> classNameSuffix)
    {
        if (columnarFilterEvaluationEnabled && filter.isPresent()) {
            return createColumnarFilterEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, filter.get(), columnarFilterCompiler, pageFunctionCompiler, classNameSuffix);
        }
        return Optional.empty();
    }

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            RowExpression rowExpression,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            Optional<String> classNameSuffix)
    {
        // Eventually this should use RowExpressionVisitor when we handle nested RowExpressions
        if (rowExpression instanceof ConstantExpression constantExpression) {
            if (constantExpression.value() instanceof Boolean booleanValue) {
                return booleanValue ? Optional.of(SelectAllEvaluator::new) : Optional.of(SelectNoneEvaluator::new);
            }
        }
        if (rowExpression instanceof CallExpression callExpression) {
            if (callExpression.resolvedFunction().functionKind() == BATCH) {
                // Batch functions are not supported in columnar filter evaluation
                return Optional.empty();
            }
            if (isNotExpression(callExpression)) {
                // "not(is_null(input_reference))" is handled explicitly as it is easy.
                if (callExpression.arguments().getFirst() instanceof SpecialForm specialFormArg && specialFormArg.form() == IS_NULL) {
                    return createIsNotNullExpressionEvaluator(compiler, callExpression);
                }
            }
            return createCallExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, callExpression, classNameSuffix);
        }
        if (rowExpression instanceof SpecialForm specialFormArg) {
            if (specialFormArg.form() == IS_NULL) {
                return createIsNullExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.form() == AND) {
                return createAndExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, specialFormArg, classNameSuffix);
            }
            if (specialFormArg.form() == OR) {
                return createOrExpressionEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, specialFormArg, classNameSuffix);
            }
            if (specialFormArg.form() == BETWEEN) {
                return createBetweenEvaluator(columnarFilterSubexpressionEvaluationEnabled, isDebugOutputEnabled, compiler, pageFunctionCompiler, specialFormArg, classNameSuffix);
            }
            if (specialFormArg.form() == IN) {
                return createInExpressionEvaluator(compiler, specialFormArg);
            }
        }
        return Optional.empty();
    }

    static boolean isNotExpression(CallExpression callExpression)
    {
        CatalogSchemaFunctionName functionName = callExpression.resolvedFunction().name();
        return isBuiltinFunctionName(functionName) && functionName.functionName().equals("$not");
    }

    private static Optional<Supplier<FilterEvaluator>> createBetweenEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            SpecialForm specialForm,
            Optional<String> classNameSuffix)
    {
        checkArgument(specialForm.form() == BETWEEN, "specialForm should be BETWEEN");
        checkArgument(specialForm.arguments().size() == 3, "BETWEEN should have 3 arguments %s", specialForm.arguments());
        checkArgument(specialForm.functionDependencies().size() == 1, "BETWEEN should have 1 functional dependency %s", specialForm.functionDependencies());

        ResolvedFunction lessThanOrEqual = specialForm.getOperatorDependency(LESS_THAN_OR_EQUAL);
        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on InputReference
        RowExpression valueExpression = specialForm.arguments().get(0);
        if (!(valueExpression instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // When the min and max arguments of a BETWEEN expression are both constants, evaluating them inline is cheaper than AND-ing subexpressions
        if (specialForm.arguments().get(1) instanceof ConstantExpression && specialForm.arguments().get(2) instanceof ConstantExpression) {
            Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
            return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
        }
        return createAndExpressionEvaluator(
                columnarFilterSubexpressionEvaluationEnabled,
                isDebugOutputEnabled,
                compiler,
                pageFunctionCompiler,
                new SpecialForm(
                        AND,
                        BOOLEAN,
                        ImmutableList.of(
                                call(lessThanOrEqual, specialForm.arguments().get(1), valueExpression),
                                call(lessThanOrEqual, valueExpression, specialForm.arguments().get(2))),
                        ImmutableList.of()),
                classNameSuffix);
    }

    private static Optional<Supplier<FilterEvaluator>> createInExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == IN, "specialForm %s should be IN", specialForm);
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createCallExpressionEvaluator(
            boolean columnarFilterSubexpressionEvaluationEnabled,
            boolean isDebugOutputEnabled,
            ColumnarFilterCompiler compiler,
            PageFunctionCompiler pageFunctionCompiler,
            CallExpression callExpression,
            Optional<String> classNameSuffix)
    {
        if (!extractLambdaExpressions(callExpression).isEmpty()) {
            // not supported
            return Optional.empty();
        }
        List<RowExpression> arguments = callExpression.arguments();
        long intermediateProjectChannels = arguments.stream()
                .filter(argumentExpression -> !(argumentExpression instanceof ConstantExpression || argumentExpression instanceof InputReferenceExpression))
                .count();
        List<Supplier<PageProjection>> argumentProjections;
        List<RowExpression> argumentExpressions;
        CallExpression rewrittenCallExpression;
        if (intermediateProjectChannels == 0) {
            argumentProjections = ImmutableList.of();
            argumentExpressions = ImmutableList.of();
            rewrittenCallExpression = callExpression;
        }
        else {
            if (!columnarFilterSubexpressionEvaluationEnabled) {
                return Optional.empty();
            }
            ImmutableList.Builder<Supplier<PageProjection>> argumentProjectionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> argumentExpressionsBuilder = ImmutableList.builder();
            for (RowExpression argumentExpression : arguments) {
                if (argumentExpression instanceof ConstantExpression) {
                    // ConstantExpression is handled directly in the filter evaluation
                    continue;
                }
                Optional<Supplier<PageProjection>> projection = compileProjection(pageFunctionCompiler, argumentExpression, classNameSuffix);
                if (projection.isEmpty()) {
                    return Optional.empty();
                }
                argumentProjectionsBuilder.add(projection.get());
                argumentExpressionsBuilder.add(argumentExpression);
            }
            argumentProjections = argumentProjectionsBuilder.build();
            argumentExpressions = argumentExpressionsBuilder.build();

            ImmutableList.Builder<RowExpression> filterInputExpressionsBuilder = ImmutableList.builder();
            int argumentIndex = 0;
            for (RowExpression argumentExpression : arguments) {
                if (argumentExpression instanceof ConstantExpression) {
                    filterInputExpressionsBuilder.add(argumentExpression);
                }
                else {
                    filterInputExpressionsBuilder.add(new InputReferenceExpression(argumentIndex, argumentExpression.type()));
                    argumentIndex++;
                }
            }
            rewrittenCallExpression = new CallExpression(callExpression.resolvedFunction(), filterInputExpressionsBuilder.build());
        }

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(rewrittenCallExpression);
        boolean isDeterministic = isDeterministic(callExpression);
        return compiledFilter.map(filterSupplier -> () -> {
            ColumnarFilter filter = filterSupplier.get();
            FilterEvaluator evaluator = filter.getInputChannels().size() == 1 && isDeterministic ? createDictionaryAwareEvaluator(filter) : new ColumnarFilterEvaluator(filter);
            if (intermediateProjectChannels == 0) {
                return evaluator;
            }
            DebugContext debugContext = new DebugContext(argumentExpressions, rewrittenCallExpression.toString(), isDebugOutputEnabled);
            return new ColumnarFilterEvaluatorWithProjectedArguments(
                    debugContext,
                    argumentProjections.stream().map(Supplier::get).collect(toImmutableList()),
                    evaluator);
        });
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNotNullExpressionEvaluator(ColumnarFilterCompiler compiler, CallExpression callExpression)
    {
        checkArgument(isNotExpression(callExpression), "callExpression %s should be not", callExpression);
        checkArgument(callExpression.arguments().size() == 1);
        SpecialForm specialForm = (SpecialForm) callExpression.arguments().getFirst();
        checkArgument(specialForm.form() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        Type argumentType = specialForm.arguments().getFirst().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(callExpression);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNullExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        Type argumentType = specialForm.arguments().getFirst().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static FilterEvaluator createDictionaryAwareEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1, "filter should have 1 input channel");
        return new ColumnarFilterEvaluator(new DictionaryAwareColumnarFilter(filter));
    }

    private static Optional<Supplier<PageProjection>> compileProjection(PageFunctionCompiler compiler, RowExpression expression, Optional<String> classNameSuffix)
    {
        try {
            return Optional.of(compiler.compileProjection(expression, classNameSuffix));
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
}
