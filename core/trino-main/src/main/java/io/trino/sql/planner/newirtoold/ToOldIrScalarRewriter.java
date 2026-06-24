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
package io.trino.sql.planner.newirtoold;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Context.RowField;
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Bind;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Case;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operation.Coalesce;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Let;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.dialect.trino.operation.TrinoOperationVisitor;
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical.Operator;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata.FIELD_INDEX;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LOGICAL_OPERATOR;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isFieldSelector;
import static java.util.Objects.requireNonNull;

/**
 * A visitor-based rewriter for scalar operations from new IR to old IR.
 * <p>
 * Note: This rewriter assumes that the input block is correctly structured wrt SSA.
 * This rewriter does not support outer correlation. All values must be resolved within the given block.
 */
public class ToOldIrScalarRewriter
{
    private final Rewriter rewriter;

    public ToOldIrScalarRewriter(SymbolAllocator symbolAllocator)
    {
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.rewriter = new Rewriter(symbolAllocator);
    }

    public Expression toOldIr(Block block, List<List<Symbol>> inputSymbols)
    {
        validateInputSymbols(block, inputSymbols);

        // Note 1: through this mapping we can only refer to the parameters of the given block. No outer correlation is supported. We assume that the given block is uncorrelated.
        // Note 2: this mapping only supports references to block parameters by field. In MLIR, the block parameter is a value which can be referenced as a whole.
        // In our Trino implementation we assumed a limitation that block parameters are of row type, and they can only be referenced by field.
        // It applies both to blocks in relational operations (like the filter predicate), and to scalar lambda parameters.
        // The exception to this rule is the aggregateCalls block in Aggregate operation which takes relation type. This block is not processed by ToOldIrScalarRewriter.
        ImmutableMap.Builder<RowField, Symbol> fieldMapping = ImmutableMap.builder();
        for (int i = 0; i < block.parameters().size(); i++) {
            Block.Parameter parameter = block.parameters().get(i);
            List<Symbol> symbols = inputSymbols.get(i);
            for (int j = 0; j < symbols.size(); j++) {
                fieldMapping.put(new RowField(parameter, j), symbols.get(j));
            }
        }

        // Note: through this mapping, we can only refer to operations of the given block.
        // In MLIR, generally the scope contains all operation results declared above and outside the current site, so we should also see operations from enclosing blocks.
        // However, in our Trino implementation we assumed a limitation that correlation is only realized through block parameters, and outer scope operations
        // cannot be directly accessed.
        Map<Value, TrinoOperation> valueToOperation = block.operations().stream()
                .collect(toImmutableMap(Operation::result, TrinoOperation.class::cast));

        return rewriter.toOldIr(block, new Context(fieldMapping.buildOrThrow(), valueToOperation));
    }

    public List<Symbol> getSelectedSymbols(Block block, List<Symbol> inputSymbols)
    {
        validateInputSymbols(block, ImmutableList.of(inputSymbols));
        checkArgument(isFieldSelector(block, true), "Expected field selector block");

        if (isEmptyFieldSelector(block)) {
            return ImmutableList.of();
        }

        Row row = (Row) toOldIr(block, ImmutableList.of(inputSymbols));

        return row.items().stream()
                .map(Reference.class::cast)
                .map(Symbol::from)
                .collect(toImmutableList());
    }

    public Optional<Symbol> getOptionalSelectedSymbol(Block block, List<Symbol> inputSymbols)
    {
        List<Symbol> selectedSymbols = getSelectedSymbols(block, inputSymbols);

        return selectedSymbols.isEmpty() ? Optional.empty() : Optional.of(getOnlyElement(selectedSymbols));
    }

    public Symbol getSelectedSymbol(Block block, List<Symbol> inputSymbols)
    {
        List<Symbol> selectedSymbols = getSelectedSymbols(block, inputSymbols);

        return getOnlyElement(selectedSymbols);
    }

    public List<Expression> getExpressions(Block block, List<Symbol> inputSymbols)
    {
        validateInputSymbols(block, ImmutableList.of(inputSymbols));

        if (isEmptyFieldSelector(block)) {
            return ImmutableList.of();
        }

        Expression expression = toOldIr(block, ImmutableList.of(inputSymbols));
        checkArgument(expression instanceof Row, "Expected block returning a row");

        return ((Row) expression).items();
    }

    private static class Rewriter
            extends TrinoOperationVisitor<Expression, Context>
    {
        private final SymbolAllocator symbolAllocator;

        public Rewriter(SymbolAllocator symbolAllocator)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        public Expression toOldIr(Block block, Context context)
        {
            Return returnOperation = (Return) block.getTerminalOperation();
            return returnOperation.accept(this, context);
        }

        @Override
        protected Expression visitOperation(TrinoOperation operation, Context context)
        {
            throw new UnsupportedOperationException("ToOldIrScalarRewriter is not implemented for " + operation.name() + ". It must support all scalar operations.");
        }

        @Override
        public Expression visitArray(Array operation, Context context)
        {
            Type elementType = ((ArrayType) trinoType(operation.result().type())).getElementType();
            List<Expression> elements = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.Array(elementType, elements);
        }

        @Override
        public Expression visitBind(Bind operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.Bind(arguments.subList(0, arguments.size() - 1), (io.trino.sql.ir.Lambda) arguments.getLast());
        }

        @Override
        public Expression visitCall(Call operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.Call(RESOLVED_FUNCTION.getAttribute(operation.attributes()), arguments);
        }

        @Override
        public Expression visitCase(Case operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            int whenSize = (operation.arguments().size() - 1) / 2;
            List<Expression> when = arguments.subList(0, whenSize);
            List<Expression> then = arguments.subList(whenSize, whenSize * 2);
            ImmutableList.Builder<WhenClause> whenClauses = ImmutableList.builder();
            for (int i = 0; i < whenSize; i++) {
                whenClauses.add(new WhenClause(when.get(i), then.get(i)));
            }
            Expression defaultValue = arguments.getLast();

            return new io.trino.sql.ir.Case(whenClauses.build(), defaultValue);
        }

        @Override
        public Expression visitCast(Cast operation, Context context)
        {
            Expression argument = context.getOperation(operation.argument()).accept(this, context);
            Type type = trinoType(operation.result().type());

            return new io.trino.sql.ir.Cast(argument, type);
        }

        @Override
        public Expression visitCoalesce(Coalesce operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.Coalesce(arguments);
        }

        @Override
        public Expression visitConstant(Constant operation, Context context)
        {
            return new io.trino.sql.ir.Constant(trinoType(operation.result().type()), CONSTANT_VALUE.getAttribute(operation.attributes()).getValue());
        }

        @Override
        public Expression visitFieldReference(FieldReference operation, Context context)
        {
            Value base = operation.base();
            int index = FIELD_INDEX.getAttribute(operation.attributes());

            if (context.canResolveOperation(base)) {
                Expression baseExpression = context.getOperation(base).accept(this, context);
                return new io.trino.sql.ir.FieldReference(baseExpression, index);
            }

            Symbol symbol = context.getMapping(base, index);
            return symbol.toSymbolReference();
        }

        @Override
        public Expression visitIn(In operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.In(arguments.getFirst(), arguments.subList(1, arguments.size()));
        }

        @Override
        public Expression visitIsNull(IsNull operation, Context context)
        {
            Expression argument = context.getOperation(operation.argument()).accept(this, context);

            return new io.trino.sql.ir.IsNull(argument);
        }

        @Override
        public Expression visitLambda(Lambda operation, Context context)
        {
            // build context to visit the lambda body
            // - compose field mapping from enclosing block mapping and lambda parameters mapping
            ImmutableMap.Builder<RowField, Symbol> fieldMapping = ImmutableMap.builder();
            fieldMapping.putAll(context.fieldMapping());
            // assign symbols for lambda parameters
            Block lambdaBody = operation.lambdaBody();
            Block.Parameter lambdaBlockParameter = getOnlyElement(lambdaBody.parameters());
            List<Symbol> lambdaParameters = trinoType(lambdaBlockParameter.type()).getTypeParameters().stream()
                    .map(type -> symbolAllocator.newSymbol("lambda_parameter", type))
                    .collect(toImmutableList());
            for (int i = 0; i < lambdaParameters.size(); i++) {
                fieldMapping.put(new RowField(lambdaBlockParameter, i), lambdaParameters.get(i));
            }
            // - build value to operation mapping for the lambda body
            Map<Value, TrinoOperation> valueToOperation = lambdaBody.operations().stream()
                    .collect(toImmutableMap(Operation::result, TrinoOperation.class::cast));

            Expression lambdaExpression = toOldIr(lambdaBody, new Context(fieldMapping.buildOrThrow(), valueToOperation));

            return new io.trino.sql.ir.Lambda(lambdaParameters, lambdaExpression);
        }

        @Override
        public Expression visitLet(Let operation, Context context)
        {
            Expression value = context.getOperation(operation.value()).accept(this, context);

            // build context to visit the let body
            // - compose field mapping from enclosing block mapping and the bound value mapping
            Block body = operation.body();
            Block.Parameter parameter = getOnlyElement(body.parameters());
            Symbol symbol = symbolAllocator.newSymbol("let", getOnlyElement(trinoType(parameter.type()).getTypeParameters()));

            ImmutableMap.Builder<RowField, Symbol> fieldMapping = ImmutableMap.builder();
            fieldMapping.putAll(context.fieldMapping());
            fieldMapping.put(new RowField(parameter, 0), symbol);

            // - build value to operation mapping for the let body
            Map<Value, TrinoOperation> valueToOperation = body.operations().stream()
                    .collect(toImmutableMap(Operation::result, TrinoOperation.class::cast));

            Expression bodyExpression = toOldIr(body, new Context(fieldMapping.buildOrThrow(), valueToOperation));

            return new io.trino.sql.ir.Let(symbol, value, bodyExpression);
        }

        @Override
        public Expression visitLogical(Logical operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new io.trino.sql.ir.Logical(rewriteOperator(LOGICAL_OPERATOR.getAttribute(operation.attributes())), arguments);
        }

        private Operator rewriteOperator(LogicalOperator operator)
        {
            return switch (operator) {
                case AND -> Operator.AND;
                case OR -> Operator.OR;
            };
        }

        @Override
        public Expression visitReturn(Return operation, Context context)
        {
            return context.getOperation(operation.argument()).accept(this, context);
        }

        @Override
        public Expression visitRow(io.trino.sql.dialect.trino.operation.Row operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            return new Row(arguments);
        }

        @Override
        public Expression visitMatch(io.trino.sql.dialect.trino.operation.Match operation, Context context)
        {
            List<Expression> arguments = operation.arguments().stream()
                    .map(context::getOperation)
                    .map(element -> element.accept(this, context))
                    .collect(toImmutableList());

            int whenSize = (operation.arguments().size() - 2) / 2;
            Expression operand = arguments.getFirst();
            List<Expression> when = arguments.subList(1, whenSize + 1);
            List<Expression> then = arguments.subList(whenSize + 1, whenSize * 2 + 1);
            ImmutableList.Builder<MatchClause> matchClauses = ImmutableList.builder();
            for (int i = 0; i < whenSize; i++) {
                matchClauses.add(new MatchClause(when.get(i), then.get(i)));
            }
            Expression defaultValue = arguments.getLast();

            return new Match(operand, matchClauses.build(), defaultValue);
        }
    }

    /**
     * The context for the rewrite from new IR to old IR.
     * It allows to resolve all value references in the block.
     * Values are either operation results or block parameters.
     * The references to operation results can be resolved using valueToOperation.
     * The references to block parameters can be resolved using fieldMapping.
     * // TODO refactor this when we have proper value resolution
     */
    private record Context(Map<RowField, Symbol> fieldMapping, Map<Value, TrinoOperation> valueToOperation)
    {
        public Context
        {
            fieldMapping = ImmutableMap.copyOf(fieldMapping);
            valueToOperation = ImmutableMap.copyOf(valueToOperation);
        }

        public boolean canResolveOperation(Value value)
        {
            return value instanceof Operation.Result && valueToOperation.containsKey(value);
        }

        public TrinoOperation getOperation(Value value)
        {
            checkArgument(value instanceof Operation.Result, "Value %s is not an operation result", value.name());
            checkState(valueToOperation.containsKey(value), "Could not resolve value %s as operation result", value.name());
            return valueToOperation.get(value);
        }

        public Symbol getMapping(Value value, int index)
        {
            checkArgument(value instanceof Block.Parameter, "Value %s is not a block parameter", value.name());
            RowField rowField = new RowField((Block.Parameter) value, index);
            checkState(fieldMapping.containsKey(rowField), "Could not resolve reference %s as block parameter field", rowField);
            return fieldMapping.get(rowField);
        }
    }

    /**
     * Check that the provided lists of symbols match the block parameters.
     */
    private static void validateInputSymbols(Block block, List<List<Symbol>> inputSymbols)
    {
        List<Type> parameterTypes = block.parameters().stream()
                .map(Block.Parameter::type)
                .map(TrinoDialect::trinoType)
                .collect(toImmutableList());

        List<Type> inputTypes = inputSymbols.stream()
                .map(symbols -> {
                    if (symbols.isEmpty()) {
                        return EMPTY_ROW;
                    }
                    return RowType.anonymous(symbols.stream()
                            .map(Symbol::type)
                            .collect(toImmutableList()));
                })
                .collect(toImmutableList());

        checkArgument(parameterTypes.equals(inputTypes), "Type mismatch");
    }
}
