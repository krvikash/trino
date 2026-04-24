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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.BinaryOp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.trino.operator.gpu.GpuScore;
import io.trino.operator.project.PageFieldsToInputParametersRewriter.Result;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.gpu.GpuTypeConversion.GpuTypeMapping;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.type.LikePattern;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.operator.gpu.GpuScore.POTENTIAL;
import static io.trino.operator.gpu.GpuScore.PREFERRED;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

/**
 * Compiles Trino RowExpressions into GPU-executable operations using cuDF.
 */
public class GpuExpressionCompiler
{
    private static final int TINYINT_DECIMAL_DIGITS = 3;
    private static final int SMALLINT_DECIMAL_DIGITS = 5;
    private static final int INTEGER_DECIMAL_DIGITS = 10;
    private static final int BIGINT_DECIMAL_DIGITS = 19;

    public Optional<List<CompiledExpression>> compileExpressions(List<RowExpression> expressions)
    {
        ImmutableList.Builder<CompiledExpression> compiledExpressions = ImmutableList.builderWithExpectedSize(expressions.size());
        for (RowExpression expression : expressions) {
            Optional<CompiledExpression> compiled = compileExpression(expression);
            if (compiled.isEmpty()) {
                return Optional.empty();
            }
            compiledExpressions.add(compiled.get());
        }
        return Optional.of(compiledExpressions.build());
    }

    public Optional<CompiledExpression> compileExpression(RowExpression expression)
    {
        // Rewrite field references to use compact, consecutive indexes (0, 1, 2, ...).
        // This allows all sub-expressions to index directly into the shared inputColumns list
        // without needing per-expression input channel mappings.
        Result rewritten = rewritePageFieldsToInputParameters(expression);

        return rewritten.getRewrittenExpression()
                .accept(new CompilationVisitor(), null)
                .map(result -> new CompiledExpression(result.expression(), rewritten.getInputChannels(), result.score()));
    }

    private static class CompilationVisitor
            implements RowExpressionVisitor<Optional<CompilationResult>, Void>
    {
        @Override
        public Optional<CompilationResult> visitInputReference(InputReferenceExpression reference, Void context)
        {
            if (!isConvertible(reference.type())) {
                return Optional.empty();
            }
            int field = reference.field();
            return Optional.of(new CompilationResult(
                    (_, inputColumns) -> inputColumns.get(field).incRefCount(),
                    POTENTIAL));
        }

        @Override
        public Optional<CompilationResult> visitCall(CallExpression call, Void context)
        {
            CatalogSchemaFunctionName functionName = call.resolvedFunction().signature().getName();

            if (functionName.equals(builtinFunctionName(LIKE_FUNCTION_NAME)) &&
                    call.arguments().size() == 2 &&
                    call.arguments().get(1) instanceof ConstantExpression(Object likePattern, Type patternType) &&
                    patternType == LIKE_PATTERN) {
                return call.arguments().get(0).accept(this, context)
                        .map(searched -> new CompilationResult(
                                new GpuLike(searched.expression(), ((LikePattern) likePattern).getPattern(), ((LikePattern) likePattern).getEscape()),
                                Ordering.natural().max(searched.score(), PREFERRED)));
            }

            if (functionName.equals(builtinFunctionName("$not")) && call.arguments().size() == 1) {
                return call.arguments().getFirst().accept(this, context)
                        .map(operand -> new CompilationResult(
                                new GpuNot(operand.expression()),
                                operand.score()));
            }

            String name = functionName.functionName();
            if (isOperatorName(name)) {
                OperatorType operatorType = unmangleOperator(name);
                if (call.arguments().size() == 2) {
                    return compileBinaryExpression(call, operatorType, context);
                }
                if (operatorType == OperatorType.CAST) {
                    verify(call.arguments().size() == 1, "Expected exactly one cast argument, got: %s", call.arguments());
                    return compileCast(getOnlyElement(call.arguments()), call.type(), context);
                }
            }

            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) detect regular expression functions (as PREFERRED)

            return Optional.empty();
        }

        private Optional<CompilationResult> compileBinaryExpression(CallExpression call, OperatorType operatorType, Void context)
        {
            return toBinaryOp(operatorType)
                    .flatMap(operation -> toDType(call.type())
                            .flatMap(resultDType -> compileAll(call.arguments(), context)
                                    .map(results -> new CompilationResult(
                                            new GpuBinaryExpression(results.get(0).expression(), results.get(1).expression(), operation, resultDType),
                                            maxScore(results, POTENTIAL)))));
        }

        private static Optional<BinaryOp> toBinaryOp(OperatorType operatorType)
        {
            BinaryOp operation = switch (operatorType) {
                case ADD -> BinaryOp.ADD;
                case SUBTRACT -> BinaryOp.SUB;
                case MULTIPLY -> BinaryOp.MUL;
                case DIVIDE -> BinaryOp.DIV;
                case MODULUS -> BinaryOp.MOD;
                case EQUAL -> BinaryOp.EQUAL;
                case LESS_THAN -> BinaryOp.LESS;
                case LESS_THAN_OR_EQUAL -> BinaryOp.LESS_EQUAL;
                default -> null;
            };
            return Optional.ofNullable(operation);
        }

        private Optional<CompilationResult> compileCast(RowExpression argument, Type toType, Void context)
        {
            return toDType(toType)
                    .flatMap(resultDType -> argument.accept(this, context)
                            .flatMap(compiledArgument -> {
                                if (isCastSafe(argument.type(), toType)) {
                                    return Optional.of(new CompilationResult(
                                            new GpuCast(compiledArgument.expression(), resultDType),
                                            compiledArgument.score()));
                                }
                                return Optional.empty();
                            }));
        }

        private static boolean isCastSafe(Type fromType, Type toType)
        {
            if (fromType.equals(toType)) {
                return true;
            }
            if (fromType == TINYINT) {
                if (toType == SMALLINT || toType == INTEGER || toType == BIGINT || toType == REAL || toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
                if (toType instanceof DecimalType decimalType && TINYINT_DECIMAL_DIGITS <= decimalType.getPrecision() - decimalType.getScale()) {
                    return true;
                }
                if (toType instanceof VarcharType varcharType && TINYINT_DECIMAL_DIGITS + 1 /*sign*/ <= varcharType.getLength().orElse(Integer.MAX_VALUE)) {
                    return true;
                }
            }
            if (fromType == SMALLINT) {
                if (toType == INTEGER || toType == BIGINT || toType == REAL || toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
                if (toType instanceof DecimalType decimalType && SMALLINT_DECIMAL_DIGITS <= decimalType.getPrecision() - decimalType.getScale()) {
                    return true;
                }
                if (toType instanceof VarcharType varcharType && SMALLINT_DECIMAL_DIGITS + 1 /*sign*/ <= varcharType.getLength().orElse(Integer.MAX_VALUE)) {
                    return true;
                }
            }
            if (fromType == INTEGER) {
                if (toType == BIGINT || toType == REAL || toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
                if (toType instanceof DecimalType decimalType && INTEGER_DECIMAL_DIGITS <= decimalType.getPrecision() - decimalType.getScale()) {
                    return true;
                }
                if (toType instanceof VarcharType varcharType && INTEGER_DECIMAL_DIGITS + 1 /*sign*/ <= varcharType.getLength().orElse(Integer.MAX_VALUE)) {
                    return true;
                }
            }
            if (fromType == BIGINT) {
                if (toType == REAL || toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
                if (toType instanceof DecimalType decimalType && BIGINT_DECIMAL_DIGITS <= decimalType.getPrecision() - decimalType.getScale()) {
                    return true;
                }
                if (toType instanceof VarcharType varcharType && BIGINT_DECIMAL_DIGITS + 1 /*sign*/ <= varcharType.getLength().orElse(Integer.MAX_VALUE)) {
                    return true;
                }
            }
            if (fromType == REAL) {
                if (toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
            }
            if (fromType == DOUBLE) {
                if (toType == NUMBER) {
                    return true;
                }
            }
            if (fromType instanceof DecimalType fromDecimal) {
                if (toType == TINYINT && fromDecimal.getScale() == 0 && fromDecimal.getPrecision() < TINYINT_DECIMAL_DIGITS) {
                    return true;
                }
                if (toType == SMALLINT && fromDecimal.getScale() == 0 && fromDecimal.getPrecision() < SMALLINT_DECIMAL_DIGITS) {
                    return true;
                }
                if (toType == INTEGER && fromDecimal.getScale() == 0 && fromDecimal.getPrecision() < INTEGER_DECIMAL_DIGITS) {
                    return true;
                }
                if (toType == BIGINT && fromDecimal.getScale() == 0 && fromDecimal.getPrecision() < BIGINT_DECIMAL_DIGITS) {
                    return true;
                }
                if (toType == REAL || toType == DOUBLE || toType == NUMBER) {
                    return true;
                }
                if (toType instanceof DecimalType toDecimal &&
                        // target has at least as many fractional digits (no rounding)
                        fromDecimal.getScale() <= toDecimal.getScale() &&
                        // target has at least as many integer digits (no overflow)
                        fromDecimal.getPrecision() - fromDecimal.getScale() <= toDecimal.getPrecision() - toDecimal.getScale()) {
                    return true;
                }
            }
            if (fromType instanceof VarcharType fromVarchar) {
                if (toType instanceof VarcharType toVarchar) {
                    if (toVarchar.isUnbounded() || (!fromVarchar.isUnbounded() && fromVarchar.getBoundedLength() <= toVarchar.getBoundedLength())) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Optional<CompilationResult> visitSpecialForm(SpecialForm specialForm, Void context)
        {
            return switch (specialForm.form()) {
                case AND -> compileNary(specialForm.arguments(), GpuLogicalExpression::and, context);
                case OR -> compileNary(specialForm.arguments(), GpuLogicalExpression::or, context);
                case COALESCE -> compileNary(specialForm.arguments(), GpuCoalesce::new, context);
                case IS_NULL -> compileIsNull(specialForm.arguments(), context);
                case BETWEEN -> compileBetween(specialForm.arguments(), context);
                case IN -> compileIn(specialForm.arguments(), context);
                // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) Implement special forms (CASE, etc.)
                default -> Optional.empty();
            };
        }

        private Optional<CompilationResult> compileNary(
                List<RowExpression> arguments,
                Function<List<GpuExpression>, GpuExpression> expressionFactory,
                Void context)
        {
            checkArgument(arguments.size() >= 2, "Expression requires at least 2 arguments, got %s", arguments.size());
            return compileAll(arguments, context)
                    .map(results -> new CompilationResult(
                            expressionFactory.apply(results.stream().map(CompilationResult::expression).collect(toImmutableList())),
                            maxScore(results, POTENTIAL)));
        }

        private Optional<CompilationResult> compileIsNull(List<RowExpression> arguments, Void context)
        {
            checkArgument(arguments.size() == 1, "IS NULL requires 1 argument, got %s", arguments.size());
            return getOnlyElement(arguments).accept(this, context)
                    .map(operand -> new CompilationResult(
                            new GpuIsNull(operand.expression()),
                            Ordering.natural().max(operand.score(), POTENTIAL)));
        }

        private Optional<CompilationResult> compileBetween(List<RowExpression> arguments, Void context)
        {
            checkArgument(arguments.size() == 3, "BETWEEN requires 3 argument, got %s", arguments.size());
            return compileAll(arguments, context)
                    .map(results -> new CompilationResult(
                            new GpuBetween(results.get(0).expression(), results.get(1).expression(), results.get(2).expression()),
                            maxScore(results, POTENTIAL)));
        }

        private Optional<CompilationResult> compileIn(List<RowExpression> arguments, Void context)
        {
            checkArgument(arguments.size() >= 2, "IN requires at least 2 arguments, got %s", arguments.size());

            // First argument is the value to test
            RowExpression valueExpression = arguments.getFirst();

            Optional<GpuTypeMapping> typeMapping = toGpuMapping(valueExpression.type());
            if (typeMapping.isEmpty()) {
                return Optional.empty();
            }

            Optional<CompilationResult> valueCompiled = valueExpression.accept(this, context);
            if (valueCompiled.isEmpty()) {
                return Optional.empty();
            }

            // Currently, we support only constants
            boolean hasNull = false;
            ImmutableList.Builder<Object> nonNullConstants = ImmutableList.builder();
            for (int i = 1; i < arguments.size(); i++) {
                if (!(arguments.get(i) instanceof ConstantExpression constant)) {
                    return Optional.empty();
                }
                Object value = constant.value();
                if (value == null) {
                    hasNull = true;
                }
                else {
                    nonNullConstants.add(value);
                }
            }

            return Optional.of(new CompilationResult(
                    new GpuIn(valueCompiled.get().expression(), nonNullConstants.build(), hasNull, valueExpression.type(), typeMapping.get().toColumn()),
                    Ordering.natural().max(valueCompiled.get().score(), POTENTIAL)));
        }

        private Optional<List<CompilationResult>> compileAll(List<RowExpression> expressions, Void context)
        {
            ImmutableList.Builder<CompilationResult> results = ImmutableList.builder();
            for (RowExpression expression : expressions) {
                Optional<CompilationResult> compiled = expression.accept(this, context);
                if (compiled.isEmpty()) {
                    return Optional.empty();
                }
                results.add(compiled.get());
            }
            return Optional.of(results.build());
        }

        private static GpuScore maxScore(List<CompilationResult> results, GpuScore defaultScore)
        {
            return results.stream()
                    .map(CompilationResult::score)
                    .max(Ordering.natural())
                    .orElse(defaultScore);
        }

        @Override
        public Optional<CompilationResult> visitConstant(ConstantExpression literal, Void context)
        {
            return toGpuMapping(literal.type())
                    .map(typeMapping -> new CompilationResult(
                            new GpuConstant(typeMapping.toScalar(), Optional.ofNullable(literal.value())),
                            POTENTIAL));
        }

        @Override
        public Optional<CompilationResult> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<CompilationResult> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return Optional.empty();
        }
    }

    private record CompilationResult(GpuExpression expression, GpuScore score)
    {
        public CompilationResult
        {
            requireNonNull(expression, "expression is null");
            requireNonNull(score, "score is null");
        }
    }
}
