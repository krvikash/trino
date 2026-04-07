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
import com.google.common.collect.Ordering;
import io.trino.operator.gpu.GpuScore;
import io.trino.operator.project.PageFieldsToInputParametersRewriter.Result;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.type.LikePattern;

import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.operator.gpu.GpuScore.POTENTIAL;
import static io.trino.operator.gpu.GpuScore.PREFERRED;
import static io.trino.operator.gpu.GpuTypes.toDType;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

/**
 * Compiles Trino RowExpressions into GPU-executable operations using cuDF.
 */
public class GpuExpressionCompiler
{
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

            String name = functionName.functionName();
            if (isOperatorName(name) && call.arguments().size() == 2) {
                OperatorType operatorType = unmangleOperator(name);
                return compileBinaryExpression(call, operatorType, context);
            }

            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) detect regular expression functions (as PREFERRED)

            return Optional.empty();
        }

        private Optional<CompilationResult> compileBinaryExpression(CallExpression call, OperatorType operatorType, Void context)
        {
            return toBinaryOp(operatorType)
                    .flatMap(operation -> call.arguments().get(0).accept(this, context)
                            .flatMap(left -> call.arguments().get(1).accept(this, context)
                                    .flatMap(right -> Optional.of(new CompilationResult(
                                            new GpuBinaryExpression(left.expression(), right.expression(), operation, toDType(call.type())),
                                            Ordering.natural().max(
                                                    Ordering.natural().max(left.score(), right.score()),
                                                    POTENTIAL))))));
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

        @Override
        public Optional<CompilationResult> visitSpecialForm(SpecialForm specialForm, Void context)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) Implement special forms (AND, OR, CASE, IN, etc.)
            // This would recursively compile arguments and generate appropriate cuDF operations

            // For now, return empty as cuDF code generation not yet implemented
            return Optional.empty();
        }

        @Override
        public Optional<CompilationResult> visitConstant(ConstantExpression literal, Void context)
        {
            return Optional.of(new CompilationResult(
                    new GpuConstant(literal.value(), literal.type()),
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
