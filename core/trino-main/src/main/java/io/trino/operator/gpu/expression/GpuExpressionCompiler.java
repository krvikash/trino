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

import com.google.common.collect.Ordering;
import io.trino.operator.project.InputChannels;
import io.trino.spi.function.CatalogSchemaFunctionName;
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

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.gpu.GpuScore.POTENTIAL;
import static io.trino.operator.gpu.GpuScore.PREFERRED;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;

/**
 * Compiles Trino RowExpressions into GPU-executable operations using cuDF.
 */
public class GpuExpressionCompiler
{
    public Optional<CompiledExpression> compileExpression(RowExpression expression)
    {
        return expression.accept(new CompilationVisitor(), null);
    }

    private static class CompilationVisitor
            implements RowExpressionVisitor<Optional<CompiledExpression>, Void>
    {
        @Override
        public Optional<CompiledExpression> visitInputReference(InputReferenceExpression reference, Void context)
        {
            return Optional.of(new CompiledExpression(
                    inputColumns -> getOnlyElement(inputColumns).incRefCount(),
                    new InputChannels(reference.field()),
                    POTENTIAL));
        }

        @Override
        public Optional<CompiledExpression> visitCall(CallExpression call, Void context)
        {
            CatalogSchemaFunctionName functionName = call.resolvedFunction().signature().getName();

            if (functionName.equals(builtinFunctionName(LIKE_FUNCTION_NAME)) &&
                    call.arguments().size() == 2 &&
                    call.arguments().get(1) instanceof ConstantExpression(Object likePattern, Type patternType) &&
                    patternType == LIKE_PATTERN) {
                Optional<CompiledExpression> searchedProcessed = call.arguments().get(0).accept(this, context);
                return searchedProcessed.map(searched -> new CompiledExpression(
                        new GpuLike(searched.expression(), ((LikePattern) likePattern).getPattern(), ((LikePattern) likePattern).getEscape()),
                        searched.inputChannels(),
                        Ordering.natural().max(searched.score(), PREFERRED)));
            }

            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) detect regular expression functions (as PREFERRED)
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) implement arithmetics (as POTENTIAL)

            return Optional.empty();
        }

        @Override
        public Optional<CompiledExpression> visitSpecialForm(SpecialForm specialForm, Void context)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) Implement special forms (AND, OR, CASE, IN, etc.)
            // This would recursively compile arguments and generate appropriate cuDF operations

            // For now, return empty as cuDF code generation not yet implemented
            return Optional.empty();
        }

        @Override
        public Optional<CompiledExpression> visitConstant(ConstantExpression literal, Void context)
        {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9851) Implement constant expression
            // This would create a cuDF scalar or constant column

            // For now, return empty as cuDF code generation not yet implemented
            return Optional.empty();
        }

        @Override
        public Optional<CompiledExpression> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<CompiledExpression> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return Optional.empty();
        }
    }
}
