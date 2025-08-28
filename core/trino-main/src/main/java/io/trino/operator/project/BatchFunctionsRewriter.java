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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.FunctionKind.BATCH;

public final class BatchFunctionsRewriter
{
    private BatchFunctionsRewriter() {}

    /**
     * Rewrites batch function calls in the given expression to variable references.
     * The variable reference name is generated as `"batch_output_" + index` where index is the
     * order of appearance of the batch function call in a pre-order traversal of the expression tree.
     * Collects the batch function call expressions in a list.
     */
    public static Result rewriteBatchFunctionsToVariableReferences(RowExpression expression)
    {
        BatchFunctionToVariableReferenceRewriter visitor = new BatchFunctionToVariableReferenceRewriter();
        RowExpression rewrittenProjection = expression.accept(visitor, null);
        return new Result(rewrittenProjection, visitor.getBatchExpressions());
    }

    /**
     * Rewrites variable references representing batch function outputs to input references.
     * The input reference channel is calculated as: non-batch-input-channel-count + batch-output-index
     * where batch-output-index is extracted from the variable name `"batch_output_" + index`.
     * The non-batch-input-channel-count is calculated by counting distinct input reference channels in the
     * given expression.
     */
    public static RowExpression rewriteBatchVariableReferencesToInputReferences(RowExpression expression)
    {
        NonBatchInputReferenceCounter counter = new NonBatchInputReferenceCounter();
        expression.accept(counter, null);
        int nonBatchInputChannelsCount = counter.getNonBatchInputChannelsCount();
        BatchVariableReferenceToInputReferenceRewriter visitor = new BatchVariableReferenceToInputReferenceRewriter(nonBatchInputChannelsCount);
        return expression.accept(visitor, null);
    }

    public static boolean containsBatchFunction(RowExpression expression)
    {
        RowExpressionVisitor<Boolean, Void> visitor = new RowExpressionVisitor<>()
        {
            @Override
            public Boolean visitCall(CallExpression call, Void context)
            {
                if (call.resolvedFunction().functionKind() == BATCH) {
                    return true;
                }
                return call.arguments().stream()
                        .map(arguments -> arguments.accept(this, context))
                        .anyMatch(result -> result != null && result);
            }

            @Override
            public Boolean visitSpecialForm(SpecialForm specialForm, Void context)
            {
                return specialForm.arguments().stream()
                        .map(expression -> expression.accept(this, context))
                        .anyMatch(result -> result != null && result);
            }

            @Override
            public Boolean visitInputReference(InputReferenceExpression reference, Void context)
            {
                return false;
            }

            @Override
            public Boolean visitConstant(ConstantExpression literal, Void context)
            {
                return false;
            }

            @Override
            public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
            {
                return lambda.body().accept(this, context);
            }

            @Override
            public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
            {
                return false;
            }
        };
        return expression.accept(visitor, null);
    }

    private static class BatchFunctionToVariableReferenceRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final List<RowExpression> batchExpressions = new ArrayList<>();

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (call.resolvedFunction().functionKind() == BATCH) {
                batchExpressions.add(call);
                return new VariableReferenceExpression("batch_output_" + (batchExpressions.size() - 1), call.type());
            }
            return new CallExpression(
                    call.resolvedFunction(),
                    call.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            return new SpecialForm(
                    specialForm.form(),
                    specialForm.type(),
                    specialForm.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()),
                    specialForm.functionDependencies());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(
                    lambda.arguments(),
                    lambda.body().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        private List<RowExpression> getBatchExpressions()
        {
            return ImmutableList.copyOf(batchExpressions);
        }
    }

    private static class NonBatchInputReferenceCounter
            implements RowExpressionVisitor<Void, Void>
    {
        private final Set<Integer> nonBatchInputChannels = new HashSet<>();

        @Override
        public Void visitInputReference(InputReferenceExpression reference, Void context)
        {
            nonBatchInputChannels.add(reference.field());
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Void context)
        {
            call.arguments().forEach(expression -> expression.accept(this, context));
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, Void context)
        {
            specialForm.arguments().forEach(expression -> expression.accept(this, context));
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, Void context)
        {
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            lambda.body().accept(this, context);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return null;
        }

        private int getNonBatchInputChannelsCount()
        {
            return nonBatchInputChannels.size();
        }
    }

    private static class BatchVariableReferenceToInputReferenceRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final int nonBatchInputChannelsCount;

        public BatchVariableReferenceToInputReferenceRewriter(int nonBatchInputChannelsCount)
        {
            this.nonBatchInputChannelsCount = nonBatchInputChannelsCount;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(
                    call.resolvedFunction(),
                    call.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            return new SpecialForm(
                    specialForm.form(),
                    specialForm.type(),
                    specialForm.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()),
                    specialForm.functionDependencies());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(
                    lambda.arguments(),
                    lambda.body().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            String name = reference.name();
            if (!name.startsWith("batch_output_")) {
                // Not a batch output variable, return as is
                return reference;
            }
            int index = Integer.parseInt(name.substring(name.lastIndexOf('_') + 1));
            return new InputReferenceExpression(nonBatchInputChannelsCount + index, reference.type());
        }
    }

    public record Result(RowExpression rewrittenExpression, List<RowExpression> batchExpressions) {}
}
