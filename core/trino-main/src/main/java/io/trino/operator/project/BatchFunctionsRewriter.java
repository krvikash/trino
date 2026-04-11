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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.FunctionKind.BATCH;

public final class BatchFunctionsRewriter
{
    // Use a non-identifier character ('$') so the synthetic namespace can never collide with a user Symbol.
    public static final String BATCH_OUTPUT_PREFIX = "$batch_output_";

    private BatchFunctionsRewriter() {}

    /**
     * Rewrites batch function calls in the given expression to references named "$batch_output_N".
     * Collects the original batch function call expressions in a list, in the order they were
     * encountered (pre-order traversal).
     */
    public static Result rewriteBatchFunctionsToVariableReferences(Expression expression)
    {
        List<Expression> batchExpressions = new ArrayList<>();
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.function().functionKind() == BATCH) {
                    batchExpressions.add(node);
                    return new Reference(node.type(), BATCH_OUTPUT_PREFIX + (batchExpressions.size() - 1));
                }
                return null;
            }
        }, expression);
        return new Result(rewritten, ImmutableList.copyOf(batchExpressions));
    }

    public static boolean containsBatchFunction(Expression expression)
    {
        AtomicBoolean found = new AtomicBoolean(false);
        new DefaultTraversalVisitor<AtomicBoolean>()
        {
            @Override
            protected Void visitCall(Call node, AtomicBoolean found)
            {
                if (node.function().functionKind() == BATCH) {
                    found.set(true);
                    return null;
                }
                return super.visitCall(node, found);
            }
        }.process(expression, found);
        return found.get();
    }

    public record Result(Expression rewrittenExpression, List<Expression> batchExpressions) {}

    /**
     * Builds the layout used to compile the rewritten projection produced by
     * {@link #rewriteBatchFunctionsToVariableReferences(Expression)}.
     * <p>
     * The compact layout places the non-batch symbols referenced by the rewritten expression
     * at positions {@code 0..K-1} (in the order they are encountered in a pre-order traversal), followed by
     * {@code $batch_output_N} placeholders at positions {@code K..K+M-1}. This matches the
     * runtime page assembled by {@link ScalarProjectionOverBatchFunctions}, which puts the
     * non-batch input blocks first and appends the batch outputs.
     * <p>
     * The returned {@link Layout#inputChannels()} maps the non-batch symbols back to their channel
     * positions in the caller's source page using {@code originalLayout}.
     */
    public static Layout buildBatchOutputLayout(Expression rewrittenExpression, Map<Symbol, Integer> originalLayout, List<Expression> batchExpressions)
    {
        Set<Symbol> nonBatchSymbols = new LinkedHashSet<>();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitReference(Reference node, Void context)
            {
                if (node.name().startsWith(BATCH_OUTPUT_PREFIX)) {
                    return null;
                }
                Symbol symbol = Symbol.from(node);
                if (originalLayout.containsKey(symbol)) {
                    nonBatchSymbols.add(symbol);
                }
                return null;
            }
        }.process(rewrittenExpression, null);

        int nonBatchSymbolsCount = nonBatchSymbols.size();
        ImmutableMap.Builder<Symbol, Integer> compactLayoutBuilder = ImmutableMap.builder();
        int channel = 0;
        for (Symbol symbol : nonBatchSymbols) {
            compactLayoutBuilder.put(symbol, channel++);
        }
        for (int i = 0; i < batchExpressions.size(); i++) {
            compactLayoutBuilder.put(new Symbol(batchExpressions.get(i).type(), BATCH_OUTPUT_PREFIX + i), nonBatchSymbolsCount + i);
        }

        InputChannels inputChannels = new InputChannels(nonBatchSymbols.stream()
                .map(originalLayout::get)
                .collect(toImmutableList()));

        return new Layout(compactLayoutBuilder.buildOrThrow(), inputChannels);
    }

    public record Layout(Map<Symbol, Integer> compactLayout, InputChannels inputChannels) {}
}
