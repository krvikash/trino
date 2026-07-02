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
package io.trino.sql.analyzer;

import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.Union;

import java.util.function.BiFunction;

import static io.trino.sql.analyzer.DeterminismEvaluator.containsCurrentTimeFunctions;

public class MaterializedViewEvaluator
{
    private MaterializedViewEvaluator() {}

    public static boolean isDeterministicForMaterializedView(Analysis analysis, Node node)
    {
        return analysis.getResolvedFunctions().stream()
                .allMatch(ResolvedFunction::deterministic)
                && !containsCurrentTimeFunctions(node);
    }

    /**
     * Rejects query constructs that make row-level incremental refresh produce wrong results.
     * These are non-monotonic: adding source rows can change or remove existing output rows,
     * which an append-only refresh (insert rows above the incremental_column checkpoint) cannot
     * represent. Aggregations without GROUP BY and non-deterministic functions are validated
     * separately at CREATE time — they require full analysis and may be hidden behind referenced
     * views; this covers the remaining syntactic constructs and runs on both CREATE and REFRESH.
     */
    public static void validateIncrementalColumnSupportedConstructs(Query query, BiFunction<Node, String, TrinoException> errorCreator)
    {
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitQuerySpecification(QuerySpecification node, Void context)
            {
                if (node.getSelect().isDistinct()) {
                    throw errorCreator.apply(node.getSelect(), "DISTINCT");
                }
                if (node.getGroupBy().isPresent()) {
                    throw errorCreator.apply(node.getGroupBy().get(), "aggregations or GROUP BY");
                }
                if (node.getLimit().isPresent()) {
                    throw errorCreator.apply(node.getLimit().get(), "LIMIT or FETCH");
                }
                if (node.getOffset().isPresent()) {
                    throw errorCreator.apply(node.getOffset().get(), "OFFSET");
                }
                return super.visitQuerySpecification(node, context);
            }

            @Override
            protected Void visitQuery(Query node, Void context)
            {
                if (node.getLimit().isPresent()) {
                    throw errorCreator.apply(node.getLimit().get(), "LIMIT or FETCH");
                }
                if (node.getOffset().isPresent()) {
                    throw errorCreator.apply(node.getOffset().get(), "OFFSET");
                }
                return super.visitQuery(node, context);
            }

            @Override
            protected Void visitSetOperation(SetOperation node, Void context)
            {
                // UNION ALL is monotonic (appending rows to a branch only adds output rows), so it
                // is safe. UNION (distinct), INTERSECT and EXCEPT can dedup or remove existing
                // output rows, which an append-only refresh cannot represent.
                if (node instanceof Union union && !union.isDistinct()) {
                    return super.visitSetOperation(node, context);
                }
                throw errorCreator.apply(node, "INTERSECT, EXCEPT or UNION DISTINCT");
            }

            @Override
            protected Void visitFunctionCall(FunctionCall node, Void context)
            {
                if (node.getWindow().isPresent()) {
                    throw errorCreator.apply(node, "window functions");
                }
                return super.visitFunctionCall(node, context);
            }
        }.process(query, null);
    }
}
