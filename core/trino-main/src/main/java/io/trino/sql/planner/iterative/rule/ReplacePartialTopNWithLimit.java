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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.LocalProperties;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.topN;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public class ReplacePartialTopNWithLimit
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN()
            .matching(topN -> topN.getStep() == PARTIAL);

    private final PlannerContext plannerContext;

    public ReplacePartialTopNWithLimit(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode topN, Captures captures, Context context)
    {
        List<LocalProperty<Symbol>> desiredProperties = topN.getOrderingScheme().toLocalProperties();

        PlanNode child = context.getLookup().resolve(topN.getSource());
        SortedScanRewriter rewriter = new SortedScanRewriter(
                plannerContext.getMetadata(),
                context.getLookup(),
                context.getSession(),
                context.getIdAllocator(),
                topN.getCount());
        Optional<RewriteResult> rewriteResult = child.accept(rewriter, desiredProperties);

        if (rewriteResult.isEmpty()) {
            return Result.empty();
        }

        LimitNode limitNode = new LimitNode(
                context.getIdAllocator().getNextId(),
                rewriteResult.get().node(),
                topN.getCount(),
                Optional.empty(),
                true,
                topN.getOrderingScheme().orderBy());

        if (rewriteResult.get().retainOriginalPlan()) {
            return Result.ofNodeAlternatives(Optional.empty(), ImmutableList.of(limitNode));
        }
        return Result.ofPlanNode(limitNode);
    }

    private record RewriteResult(PlanNode node, boolean retainOriginalPlan) {}

    private static class SortedScanRewriter
            extends PlanVisitor<Optional<RewriteResult>, List<LocalProperty<Symbol>>>
    {
        private final Metadata metadata;
        private final Lookup lookup;
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final long count;

        private SortedScanRewriter(
                Metadata metadata,
                Lookup lookup,
                Session session,
                PlanNodeIdAllocator idAllocator,
                long count)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.count = count;
        }

        @Override
        protected Optional<RewriteResult> visitPlan(PlanNode node, List<LocalProperty<Symbol>> desiredProperties)
        {
            return Optional.empty();
        }

        @Override
        public Optional<RewriteResult> visitGroupReference(GroupReference node, List<LocalProperty<Symbol>> desiredProperties)
        {
            PlanNode resolved = lookup.resolve(node);
            return resolved.accept(this, desiredProperties);
        }

        @Override
        public Optional<RewriteResult> visitFilter(FilterNode node, List<LocalProperty<Symbol>> desiredProperties)
        {
            return node.getSource().accept(this, desiredProperties)
                    .map(sourceResult -> new RewriteResult(
                            new FilterNode(idAllocator.getNextId(), sourceResult.node(), node.getPredicate()),
                            sourceResult.retainOriginalPlan()));
        }

        @Override
        public Optional<RewriteResult> visitLimit(LimitNode node, List<LocalProperty<Symbol>> desiredProperties)
        {
            return node.getSource().accept(this, desiredProperties)
                    .map(sourceResult -> new RewriteResult(
                            new LimitNode(
                                    idAllocator.getNextId(),
                                    sourceResult.node(),
                                    node.getCount(),
                                    node.getTiesResolvingScheme(),
                                    node.isPartial(),
                                    node.getPreSortedInputs()),
                            sourceResult.retainOriginalPlan()));
        }

        @Override
        public Optional<RewriteResult> visitProject(ProjectNode node, List<LocalProperty<Symbol>> desiredProperties)
        {
            // Translate desired sorting properties through identity assignments (output -> input symbol).
            // Bail out if any desired property references a non-pass-through expression.
            Map<Symbol, Symbol> outputToInput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                if (assignment.getValue() instanceof Reference) {
                    outputToInput.put(assignment.getKey(), Symbol.from(assignment.getValue()));
                }
            }

            List<LocalProperty<Symbol>> translatedProperties = LocalProperties.translate(
                    desiredProperties, col -> Optional.ofNullable(outputToInput.get(col)));
            if (translatedProperties.size() < desiredProperties.size()) {
                return Optional.empty();
            }

            return node.getSource().accept(this, translatedProperties)
                    .map(sourceResult -> new RewriteResult(
                            new ProjectNode(idAllocator.getNextId(), sourceResult.node(), node.getAssignments()),
                            sourceResult.retainOriginalPlan()));
        }

        @Override
        public Optional<RewriteResult> visitTableScan(TableScanNode node, List<LocalProperty<Symbol>> desiredProperties)
        {
            Map<Symbol, ColumnHandle> assignments = node.getAssignments();

            List<SortingProperty<ColumnHandle>> sortProperties = new ArrayList<>();
            for (LocalProperty<Symbol> property : desiredProperties) {
                if (property instanceof SortingProperty<Symbol> sortingProperty) {
                    ColumnHandle columnHandle = assignments.get(sortingProperty.getColumn());
                    if (columnHandle == null) {
                        return Optional.empty();
                    }
                    sortProperties.add(new SortingProperty<>(columnHandle, sortingProperty.getOrder()));
                }
            }

            return metadata.applyPartialTopN(
                            session,
                            node.getTable(),
                            sortProperties,
                            count)
                    .map(result -> new RewriteResult(
                            new TableScanNode(
                                    idAllocator.getNextId(),
                                    result.alternative(),
                                    node.getOutputSymbols(),
                                    node.getAssignments(),
                                    node.getEnforcedConstraint(),
                                    node.getStatistics(),
                                    node.isUpdateTarget(),
                                    node.getUseConnectorNodePartitioning()),
                            result.retainOriginalPlan()));
        }
    }
}
