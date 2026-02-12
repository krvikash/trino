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
package io.trino.sql.planner.optimizations;

import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isPartialLimitHintEnabled;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer pushes partial limit hints to table scans.
 * When a LimitNode is encountered, it traverses down through
 * Exchange, Filter, and Project nodes, applying the limit hint to all
 * table scans found. The hint is dropped if any other node type is encountered.
 */
public final class ApplyPartialLimitHint
        implements PlanOptimizer
{
    private final Metadata metadata;

    public ApplyPartialLimitHint(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        requireNonNull(plan, "plan is null");

        if (!isPartialLimitHintEnabled(context.session())) {
            return plan;
        }

        return plan.accept(new Visitor(context), Optional.empty());
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Optional<Long>>
    {
        private final Context context;

        public Visitor(Context context)
        {
            this.context = requireNonNull(context, "context is null");
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Optional<Long> limitHint)
        {
            if (!node.isPartial()) {
                return propagateLimitHint(node, Optional.empty());
            }

            long currentLimit = node.getCount();
            // If there's already a limit hint, take the minimum
            long effectiveLimit = limitHint.map(existing -> min(existing, currentLimit)).orElse(currentLimit);
            return propagateLimitHint(node, Optional.of(effectiveLimit));
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, Optional<Long> limitHint)
        {
            return propagateLimitHint(node, limitHint);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Optional<Long> limitHint)
        {
            return propagateLimitHint(node, limitHint);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Optional<Long> limitHint)
        {
            return propagateLimitHint(node, limitHint);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Optional<Long> limitHint)
        {
            // Apply the limit hint to the table scan if present
            if (limitHint.isPresent()) {
                Optional<TableHandle> newTableHandle = metadata.applyPartialLimit(context.session(), node.getTable(), limitHint.get());
                if (newTableHandle.isPresent()) {
                    return node.withTableHandle(newTableHandle.get());
                }
            }
            return node;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Optional<Long> limitHint)
        {
            // For any other node type, stop propagating the limit hint
            // but still visit children without the hint
            return propagateLimitHint(node, Optional.empty());
        }

        private PlanNode propagateLimitHint(PlanNode node, Optional<Long> limitHint)
        {
            List<PlanNode> newSources = new ArrayList<>(node.getSources().size());
            boolean changed = false;
            for (PlanNode source : node.getSources()) {
                PlanNode newSource = source.accept(this, limitHint);
                newSources.add(newSource);
                if (newSource != source) {
                    changed = true;
                }
            }
            if (changed) {
                return node.replaceChildren(newSources);
            }
            return node;
        }
    }
}
