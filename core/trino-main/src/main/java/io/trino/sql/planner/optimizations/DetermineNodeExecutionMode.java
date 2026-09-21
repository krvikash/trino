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

import io.trino.sql.planner.NodeExecutionStrategy;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

/// Decides whether the query executes on a single node, and hands the decision to the optimizers that follow through
/// their [Context]. It runs just before [AddExchanges]: by then columns are pruned and predicates pushed down, and
/// the exchanges that depend on the answer have not been added yet.
public final class DetermineNodeExecutionMode
        implements PlanOptimizer
{
    private final NodeExecutionStrategy nodeExecutionStrategy;

    public DetermineNodeExecutionMode(NodeExecutionStrategy nodeExecutionStrategy)
    {
        this.nodeExecutionStrategy = requireNonNull(nodeExecutionStrategy, "nodeExecutionStrategy is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        return plan;
    }

    @Override
    public Context deriveNextContext(PlanNode plan, Context context)
    {
        if (context.singleNodeDecision().isPresent()) {
            throw new IllegalStateException("Node execution mode is already decided");
        }
        return context.withSingleNodeDecision(nodeExecutionStrategy.shouldBeExecutedOnSingleNode(context.session(), plan, context.tableStatsProvider()));
    }
}
