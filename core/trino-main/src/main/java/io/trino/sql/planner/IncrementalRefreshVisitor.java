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
package io.trino.sql.planner;

import io.trino.spi.RefreshType;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Set;

import static io.trino.spi.RefreshType.FULL;
import static io.trino.spi.RefreshType.INCREMENTAL;
import static io.trino.spi.RefreshType.INCREMENTAL_COLUMN;

public class IncrementalRefreshVisitor
        extends PlanVisitor<Boolean, Void>
{
    private static final Set<Class<? extends PlanNode>> INCREMENTALLY_REFRESHABLE_NODES = Set.of(TableScanNode.class, FilterNode.class, ProjectNode.class);

    public static RefreshType canIncrementallyRefresh(PlanNode root)
    {
        Boolean canIncrementallyRefresh = new IncrementalRefreshVisitor().visitPlan(root, null);
        return canIncrementallyRefresh ? INCREMENTAL : FULL;
    }

    /**
     * Variant used when the MV has a user-set {@code incremental_column} property. The engine has
     * already wrapped the source query with a {@code col > (SELECT max(col) FROM mv)} predicate, so
     * we force {@link RefreshType#INCREMENTAL_COLUMN} instead of consulting the plan-shape heuristic.
     * Joins and multiple sources are fine; aggregations/GROUP BY are rejected earlier, at CREATE time.
     */
    public static RefreshType canIncrementallyRefresh(PlanNode root, boolean hasIncrementalColumn)
    {
        if (hasIncrementalColumn) {
            return INCREMENTAL_COLUMN;
        }
        return canIncrementallyRefresh(root);
    }

    @Override
    protected Boolean visitPlan(PlanNode node, Void context)
    {
        if (!INCREMENTALLY_REFRESHABLE_NODES.contains(node.getClass())) {
            return false;
        }
        return node.getSources().stream().allMatch(source -> source.accept(this, null));
    }
}
