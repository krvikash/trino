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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.execution.Column;
import io.trino.execution.Input;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInputExtractor
        extends BasePlanTest
{
    @Test
    public void testSingleScan()
    {
        assertColumnsBySymbolNameMatchScanAssignments("SELECT orderkey, custkey FROM orders");
    }

    @Test
    public void testMultipleScans()
    {
        assertColumnsBySymbolNameMatchScanAssignments("SELECT o.orderkey, c.name FROM orders o JOIN customer c ON o.custkey = c.custkey");
    }

    @Test
    public void testSameTableScannedTwice()
    {
        assertColumnsBySymbolNameMatchScanAssignments("SELECT l.orderkey FROM orders l JOIN orders r ON l.custkey = r.orderkey");
    }

    /**
     * Every {@link Input} must carry the plan-time mapping from the symbol names produced by the corresponding
     * scan to the columns those symbols read, so a completed {@code QueryInfo} can be interpreted without
     * resolving column metadata again.
     */
    private void assertColumnsBySymbolNameMatchScanAssignments(@Language("SQL") String sql)
    {
        PlanTester planTester = getPlanTester();
        planTester.inTransaction(session -> {
            Plan plan = planTester.createPlan(
                    session,
                    sql,
                    planTester.getPlanOptimizers(),
                    planTester.getAlternativeOptimizers(),
                    OPTIMIZED_AND_VALIDATED,
                    false,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector());
            SubPlan subPlan = planTester.createSubPlans(session, plan, false);
            List<Input> inputs = new InputExtractor(planTester.getPlannerContext().getMetadata(), session).extractInputs(subPlan);

            ImmutableMap.Builder<ScanLocation, Map<String, Column>> expected = ImmutableMap.builder();
            for (PlanFragment fragment : subPlan.getAllFragments()) {
                List<PlanNode> scans = PlanNodeSearcher.searchFrom(fragment.getRoot())
                        .whereIsInstanceOfAny(TableScanNode.class)
                        .findAll();
                for (PlanNode scan : scans) {
                    expected.put(
                            new ScanLocation(fragment.getId(), scan.getId()),
                            ((TableScanNode) scan).getAssignments().entrySet().stream()
                                    .collect(toImmutableMap(assignment -> assignment.getKey().name(), assignment -> toColumn(assignment.getValue()))));
                }
            }

            Map<ScanLocation, Map<String, Column>> actual = inputs.stream()
                    .collect(toImmutableMap(input -> new ScanLocation(input.fragmentId(), input.planNodeId()), Input::columnsBySymbolName));

            assertThat(actual)
                    .isNotEmpty()
                    .isEqualTo(expected.buildOrThrow());

            for (Input input : inputs) {
                assertThat(input.columns()).containsExactlyInAnyOrderElementsOf(ImmutableSet.copyOf(input.columnsBySymbolName().values()));
            }
            return null;
        });
    }

    private static Column toColumn(ColumnHandle columnHandle)
    {
        TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
        return new Column(tpchColumnHandle.columnName(), tpchColumnHandle.type().toString());
    }

    private record ScanLocation(PlanFragmentId fragmentId, PlanNodeId planNodeId) {}
}
