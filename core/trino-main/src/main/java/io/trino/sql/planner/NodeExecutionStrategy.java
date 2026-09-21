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

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.metadata.Metadata;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.getMinInputRowsPerTask;
import static io.trino.SystemSessionProperties.getMinInputSizePerTask;
import static io.trino.SystemSessionProperties.getQueryDistributionMode;
import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public interface NodeExecutionStrategy
{
    NodeExecutionStrategy DISTRIBUTED_EXECUTION = (_, _, _) -> false;
    NodeExecutionStrategy SINGLE_NODE_EXECUTION = (_, _, _) -> true;

    boolean shouldBeExecutedOnSingleNode(Session session, PlanNode plan, TableStatsProvider tableStatsProvider);

    class DynamicNodeExecutionStrategy
            implements NodeExecutionStrategy
    {
        private final Metadata metadata;
        private final StatsCalculator statsCalculator;

        @Inject
        public DynamicNodeExecutionStrategy(Metadata metadata, StatsCalculator statsCalculator)
        {
            this.metadata = metadata;
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        }

        @Override
        public boolean shouldBeExecutedOnSingleNode(Session session, PlanNode plan, TableStatsProvider tableStatsProvider)
        {
            checkState(!containsRemoteExchange(plan), "Single node execution must be decided before remote exchanges are added");
            return switch (getQueryDistributionMode(session)) {
                case DISTRIBUTED -> false;
                case SINGLE_NODE -> supportsSingleNodeExecution(session, tableScans(plan));
                // fault-tolerant execution schedules and retries stages independently
                // TODO: https://starburstdata.atlassian.net/browse/ENG-28273 single node execution with task retries
                case AUTOMATIC -> getRetryPolicy(session) != RetryPolicy.TASK
                        // cheapest checks first: skip the stats-based fitsOnOneNode() unless retry policy and
                        // connector support already allow single-node execution
                        && supportsSingleNodeExecution(session, tableScans(plan))
                        && fitsOnOneNode(session, plan, tableStatsProvider);
            };
        }

        private boolean supportsSingleNodeExecution(Session session, List<TableScanNode> scans)
        {
            return scans.stream()
                    .map(TableScanNode::getTable)
                    .allMatch(handle -> metadata.supportsSingleNodeExecution(session, handle));
        }

        private static List<TableScanNode> tableScans(PlanNode root)
        {
            return searchFrom(root).where(TableScanNode.class::isInstance).findAll().stream()
                    .map(TableScanNode.class::cast)
                    .toList();
        }

        /**
         * The query fits on one node when its estimated input, and the estimated output of every plan node, is below
         * {@code min_input_size_per_task} and below half of the per-node query memory limit, and when every plan node
         * is estimated at fewer rows than {@code min_input_rows_per_task} or at no more rows than its table scans read.
         */
        private boolean fitsOnOneNode(Session session, PlanNode root, TableStatsProvider tableStatsProvider)
        {
            long maxInputBytes = min(getMinInputSizePerTask(session).toBytes(), getQueryMaxMemoryPerNode(session).toBytes() / 2);
            long maxRows = getMinInputRowsPerTask(session);
            if (maxInputBytes == 0 || maxRows == 0) {
                // a zero bound disables the decision
                return false;
            }
            List<PlanNode> sources = searchFrom(root).where(node -> node.getSources().isEmpty()).findAll();
            if (!sources.stream().allMatch(TableScanNode.class::isInstance)) {
                // only table scans are estimated; values and table functions keep the query distributed.
                // TODO: https://starburstdata.atlassian.net/browse/ENG-27703 estimate VALUES sources
                // TODO: https://starburstdata.atlassian.net/browse/ENG-28217 estimate table function sources
                return false;
            }
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, tableStatsProvider);
            List<TableScanNode> scans = sources.stream().map(TableScanNode.class::cast).toList();
            if (!inputFitsBound(scans, statsProvider, maxInputBytes)) {
                return false;
            }
            double inputRows = scans.stream().mapToDouble(scan -> statsProvider.getStats(scan).getOutputRowCount()).sum();
            // a node may output more than it reads. OutputNode and ExplainAnalyzeNode run on the coordinator and
            // add no data of their own, so skipping them plans the body of EXPLAIN ANALYZE the way the query alone
            // is planned
            return searchFrom(root)
                    .where(node -> !(node instanceof OutputNode) && !(node instanceof ExplainAnalyzeNode))
                    .findAll().stream()
                    .allMatch(node -> outputFitsBound(statsProvider.getStats(node), node, maxInputBytes, maxRows, inputRows));
        }

        private static boolean containsRemoteExchange(PlanNode root)
        {
            return searchFrom(root)
                    .where(node -> node instanceof ExchangeNode exchange && exchange.getScope() == REMOTE)
                    .matches();
        }

        /**
         * Whether the estimated bytes produced by the plan's table scans are below the bound. An unknown row count
         * keeps the query distributed; a column without a size statistic is estimated at the default width, as in
         * every other cost-based decision.
         */
        private static boolean inputFitsBound(List<TableScanNode> scans, StatsProvider statsProvider, long maxInputBytes)
        {
            double inputBytes = 0;
            for (TableScanNode scan : scans) {
                PlanNodeStatsEstimate stats = statsProvider.getStats(scan);
                if (stats.isOutputRowCountUnknown()) {
                    return false;
                }
                inputBytes += stats.getOutputSizeInBytes(scan.getOutputSymbols());
                if (inputBytes >= maxInputBytes) {
                    return false;
                }
            }
            return true;
        }

        /**
         * An unknown row count keeps the query distributed. The row bound of
         * {@link io.trino.sql.planner.optimizations.DeterminePartitionCount} catches a fan-out the size estimate
         * misses: a node with no output symbols, as under {@code count(*)}, is estimated at zero bytes whatever its
         * row count. A node that outputs no more rows than the scans read is no fan-out, so a query over one large
         * table is held to its size alone.
         */
        private static boolean outputFitsBound(PlanNodeStatsEstimate stats, PlanNode node, long maxInputBytes, long maxRows, double inputRows)
        {
            return !stats.isOutputRowCountUnknown()
                    && (stats.getOutputRowCount() < maxRows || stats.getOutputRowCount() <= inputRows)
                    && stats.getOutputSizeInBytes(node.getOutputSymbols()) < maxInputBytes;
        }
    }
}
