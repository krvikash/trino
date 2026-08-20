/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.SessionTestUtils;
import io.trino.connector.CatalogHandle;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.Column;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StageStats;
import io.trino.execution.StagesInfo;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TaskStats;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ProfilerFixtures
{
    public static final String INPUT_ROWS_DISTRIBUTION_METRIC = "Input rows distribution";

    private static final QueryId QUERY_ID = new QueryId("query_profiler_test");
    private static final PlanFragmentId FRAGMENT_ID = new PlanFragmentId("0");
    private static final StageId STAGE_ID = StageId.create(QUERY_ID, FRAGMENT_ID);
    private static final PlanFragmentId CHILD_FRAGMENT_ID = new PlanFragmentId("1");
    private static final StageId CHILD_STAGE_ID = StageId.create(QUERY_ID, CHILD_FRAGMENT_ID);
    private static final CatalogHandle CATALOG_HANDLE = createTestCatalogHandle("test_catalog");
    private static final Duration ZERO_DURATION = new Duration(0, NANOSECONDS);
    private static final Duration ONE_SECOND = new Duration(1, SECONDS);
    private static final DataSize ZERO_BYTES = DataSize.ofBytes(0);

    private ProfilerFixtures() {}

    /**
     * Connector table handle whose class name ends in {@code JdbcTableHandle}.
     */
    public static final class TestingJdbcTableHandle
            implements ConnectorTableHandle {}

    /**
     * Connector table handle whose class name ends in {@code HiveTableHandle}.
     */
    public static final class TestingHiveTableHandle
            implements ConnectorTableHandle {}

    /**
     * Mirrors the record components the Hive rule reads reflectively from {@code connectorInfo}.
     */
    public record TestingHiveConnectorInfo(String tableDefaultFileFormat) {}

    // ----- plan nodes -----

    public static TableScanNode jdbcScan(String id)
    {
        return scan(id, new TestingJdbcTableHandle());
    }

    public static TableScanNode jdbcScan(String id, CatalogHandle catalogHandle)
    {
        return scan(id, catalogHandle, new TestingJdbcTableHandle());
    }

    public static TableScanNode hiveScan(String id)
    {
        return scan(id, new TestingHiveTableHandle());
    }

    public static TableScanNode scan(String id, ConnectorTableHandle connectorHandle)
    {
        return scan(id, CATALOG_HANDLE, connectorHandle);
    }

    public static TableScanNode scan(String id, CatalogHandle catalogHandle, ConnectorTableHandle connectorHandle)
    {
        Symbol symbol = new Symbol(BIGINT, "s_" + id);
        ColumnHandle column = new TestingColumnHandle("col_" + id);
        TableHandle tableHandle = new TableHandle(catalogHandle, connectorHandle, TestingTransactionHandle.create());
        return new TableScanNode(
                new PlanNodeId(id),
                tableHandle,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, column),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static FilterNode filter(String id, PlanNode source)
    {
        return new FilterNode(new PlanNodeId(id), source, new Constant(BOOLEAN, true));
    }

    public static ProjectNode nonIdentityProject(String id, PlanNode source)
    {
        Assignments assignments = Assignments.builder()
                .put(new Symbol(BIGINT, "p_" + id), new Constant(BIGINT, 1L))
                .build();
        return new ProjectNode(new PlanNodeId(id), source, assignments);
    }

    public static JoinNode join(String id, PlanNode left, PlanNode right)
    {
        return new JoinNode(
                new PlanNodeId(id),
                JoinType.INNER,
                left,
                right,
                ImmutableList.of(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    // ----- inputs / tables -----

    public static Input input(String planNodeId, Object connectorInfo)
    {
        return new Input(
                Optional.of("test_connector"),
                "test_catalog",
                new CatalogVersion("test"),
                "test_schema",
                "test_table",
                Optional.ofNullable(connectorInfo),
                ImmutableList.of(new Column("col", "bigint")),
                ImmutableMap.of("col", new Column("col", "bigint")),
                FRAGMENT_ID,
                new PlanNodeId(planNodeId));
    }

    public static Map<PlanNodeId, TableInfo> tables(String planNodeId, String tableName)
    {
        return ImmutableMap.of(
                new PlanNodeId(planNodeId), new TableInfo(
                        Optional.of("test_connector"),
                        new QualifiedObjectName("test_catalog", "test_schema", tableName),
                        TupleDomain.all()));
    }

    // ----- stats -----

    public static Metrics inputRowsDistribution(double p75)
    {
        Metric<?> snapshot = new DistributionSnapshot(1000L, 0, 0, 0, 0, 0, 0, 0, p75, 0, 0, 0);
        return new Metrics(ImmutableMap.of(INPUT_ROWS_DISTRIBUTION_METRIC, snapshot));
    }

    public static OperatorStats operator(String planNodeId, String type, long totalDrivers, DataSize physicalInput, long inputPositions, Metrics metrics)
    {
        return new OperatorStats(
                0,
                0,
                0,
                0,
                new PlanNodeId(planNodeId),
                Optional.empty(),
                type,
                totalDrivers,
                0,
                ONE_SECOND,
                ONE_SECOND,
                physicalInput,
                inputPositions,
                ZERO_DURATION,
                ZERO_BYTES,
                0,
                physicalInput,
                inputPositions,
                0.0,
                0,
                ONE_SECOND,
                ONE_SECOND,
                ZERO_BYTES,
                0,
                0,
                metrics,
                Metrics.EMPTY,
                Metrics.EMPTY,
                ZERO_BYTES,
                ZERO_DURATION,
                0,
                ONE_SECOND,
                ONE_SECOND,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                Optional.empty(),
                null);
    }

    public static OperatorStats operator(String planNodeId, String type, long totalDrivers)
    {
        return operator(planNodeId, type, totalDrivers, ZERO_BYTES, 0, Metrics.EMPTY);
    }

    public static OperatorStats smallFilesOperator(String planNodeId, long totalDrivers, double p75)
    {
        return operator(planNodeId, "TableScanOperator", totalDrivers, ZERO_BYTES, 0, inputRowsDistribution(p75));
    }

    public static OperatorStats bigScanOperator(String planNodeId, DataSize physicalInput)
    {
        return operator(planNodeId, "TableScanOperator", 1, physicalInput, 100, Metrics.EMPTY);
    }

    public static QueryStats emptyQueryStats()
    {
        return queryStats(ZERO_BYTES, ZERO_DURATION, ZERO_DURATION, ZERO_DURATION, ImmutableList.of());
    }

    public static QueryStats queryStats(
            DataSize outputDataSize,
            Duration totalCpuTime,
            Duration totalScheduledTime,
            Duration executionTime,
            List<OperatorStats> operatorSummaries)
    {
        return queryStats(outputDataSize, totalCpuTime, totalScheduledTime, executionTime, ZERO_BYTES, ZERO_BYTES, operatorSummaries);
    }

    public static QueryStats queryStats(
            DataSize outputDataSize,
            Duration totalCpuTime,
            Duration totalScheduledTime,
            Duration executionTime,
            DataSize physicalInputDataSize,
            DataSize internalNetworkInputDataSize,
            List<OperatorStats> operatorSummaries)
    {
        Instant now = Instant.now();
        return new QueryStats(
                now,
                now,
                now,
                now,
                ZERO_DURATION, // elapsedTime
                ZERO_DURATION, // queuedTime
                ZERO_DURATION, // resourceWaitingTime
                ZERO_DURATION, // dispatchingTime
                executionTime,
                ZERO_DURATION, // profilerTime
                ZERO_DURATION, // planningTime
                ZERO_DURATION, // planningCpuTime
                ZERO_DURATION, // startingTime
                ZERO_DURATION, // finishingTime
                0,
                0,
                0,
                0, // totalTasks, runningTasks, completedTasks, failedTasks
                0,
                0,
                0,
                0,
                0, // totalDrivers, queuedDrivers, runningDrivers, blockedDrivers, completedDrivers
                0.0,
                0.0, // cumulativeUserMemory, failedCumulativeUserMemory
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                false, // scheduled
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                totalScheduledTime,
                ZERO_DURATION, // failedScheduledTime
                totalCpuTime,
                ZERO_DURATION, // failedCpuTime
                ZERO_DURATION, // totalBlockedTime
                false, // fullyBlocked
                ImmutableSet.of(),
                physicalInputDataSize,
                ZERO_BYTES,
                0,
                0, // physicalInput
                ZERO_DURATION,
                ZERO_DURATION, // physicalInputReadTime
                internalNetworkInputDataSize,
                ZERO_BYTES,
                0,
                0, // internalNetworkInput
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0, // processedInput
                ZERO_DURATION,
                ZERO_DURATION, // inputBlockedTime
                outputDataSize,
                ZERO_BYTES,
                0,
                0, // output
                ZERO_DURATION,
                ZERO_DURATION, // outputBlockedTime
                ZERO_BYTES,
                ZERO_BYTES, // physicalWrittenDataSize
                ImmutableList.of(), // stageGcStatistics
                DynamicFiltersStats.EMPTY,
                ImmutableMap.of(),
                ImmutableMap.of(),
                operatorSummaries,
                ImmutableList.of());
    }

    // ----- tasks -----

    /**
     * A task with a specific {@code processedInputDataSize}, used to fabricate stage skew.
     */
    public static TaskInfo taskWithProcessedInput(DataSize processedInput)
    {
        return taskInfo(taskStats(processedInput, ImmutableList.of()));
    }

    /**
     * A task carrying a single scan pipeline so {@code PlanNodeStatsSummarizer} attaches node stats.
     */
    public static TaskInfo taskWithScanOperator(OperatorStats operator)
    {
        PipelineStats pipeline = new PipelineStats(
                0,
                Instant.now(),
                Instant.now(),
                Instant.now(),
                true,
                true,
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                emptyDistribution(),
                emptyDistribution(),
                ZERO_DURATION,
                ZERO_DURATION,
                ZERO_DURATION,
                false,
                ImmutableSet.of(),
                operator.getPhysicalInputDataSize(),
                operator.getPhysicalInputPositions(),
                ZERO_DURATION,
                ZERO_BYTES,
                0,
                operator.getInputDataSize(),
                operator.getInputPositions(),
                ZERO_DURATION,
                ZERO_BYTES,
                0,
                ZERO_DURATION,
                ZERO_BYTES,
                ImmutableList.of(operator),
                ImmutableList.of());
        return taskInfo(taskStats(ZERO_BYTES, ImmutableList.of(pipeline)));
    }

    private static TaskInfo taskInfo(TaskStats stats)
    {
        return TaskInfo.createInitialTask(
                new TaskId(STAGE_ID, 0, 0),
                URI.create("http://localhost"),
                "node",
                false,
                Optional.empty(),
                stats);
    }

    private static TaskStats taskStats(DataSize processedInput, List<PipelineStats> pipelines)
    {
        Instant now = Instant.now();
        return new TaskStats(
                now,
                now,
                now,
                now,
                now,
                now,
                ZERO_DURATION,
                ZERO_DURATION,
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                0,
                0.0,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_DURATION,
                ZERO_DURATION,
                ZERO_DURATION,
                false,
                ImmutableSet.of(),
                ZERO_BYTES,
                0,
                ZERO_DURATION,
                ZERO_BYTES,
                0,
                processedInput,
                0,
                ZERO_DURATION,
                ZERO_BYTES,
                0,
                ZERO_DURATION,
                ZERO_BYTES,
                ZERO_BYTES,
                OptionalInt.empty(),
                0,
                ZERO_DURATION,
                pipelines);
    }

    private static Distribution.DistributionSnapshot emptyDistribution()
    {
        return new Distribution.DistributionSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    // ----- assembly -----

    public static QueryInfo queryInfo(PlanNode root)
    {
        return queryInfo(root, ImmutableList.of(), ImmutableMap.of(), emptyQueryStats(), ImmutableList.of());
    }

    public static QueryInfo queryInfo(
            PlanNode root,
            List<Input> inputs,
            Map<PlanNodeId, TableInfo> tables,
            QueryStats queryStats,
            List<TaskInfo> tasks)
    {
        StageInfo stageInfo = stageInfo(STAGE_ID, FRAGMENT_ID, root, tasks, tables, ImmutableList.of());
        StagesInfo stages = new StagesInfo(STAGE_ID, ImmutableList.of(stageInfo));
        return buildQueryInfo(queryStats, stages, ImmutableSet.copyOf(inputs));
    }

    /**
     * A query with a single non-output stage carrying one task per node id, so that
     * {@code countUniqueWorkers} resolves to the number of distinct node ids.
     */
    public static QueryInfo queryInfoWithWorkerNodes(QueryStats queryStats, List<String> workerNodeIds)
    {
        StageInfo outputStage = stageInfo(STAGE_ID, FRAGMENT_ID, jdbcScan("root"), ImmutableList.of(), ImmutableMap.of(), ImmutableList.of(CHILD_STAGE_ID));
        List<TaskInfo> workerTasks = IntStream.range(0, workerNodeIds.size())
                .mapToObj(partition -> workerTask(CHILD_STAGE_ID, partition, workerNodeIds.get(partition)))
                .collect(toImmutableList());
        StageInfo workerStage = stageInfo(CHILD_STAGE_ID, CHILD_FRAGMENT_ID, jdbcScan("child"), workerTasks, ImmutableMap.of(), ImmutableList.of());
        StagesInfo stages = new StagesInfo(STAGE_ID, ImmutableList.of(outputStage, workerStage));
        return buildQueryInfo(queryStats, stages, ImmutableSet.of());
    }

    private static StageInfo stageInfo(
            StageId stageId,
            PlanFragmentId fragmentId,
            PlanNode root,
            List<TaskInfo> tasks,
            Map<PlanNodeId, TableInfo> tables,
            List<StageId> subStages)
    {
        Symbol firstOutput = root.getOutputSymbols().getFirst();
        PlanFragment fragment = new PlanFragment(
                fragmentId,
                root,
                SymbolsExtractor.extractUnique(root),
                SINGLE_DISTRIBUTION,
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(firstOutput)),
                OptionalInt.empty(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());

        return new StageInfo(
                stageId,
                StageState.FINISHED,
                fragment,
                false,
                ImmutableList.of(BIGINT),
                StageStats.createInitial(),
                tasks,
                subStages,
                tables,
                ImmutableSetMultimap.of(),
                null);
    }

    private static TaskInfo workerTask(StageId stageId, int partition, String nodeId)
    {
        return TaskInfo.createInitialTask(
                new TaskId(stageId, partition, 0),
                URI.create("http://localhost"),
                nodeId,
                false,
                Optional.empty(),
                taskStats(ZERO_BYTES, ImmutableList.of()));
    }

    private static QueryInfo buildQueryInfo(QueryStats queryStats, StagesInfo stages, Set<Input> inputs)
    {
        return new QueryInfo(
                QUERY_ID,
                SessionTestUtils.TEST_SESSION.toSessionRepresentation(),
                QueryState.FINISHED,
                URI.create("http://localhost"),
                ImmutableList.of(),
                "SELECT 1",
                Optional.empty(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.of(stages),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.copyOf(inputs),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                true,
                Optional.empty(),
                Optional.empty(),
                RetryPolicy.NONE,
                false,
                new NodeVersion("test"));
    }
}
