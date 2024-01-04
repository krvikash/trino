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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TaskTestUtils;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.node.InternalNode;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.AllSystemAccessControlsSelector;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.LocationAccessControl;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.transaction.TransactionManager;
import io.trino.util.Failures;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.SqlStage.createSqlStage;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStageRegistry
{
    // stage creation requires a fragment without bucket to partition mapping
    private static final PlanFragment PLAN_FRAGMENT = TaskTestUtils.PLAN_FRAGMENT.withOutputPartitioning(Optional.empty(), OptionalInt.empty());

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        executor = newSingleThreadExecutor(daemonThreadsNamed("test-stage-registry-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-stage-registry-scheduled-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testInitializeRestartAllowsReAddingStageId()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        SubPlan plan = new SubPlan(PLAN_FRAGMENT, ImmutableList.of());
        EventDrivenFaultTolerantQueryScheduler.StageRegistry registry = new EventDrivenFaultTolerantQueryScheduler.StageRegistry(stateMachine, plan);

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStage stage = createStage(stageId);
        registry.add(stage);

        assertThatThrownBy(() -> registry.add(stage))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining(stageId.toString());

        registry.initializeRestart(plan, Failures.toFailure(new RuntimeException("restart")));
        // After initializeRestart, adding the same stage id must succeed
        registry.add(stage);
    }

    @Test
    public void testInitializeRestartClearsAllStages()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        SubPlan plan = new SubPlan(PLAN_FRAGMENT, ImmutableList.of());
        EventDrivenFaultTolerantQueryScheduler.StageRegistry registry = new EventDrivenFaultTolerantQueryScheduler.StageRegistry(stateMachine, plan);

        for (int i = 0; i < 3; i++) {
            registry.add(createStage(new StageId(new QueryId("query"), i)));
        }

        registry.initializeRestart(plan, Failures.toFailure(new RuntimeException("restart")));

        // All three stage ids can be re-added after initializeRestart
        for (int i = 0; i < 3; i++) {
            registry.add(createStage(new StageId(new QueryId("query"), i)));
        }
    }

    @Test
    public void testFailTaskRemotelyIgnoresTaskOfDiscardedAttempt()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        SubPlan plan = new SubPlan(PLAN_FRAGMENT, ImmutableList.of());
        EventDrivenFaultTolerantQueryScheduler.StageRegistry registry = new EventDrivenFaultTolerantQueryScheduler.StageRegistry(stateMachine, plan);

        SqlStage stage = createStage(new StageId(new QueryId("query"), 0));
        registry.add(stage);
        RemoteTask task = createTask(stage);
        registry.initializeRestart(plan, Failures.toFailure(new RuntimeException("restart")));

        // failTaskRemotely is reachable from outside the scheduler, so a task of the discarded attempt can report
        // a failure after the restart
        registry.failTaskRemotely(task.getTaskId(), new RuntimeException("remote failure"));
    }

    private static RemoteTask createTask(SqlStage stage)
    {
        InternalNode node = new InternalNode("node", URI.create("http://node"), NodeVersion.UNKNOWN, false);
        return stage.createTask(
                        node,
                        0,
                        0,
                        Optional.empty(),
                        OptionalInt.empty(),
                        PipelinedOutputBuffers.createInitial(ARBITRARY),
                        ImmutableListMultimap.of(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        false)
                .orElseThrow();
    }

    private SqlStage createStage(StageId stageId)
    {
        return createSqlStage(
                dummyMetadata(),
                stageId,
                PLAN_FRAGMENT,
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                new NodeTaskMap(new FinalizerService()),
                executor,
                Tracing.noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats(),
                (_, _) -> OptionalInt.empty());
    }

    private QueryStateMachine createQueryStateMachine()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControl = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME,
                LocationAccessControl.DEFAULT_NAME,
                new AllSystemAccessControlsSelector());
        accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        return QueryStateMachine.begin(
                Optional.empty(),
                "SELECT 1",
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://fake"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                dummyMetadata(),
                NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, Duration.ofMillis(1)),
                Optional.empty(),
                false,
                Optional.empty(),
                new NodeVersion("test"));
    }
}
