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
package io.trino.split.remote;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.exchange.ExchangeManagerConfig;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.LocationFactory;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagementExecutor;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.dedicated.ThreadPerDriverTaskExecutor;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.LanguageFunctionEngineManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.WorkerLanguageFunctionProvider;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.operator.gpu.GpuConfig;
import io.trino.operator.gpu.GpuNodeSetup;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.sql.planner.CompilerConfig;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingTransactionHandle;
import io.trino.util.EmbedVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.memory.MemoryPool.newEmptyMemoryPool;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_QUEUE_FULL;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.EmbedVersion.testingVersionEmbedder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Exercises {@link RemoteSplitsTaskManager} directly (no server), with a hand-written
 * {@link RecordingSplitSource} standing in for the connector and a real, minimal
 * {@link SqlTaskManager} standing in for the worker's task infrastructure (needed only for
 * {@link SqlTaskManager#getQueryContext}).
 */
@TestInstance(PER_CLASS)
final class TestRemoteSplitsTaskManager
{
    private static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(new CatalogName("test"), new CatalogVersion("test"));
    private static final ConnectorTableHandle TABLE_HANDLE = new TestingTableHandle();
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();
    private static final Set<ColumnHandle> NO_COLUMNS = ImmutableSet.of();
    private static final DynamicFilterSnapshot NO_DYNAMIC_FILTER = new DynamicFilterSnapshot(TupleDomain.all(), true);

    private TaskExecutor taskExecutor;
    private TaskManagementExecutor taskManagementExecutor;
    private SqlTaskManager sqlTaskManager;

    @BeforeAll
    void setUp()
    {
        taskExecutor = new ThreadPerDriverTaskExecutor(
                Tracing.noopTracer(),
                testingVersionEmbedder(),
                new FairScheduler(8, "Runner-%d", Ticker.systemTicker()),
                1,
                Integer.MAX_VALUE,
                8);
        taskExecutor.start();
        taskManagementExecutor = new TaskManagementExecutor();
        sqlTaskManager = createSqlTaskManager();
    }

    @AfterAll
    void tearDown()
    {
        sqlTaskManager.close();
        taskExecutor.stop();
        taskExecutor = null;
        taskManagementExecutor.close();
        taskManagementExecutor = null;
    }

    @Test
    void testDuplicateCreateOnSameWorkerDiscardsNewSource()
    {
        RecordingSplitSource winner = new RecordingSplitSource(3);
        RecordingSplitSource loser = new RecordingSplitSource(3);
        String taskId = "task-1";
        // Two creates for the same id race past the registered-task fast path: while the first
        // create is still inside getSplits, a duplicate create (e.g. a retried POST whose original
        // is stuck in transport) runs to completion and registers its task, so the first create
        // loses the registration and must close its source instead of leaking it.
        AtomicReference<RemoteSplitsTaskManager> manager = new AtomicReference<>();
        AtomicInteger getSplitsCalls = new AtomicInteger();
        ConnectorSplitManager splitManager = new ConnectorSplitManager()
        {
            @Override
            public ConnectorSplitSource getSplits(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Set<ColumnHandle> dynamicFilterColumns,
                    Constraint constraint)
            {
                if (getSplitsCalls.incrementAndGet() == 1) {
                    manager.get().runSplitsTask(createRequest(taskId));
                    return loser;
                }
                return winner;
            }
        };
        manager.set(newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManager)));

        manager.get().runSplitsTask(createRequest(taskId));

        assertThat(loser.isClosed()).isTrue();
        assertThat(winner.isClosed()).isFalse();
        assertThat(manager.get().getTaskForTesting(taskId)).isNotNull();

        // a sequential re-POST for a registered task never reaches the split manager
        manager.get().runSplitsTask(createRequest(taskId));
        assertThat(getSplitsCalls).hasValue(2);
    }

    @Test
    void testStaleTaskIsReaped()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));
        assertThat(manager.touch(taskId)).isTrue();

        // simulate the coordinator going dark past the stale timeout, then the reaper's sweep
        manager.getTaskForTesting(taskId).expireForTesting();
        manager.cleanupExpired();

        assertThat(manager.getTaskForTesting(taskId)).isNull();
        assertThat(splitSource.isClosed()).isTrue();
        // once reaped, a heartbeat from a coordinator that thinks the task is still alive
        // must be told otherwise, not silently accepted
        assertThat(manager.touch(taskId)).isFalse();
    }

    @Test
    void testFreshTaskSurvivesReaping()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));
        manager.cleanupExpired();

        assertThat(manager.getTaskForTesting(taskId)).isNotNull();
        assertThat(splitSource.isClosed()).isFalse();
    }

    @Test
    void testWorkerSideFailureMidStreamClosesAndPreservesErrorCode()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.failOnCall(1, new TrinoException(PERMISSION_DENIED, "no access to table"));
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));

        assertThatThrownBy(() -> manager.fetchNextBatch(taskId, request(0)).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(TrinoException.class)
                .extracting(cause -> ((TrinoException) cause).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());

        // a task that dies mid-stream must not linger: the coordinator's DELETE would otherwise
        // race a reaper that already thinks it's gone, or worse, never get cleaned up at all
        assertThat(manager.getTaskForTesting(taskId)).isNull();
        assertThat(splitSource.isClosed()).isTrue();
    }

    @Test
    void testTouchUnknownTaskReturnsFalse()
    {
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.fail());
        assertThat(manager.touch("unknown-task")).isFalse();
    }

    @Test
    void testFetchNextBatchRenewsTaskBeforeStaleCleanup()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3, 2);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));
        manager.getTaskForTesting(taskId).expireForTesting();

        assertThat(manager.fetchNextBatch(taskId, request(0)).join().splits()).isNotEmpty();
        manager.cleanupExpired();

        assertThat(manager.getTaskForTesting(taskId)).isNotNull();
        assertThat(splitSource.isClosed()).isFalse();
    }

    @Test
    void testFetchNextBatchUnknownTaskReturnsFailedFuture()
    {
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.fail());
        assertThatThrownBy(() -> manager.fetchNextBatch("unknown-task", request(0)).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testInvalidTokenReturnsFailedFutureAndClosesTask()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));

        assertThatThrownBy(() -> manager.fetchNextBatch(taskId, request(2)).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(manager.getTaskForTesting(taskId)).isNull();
        assertThat(splitSource.isClosed()).isTrue();
    }

    @Test
    void testFetchNextBatchReturnsBatchAndKeepsTaskRegistered()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3, 2);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));

        RemoteSplitsTaskResponse response = manager.fetchNextBatch(taskId, request(0)).join();

        assertThat(response.token()).isZero();
        assertThat(response.nextToken()).isEqualTo(1);
        assertThat(response.splits()).hasSize(2);
        assertThat(response.noMoreResults()).isFalse();
        assertThat(manager.getTaskForTesting(taskId)).isNotNull();
        assertThat(splitSource.isClosed()).isFalse();
    }

    @Test
    void testNextTokenAcknowledgesBatchAndOldTokenIsHarmless()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3, 2);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));

        RemoteSplitsTaskResponse first = manager.fetchNextBatch(taskId, request(0)).join();
        assertThat(manager.fetchNextBatch(taskId, request(0)).join()).isEqualTo(first);
        assertThat(splitSource.requests()).hasSize(1);

        RemoteSplitsTaskResponse second = manager.fetchNextBatch(taskId, request(1)).join();
        assertThat(second.token()).isEqualTo(1);
        assertThat(second.nextToken()).isEqualTo(2);
        assertThat(splitSource.requests()).hasSize(2);

        // A delayed request for an acknowledged token must not close or advance the task.
        RemoteSplitsTaskResponse delayed = manager.fetchNextBatch(taskId, request(0)).join();
        assertThat(delayed.token()).isZero();
        assertThat(delayed.nextToken()).isZero();
        assertThat(delayed.splits()).isEmpty();
        assertThat(splitSource.requests()).hasSize(2);
        assertThat(manager.getTaskForTesting(taskId)).isNotNull();
    }

    @Test
    void testFinalBatchIsReplayableUntilClosed()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(1, 2);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));

        RemoteSplitsTaskResponse response = manager.fetchNextBatch(taskId, request(0)).join();

        // the source can close, but the task and final response remain until coordinator DELETE
        assertThat(response.noMoreResults()).isTrue();
        assertThat(response.splits()).hasSize(2);
        assertThat(manager.getTaskForTesting(taskId)).isNotNull();
        assertThat(splitSource.isClosed()).isTrue();

        assertThat(manager.fetchNextBatch(taskId, request(0)).join()).isEqualTo(response);
        assertThat(splitSource.requests()).hasSize(1);

        manager.close(taskId);
        assertThat(manager.getTaskForTesting(taskId)).isNull();
    }

    @Test
    void testUnacknowledgedFinalBatchExpiresWhenCoordinatorIsGone()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(1, 2);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        manager.runSplitsTask(createRequest(taskId));
        assertThat(manager.fetchNextBatch(taskId, request(0)).join().noMoreResults()).isTrue();

        manager.cleanupExpired();
        assertThat(manager.getTaskForTesting(taskId)).isNotNull();

        manager.getTaskForTesting(taskId).expireForTesting();
        manager.cleanupExpired();
        assertThat(manager.getTaskForTesting(taskId)).isNull();
    }

    @Test
    void testCloseFailureDuringConstructionFailureIsSuppressedNotMasking()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RuntimeException constructionFailure = new RuntimeException("construction failed");
        RuntimeException closeFailure = new RuntimeException("close failed");
        splitSource.failConstructionWith(constructionFailure);
        splitSource.failCloseWith(closeFailure);
        RemoteSplitsTaskManager manager = newManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)));

        String taskId = "task-1";
        // the close failure triggered while cleaning up after the construction failure must not
        // replace it; it must ride along as a suppressed exception instead
        assertThatThrownBy(() -> manager.runSplitsTask(createRequest(taskId)))
                .isSameAs(constructionFailure)
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(closeFailure));

        assertThat(manager.getTaskForTesting(taskId)).isNull();
    }

    @Test
    void testSaturatedCreationExecutorRejectsWithRetryableCode()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        // a shut-down executor rejects exactly like a full bounded queue does
        ExecutorService rejectingExecutor = newDirectExecutorService();
        rejectingExecutor.shutdown();
        RemoteSplitsTaskManager manager = newManager(
                CatalogServiceProvider.singleton(CATALOG_HANDLE, splitManagerReturning(splitSource)),
                rejectingExecutor);

        assertThatThrownBy(() -> manager.runSplitsTaskAsync(createRequest("task-queue-full")).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(TrinoException.class)
                .extracting(failure -> ((TrinoException) failure).getErrorCode())
                .isEqualTo(REMOTE_SPLITS_TASK_QUEUE_FULL.toErrorCode());
        assertThat(manager.getTaskForTesting("task-queue-full")).isNull();
    }

    private RemoteSplitsTaskManager newManager(CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider)
    {
        return newManager(splitManagerProvider, newDirectExecutorService());
    }

    private RemoteSplitsTaskManager newManager(CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider, ExecutorService taskCreationExecutor)
    {
        ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor();
        return new RemoteSplitsTaskManager(
                splitManagerProvider,
                new NoOpConnectorServicesProvider(),
                noopTracer(),
                new SessionPropertyManager(),
                sqlTaskManager,
                new NodeMemoryConfig(),
                taskCreationExecutor,
                cleanupExecutor);
    }

    private static ConnectorSplitManager splitManagerReturning(ConnectorSplitSource... sources)
    {
        Deque<ConnectorSplitSource> queue = new ArrayDeque<>(List.of(sources));
        return new ConnectorSplitManager()
        {
            @Override
            public ConnectorSplitSource getSplits(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Set<ColumnHandle> dynamicFilterColumns,
                    Constraint constraint)
            {
                return queue.remove();
            }
        };
    }

    private static CreateRemoteSplitsTaskRequest createRequest(String taskId)
    {
        Session session = testSessionBuilder().build();
        return new CreateRemoteSplitsTaskRequest(
                taskId,
                session.toSessionRepresentation(),
                TRANSACTION_HANDLE,
                TABLE_HANDLE,
                Span.getInvalid(),
                CATALOG_HANDLE,
                Optional.empty(),
                NO_COLUMNS,
                Constraint.alwaysTrue());
    }

    private static GetRemoteSplitsTaskRequest request(long token)
    {
        return new GetRemoteSplitsTaskRequest(token, 10, NO_DYNAMIC_FILTER);
    }

    private SqlTaskManager createSqlTaskManager()
    {
        return new SqlTaskManager(
                new EmbedVersion("testversion"),
                new NoOpConnectorServicesProvider(),
                createTestingPlanner(),
                new WorkerLanguageFunctionProvider(new LanguageFunctionEngineManager(), PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), new CompilerConfig()),
                new NoOpLocationFactory(),
                taskExecutor,
                new NodeInfo("test"),
                new LocalMemoryManager(new NodeMemoryConfig()),
                newEmptyMemoryPool(),
                newEmptyMemoryPool(),
                taskManagementExecutor,
                new TaskManagerConfig(),
                new NodeMemoryConfig(),
                new GpuNodeSetup.Disabled(),
                new GpuConfig(),
                new LocalSpillManager(new NodeSpillConfig()),
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry(OpenTelemetry.noop(), TestingInternalNodeManager.createDefault().getTestingInternalCoordinatorLocator(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of()), new ExchangeManagerConfig()));
    }

    private static class NoOpConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(List<CatalogProperties> catalogs) {}

        @Override
        public PrunableState getPrunableState()
        {
            return PrunableState.empty();
        }

        @Override
        public void pruneCatalogs(PrunableState prunableState, Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class NoOpLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return URI.create("http://fake.invalid/query/" + queryId);
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + taskId);
        }

        @Override
        public URI createTaskLocation(InternalNode node, TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + node.getNodeIdentifier() + "/" + taskId);
        }

        @Override
        public URI createMemoryInfoLocation(InternalNode node)
        {
            return URI.create("http://fake.invalid/" + node.getNodeIdentifier() + "/memory");
        }
    }
}
