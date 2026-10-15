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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Scopes;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.FailureInjector;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryRunnerUtil;
import io.trino.execution.QueryState;
import io.trino.execution.TaskInfo;
import io.trino.execution.TestingFailureInjectionConfig;
import io.trino.execution.TestingFailureInjector;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.node.InternalNode;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.Uninterruptibles.awaitTerminationUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.RETRY_INITIAL_DELAY;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.FileUtils.createTempFileForTesting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/// Tests that `experimental.force-single-node-query` gives each concurrent query its own worker.
@Execution(SAME_THREAD) // Each test expects to have the workers to itself
final class TestForceQuerySingleNodePlacement
        extends AbstractTestQueryFramework
{
    /// Runs until it is canceled, holding its worker.
    private static final String SLOW_QUERY = "SELECT count(*) FROM blackhole.default.slow";
    private static final int WORKER_COUNT = 3;
    /// The source of a session whose queries a resource group with one slot per worker admits
    private static final String LIMITED_SOURCE = "limited";
    private static final int QUERIES_PER_STREAM = 3;
    /// A random pick passes one round at most a third of the time, so it cannot pass this many.
    private static final int ROUNDS = 22;

    private ListeningExecutorService executorService;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .setWorkerCount(WORKER_COUNT)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .addExtraProperty("experimental.force-single-node-query", "true")
                // One callback thread per query, so a test can hold the listeners of a query
                .addExtraProperty("query.max-state-machine-callback-threads", "1")
                .setAdditionalModule(binder -> {
                    configBinder(binder).bindConfig(TestingFailureInjectionConfig.class);
                    newOptionalBinder(binder, FailureInjector.class).setBinding().to(TestingFailureInjector.class).in(Scopes.SINGLETON);
                })
                .build();
        try {
            // One slot per worker for the "limited" source, configured before the first query creates the groups
            Path resourceGroups = createTempFileForTesting("resource-groups", ".json");
            Files.writeString(resourceGroups,
                    """
                    {
                      "rootGroups": [
                        {"name": "limited", "softMemoryLimit": "100%%", "hardConcurrencyLimit": %s, "maxQueued": 1000},
                        {"name": "unlimited", "softMemoryLimit": "100%%", "hardConcurrencyLimit": 1000, "maxQueued": 1000}
                      ],
                      "selectors": [{"source": "%s", "group": "limited"}, {"group": "unlimited"}]
                    }
                    """.formatted(WORKER_COUNT, LIMITED_SOURCE));
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().orElseThrow()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", resourceGroups.toString()));

            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
            queryRunner.addFunctions(InternalFunctionBundle.builder().scalars(DriverGates.class).build());
            // The delay keeps every query running until it is canceled
            queryRunner.execute("CREATE TABLE blackhole.default.slow (a bigint) WITH (split_count = 1, pages_per_split = 1, rows_per_page = 1, page_processing_delay = '1d')");
            // The one row holds a 0, so dividing by it fails at run time
            queryRunner.execute("CREATE TABLE blackhole.default.quick (a bigint) WITH (split_count = 1, pages_per_split = 1, rows_per_page = 1)");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @BeforeEach
    void setUp()
    {
        executorService = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
    }

    @AfterEach
    void tearDown()
    {
        // A failed test leaves streams behind, which would keep submitting queries
        executorService.shutdownNow();
        boolean testThreadsEnded = awaitTerminationUninterruptibly(executorService, 60, SECONDS);
        executorService = null;
        DriverGates.clear();
        assertThat(testThreadsEnded).describedAs("the test threads ended").isTrue();
    }

    @Test
    @Timeout(120)
    void testConcurrentQueriesRunOnDistinctWorkers()
    {
        List<String> workerNodeIds = workerNodeIds();
        for (int round = 0; round < ROUNDS; round++) {
            assertConcurrentQueriesRunOnDistinctWorkers(SLOW_QUERY, workerNodeIds);
        }
    }

    @Test
    @Timeout(120)
    void testConcurrentTableFunctionQueriesRunOnDistinctWorkers()
    {
        List<String> workerNodeIds = workerNodeIds();
        // A leaf table function goes through the partitioning cache, yet still holds one worker
        for (int round = 0; round < ROUNDS; round++) {
            assertConcurrentQueriesRunOnDistinctWorkers("SELECT count(*) FROM TABLE(sequence(1, 1000000000000))", workerNodeIds);
        }
    }

    @Test
    @Timeout(120)
    void testQueriesWithoutSplitsRunOnDistinctWorkers()
    {
        List<String> workerNodeIds = workerNodeIds();
        // Queries with and without splits take different paths, so they must respect each other's placements
        for (int round = 0; round < ROUNDS; round++) {
            assertQueriesWithoutSplitsRunOnTheirOwnWorkers(round, workerNodeIds);
        }
    }

    @Test
    @Timeout(120)
    void testNewQueryTakesWorkerOfCanceledQuery()
    {
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        List<QueryId> queries = new ArrayList<>();
        try {
            for (int query = 0; query < WORKER_COUNT; query++) {
                queries.add(submitQuery(SLOW_QUERY));
            }
            assertThat(queries.stream().map(this::awaitPlacement))
                    .containsExactlyInAnyOrderElementsOf(workerNodeIds());

            // A query is canceled while every other worker stays busy, and a new one is submitted
            for (int replacement = 0; replacement < 3; replacement++) {
                QueryId replacedQuery = queries.removeLast();
                String worker = awaitPlacement(replacedQuery);
                queryManager.cancelQuery(replacedQuery);
                assertThat(queryManager.getQueryState(replacedQuery).isDone()).isTrue();

                QueryId newQuery = submitQuery(SLOW_QUERY);
                queries.add(newQuery);
                assertThat(awaitPlacement(newQuery)).isEqualTo(worker);
            }
        }
        finally {
            cancelAndAwaitDone(queries);
        }
    }

    /// A completed query's worker is free before the listener dropping its placement runs: the reservation reads the query's phase itself
    @Test
    @Timeout(120)
    void testNewQueryTakesWorkerOfCompletedQueryWithHeldListeners()
            throws Exception
    {
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        List<QueryId> queries = new ArrayList<>();
        CountDownLatch releaseListeners = new CountDownLatch(1);
        DriverGate firstGate = newDriverGate();
        DriverGate secondGate = newDriverGate();
        try {
            // The first query takes the worker a tie goes to, so the next query reaches the completed query's worker only by seeing it completed
            QueryId firstQuery = submitQuery(firstGate.query());
            queries.add(firstQuery);
            awaitArrival(firstGate, firstQuery);
            awaitPlacement(firstQuery);

            Future<QueryId> secondClient = executorService.submit(() -> getDistributedQueryRunner().executeWithQueryId(getSession(), secondGate.query()).queryId());
            QueryId secondQuery = awaitSubmitted(secondGate.query());
            queries.add(secondQuery);
            awaitArrival(secondGate, secondQuery);
            String secondWorker = awaitPlacement(secondQuery);

            for (int query = 0; query < WORKER_COUNT - 2; query++) {
                queries.add(submitQuery(SLOW_QUERY));
            }
            queries.forEach(this::awaitPlacement);

            // The only callback thread of the completed query blocks here, so the listener releasing its placement waits
            CountDownLatch listenersHeld = new CountDownLatch(1);
            queryManager.addStateChangeListener(secondQuery, state -> {
                if (state.isDone()) {
                    listenersHeld.countDown();
                    awaitUninterruptibly(releaseListeners);
                }
            });
            CountDownLatch released = new CountDownLatch(1);
            queryManager.addFinalQueryInfoListener(secondQuery, _ -> released.countDown());
            // Completes the query while its done listeners stay blocked, so nothing has dropped its placement yet
            secondGate.open();
            assertThat(awaitUninterruptibly(listenersHeld, 60, SECONDS)).describedAs("the listeners of %s are held", secondQuery).isTrue();
            assertEventually(() -> assertThat(queryManager.getFullQueryInfo(secondQuery).isFinalQueryInfo()).isTrue());
            // A tearing-down worker outranks a running one, so the new query reaches the completed worker only if the reservation
            // drops a completed placement outright instead of counting it as tearing down
            queryManager.cancelQuery(firstQuery);

            QueryId newQuery = submitQuery(SLOW_QUERY);
            queries.add(newQuery);
            assertThat(awaitPlacement(newQuery)).isEqualTo(secondWorker);
            // No listener released the placement; the reservation pulled it out
            assertThat(released.getCount()).isEqualTo(1);
            assertThat(queryManager.getFullQueryInfo(firstQuery).isFinalQueryInfo()).isFalse();

            releaseListeners.countDown();
            assertThat(secondClient.get(60, SECONDS)).isEqualTo(secondQuery);
        }
        finally {
            releaseListeners.countDown();
            firstGate.open();
            secondGate.open();
            cancelAndAwaitDone(queries);
        }
    }

    /// A query ending with a task still on its worker leaves that worker without a running query, so a new query takes it
    @ParameterizedTest
    @EnumSource(AbortCause.class)
    @Timeout(120)
    void testNewQueryTakesWorkerOfTearingDownQuery(AbortCause cause)
            throws Exception
    {
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        List<QueryId> queries = new ArrayList<>();
        try (GatedQuery gated = startGatedQuery(cause, newQuerySession())) {
            for (int query = 0; query < WORKER_COUNT - 1; query++) {
                queries.add(submitQuery(SLOW_QUERY));
            }
            queries.forEach(this::awaitPlacement);

            gated.abort();
            QueryId newQuery = submitQuery(SLOW_QUERY);
            queries.add(newQuery);

            // Only the tearing-down worker has no running query, so the new query goes there despite the task still on it
            assertThat(awaitPlacement(newQuery)).isEqualTo(gated.worker());
            assertThat(queryManager.getFullQueryInfo(gated.queryId()).isFinalQueryInfo()).isFalse();
        }
        finally {
            cancelAndAwaitDone(queries);
        }
    }

    /// A worker with no query beats one whose done query still has a task on it, whichever way that query ended
    @ParameterizedTest
    @EnumSource(AbortCause.class)
    @Timeout(120)
    void testIdleWorkerBeatsTearingDownWorker(AbortCause cause)
            throws Exception
    {
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        List<QueryId> queries = new ArrayList<>();
        try (GatedQuery gated = startGatedQuery(cause, newQuerySession())) {
            for (int query = 0; query < WORKER_COUNT - 2; query++) {
                queries.add(submitQuery(SLOW_QUERY));
            }
            String gatedWorker = gated.worker();
            List<String> busyWorkers = queries.stream().map(this::awaitPlacement).toList();
            assertThat(busyWorkers).doesNotContain(gatedWorker);
            String idleWorker = workerNodeIds().stream()
                    .filter(worker -> !worker.equals(gatedWorker) && !busyWorkers.contains(worker))
                    .collect(onlyElement());
            gated.abort();

            QueryId first = submitQuery(SLOW_QUERY);
            queries.add(first);
            assertThat(awaitPlacement(first)).isEqualTo(idleWorker);

            QueryId second = submitQuery(SLOW_QUERY);
            queries.add(second);
            assertThat(awaitPlacement(second)).isEqualTo(gatedWorker);
            assertThat(queryManager.getFullQueryInfo(gated.queryId()).isFinalQueryInfo()).isFalse();
        }
        finally {
            cancelAndAwaitDone(queries);
        }
    }

    @Test
    @Timeout(120)
    void testRetriedQueryKeepsItsWorker()
    {
        for (int round = 0; round < ROUNDS; round++) {
            Session retriedSession = retriedQuerySession(newQuerySession());
            failFirstAttempt(retriedSession);
            QueryId retriedQuery = submitQuery(retriedSession, SLOW_QUERY);
            try {
                String firstAttemptWorker = awaitPlacement(retriedQuery, 0);
                assertThat(awaitPlacement(retriedQuery, 1)).isEqualTo(firstAttemptWorker);
            }
            finally {
                cancelAndAwaitDone(ImmutableList.of(retriedQuery));
            }
        }
    }

    /// Every query ends while the next ones are being placed, under each submission, query end, and callback timing.
    @ParameterizedTest
    @MethodSource("scenarios")
    @Timeout(300)
    void testQueryStreamsNeverShareAWorker(Scenario scenario)
            throws Exception
    {
        int streamCount = switch (scenario.submission()) {
            case ONE_PER_WORKER -> WORKER_COUNT;
            case TWO_PER_WORKER_QUEUED -> 2 * WORKER_COUNT;
        };
        List<List<QueryRun>> streams = allAsList(IntStream.range(0, streamCount)
                .mapToObj(_ -> executorService.submit(() -> runQueryStream(scenario)))
                .collect(toImmutableList()))
                .get(240, SECONDS);
        assertNoWorkerRunsTwoQueriesAtOnce(streams.stream().flatMap(List::stream).collect(toImmutableList()));
    }

    /// Why a query's task is aborted while a driver of it still waits on a gate, which keeps the task alive past the query's end.
    private enum AbortCause
    {
        /// Canceled while both of its drivers wait on their gates
        CANCELED,
        /// Fails in one driver once its gate opens, while the other driver waits on its gate
        FAILED,
        /// Fails in one driver with a retryable error once its gate opens, then succeeds on the retry attempt while the first attempt's driver waits on its gate
        RETRIED;

        /// A query whose two branches run as two drivers of one task: one waits on `holdGate` and the other ends the query at `endGate`
        String query(DriverGate holdGate, DriverGate endGate)
        {
            String holding = switch (this) {
                case CANCELED, FAILED -> "count_if(await_gate('%s'))".formatted(holdGate.name());
                // Only the first attempt waits, so the retry attempt passes while the first attempt's driver still waits
                case RETRIED -> "count_if(await_gate_once('%s'))".formatted(holdGate.name());
            };
            String ending = switch (this) {
                case CANCELED -> "count_if(await_gate('%s'))".formatted(endGate.name());
                // The one row holds a 0, so the division fails once the gate opens
                case FAILED -> "count(IF(await_gate('%s'), 1 / a))".formatted(endGate.name());
                case RETRIED -> "count_if(fail_at_gate_once('%s'))".formatted(endGate.name());
            };
            return "SELECT * FROM (SELECT %s FROM blackhole.default.quick UNION ALL SELECT %s FROM blackhole.default.quick)".formatted(holding, ending);
        }
    }

    /// Starts a query that `cause` ends with a task still on its worker, under `session`, and returns once both of its drivers wait on their gates.
    private GatedQuery startGatedQuery(AbortCause cause, Session session)
    {
        DriverGate holdGate = newDriverGate();
        DriverGate endGate = newDriverGate();
        String sql = cause.query(holdGate, endGate);
        GatedQuery gated = null;
        try {
            Optional<Future<?>> client = Optional.empty();
            QueryId queryId;
            if (cause == AbortCause.RETRIED) {
                // Only a client reads the output of a query, without which a successful query never finishes
                Session retriedSession = retriedQuerySession(session);
                client = Optional.of(executorService.submit(() -> getDistributedQueryRunner().execute(retriedSession, sql)));
                queryId = awaitSubmitted(sql);
            }
            else {
                queryId = submitQuery(session, sql);
            }
            gated = new GatedQuery(queryId, cause, holdGate, endGate, client);
            awaitArrival(holdGate, queryId);
            awaitArrival(endGate, queryId);
            return gated;
        }
        catch (Throwable e) {
            // A query submitted but not yet found would otherwise keep a driver waiting on its worker
            holdGate.open();
            endGate.open();
            if (gated != null) {
                closeAllSuppress(e, gated);
            }
            throw e;
        }
    }

    /// A query whose two drivers wait on gates on one worker: `cause` ends the query while the first driver keeps waiting, until the query is closed
    private final class GatedQuery
            implements AutoCloseable
    {
        private final QueryId queryId;
        private final AbortCause cause;
        private final DriverGate holdGate;
        private final DriverGate endGate;
        private final Optional<Future<?>> client;

        private GatedQuery(QueryId queryId, AbortCause cause, DriverGate holdGate, DriverGate endGate, Optional<Future<?>> client)
        {
            this.queryId = queryId;
            this.cause = cause;
            this.holdGate = holdGate;
            this.endGate = endGate;
            this.client = client;
        }

        public QueryId queryId()
        {
            return queryId;
        }

        /// The worker the first attempt's task runs on
        public String worker()
        {
            return awaitPlacement(queryId, 0);
        }

        /// Aborts the first task through `cause` and returns once the query is done, with that task still waiting on its gate
        public void abort()
                throws Exception
        {
            QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
            switch (cause) {
                case CANCELED -> queryManager.cancelQuery(queryId);
                case FAILED -> {
                    endGate.open();
                    QueryRunnerUtil.waitForQueryState(getQueryRunner(), queryId, QueryState.FAILED);
                }
                case RETRIED -> {
                    endGate.open();
                    QueryRunnerUtil.waitForQueryState(getQueryRunner(), queryId, QueryState.FINISHED);
                    // The retry attempt ran beside the first attempt's task
                    assertThat(awaitPlacement(queryId, 1)).isEqualTo(worker());
                }
            }
            assertThat(queryManager.getQueryState(queryId).isDone()).isTrue();
        }

        /// Opens the gates the drivers of the aborted task wait on, so the task ends; a canceled query holds both
        public void endHeldTask()
        {
            holdGate.open();
            endGate.open();
        }

        /// Opens the gates, so the waiting drivers end, then waits until the query and its client are done
        @Override
        public void close()
                throws Exception
        {
            holdGate.open();
            endGate.open();
            cancelAndAwaitDone(ImmutableList.of(queryId));
            if (client.isPresent()) {
                client.get().get(60, SECONDS);
            }
        }
    }

    /// Two queries on one worker overlap only if one was placed while the other ran
    private static void assertNoWorkerRunsTwoQueriesAtOnce(List<QueryRun> runs)
    {
        Map<String, List<QueryRun>> runsByWorker = runs.stream()
                .collect(groupingBy(QueryRun::worker));
        for (List<QueryRun> workerRuns : runsByWorker.values()) {
            workerRuns.sort(Comparator.comparing(QueryRun::start));
            for (int i = 1; i < workerRuns.size(); i++) {
                QueryRun previous = workerRuns.get(i - 1);
                QueryRun next = workerRuns.get(i);
                assertThat(next.start())
                        .describedAs("%s placed while %s was running", next, previous)
                        .isAfter(previous.end());
            }
        }
    }

    static List<Scenario> scenarios()
    {
        ImmutableList.Builder<Scenario> scenarios = ImmutableList.builder();
        for (Submission submission : Submission.values()) {
            for (QueryKind kind : QueryKind.values()) {
                for (PlacementRelease release : PlacementRelease.values()) {
                    scenarios.add(new Scenario(submission, kind, release));
                }
            }
        }
        return scenarios.build();
    }

    private record Scenario(Submission submission, QueryKind kind, PlacementRelease release) {}

    /// How many clients submit queries, and what admits them
    enum Submission
    {
        /// One client per worker, each submitting its next query once it sees the previous one done
        ONE_PER_WORKER,
        /// Two clients per worker, with a resource group admitting one query per worker
        TWO_PER_WORKER_QUEUED,
    }

    /// How a query ends, each one waiting on a gate on its worker so that a test controls when.
    enum QueryKind
    {
        FINISHED,
        FAILED,
        RETRIED,
        CANCELED;

        String query(DriverGate gate)
        {
            return switch (this) {
                case FINISHED, RETRIED, CANCELED -> gate.query();
                // The one row holds a 0, so the division fails once the gate opens
                case FAILED -> "SELECT count(IF(await_gate('%s'), 1 / a)) FROM blackhole.default.quick".formatted(gate.name());
            };
        }
    }

    /// How a done query's placement gets out of the way of the next query
    enum PlacementRelease
    {
        /// The query's listeners push it out as soon as the query is done, as in production
        PUSH,
        /// The next reservation pulls it out by reading the query's phase itself: the listeners are held until the client has placed
        /// its next query, as on a saturated callback executor
        PULL,
    }

    /// Runs queries one after another, as a client would, and reports where and when each ran.
    private List<QueryRun> runQueryStream(Scenario scenario)
            throws Exception
    {
        List<StreamQuery> queries = new ArrayList<>();
        try {
            Optional<StreamQuery> last = Optional.empty();
            for (int i = 0; i < QUERIES_PER_STREAM; i++) {
                StreamQuery query = new StreamQuery(scenario, newDriverGate());
                queries.add(query);
                switch (scenario.submission()) {
                    case ONE_PER_WORKER -> {}
                    // The resource group frees a slot from a state change listener, which the held callbacks of the previous query block
                    case TWO_PER_WORKER_QUEUED -> last.ifPresent(StreamQuery::close);
                }
                query.place();
                // Under one query per worker the previous query closes only after the next one is placed
                last.ifPresent(StreamQuery::close);
                query.end();
                last = Optional.of(query);
            }
            return queries.stream().map(StreamQuery::toQueryRun).collect(toImmutableList());
        }
        finally {
            // Closing a done query again is harmless, so this ends only what a failed stream left behind
            queries.forEach(StreamQuery::close);
            cancelAndAwaitDone(queries.stream().map(StreamQuery::queryId).collect(toImmutableList()));
        }
    }

    /// One query of a stream, from its submission to its end, which it reaches the way `scenario` says
    private final class StreamQuery
    {
        private final Scenario scenario;
        private final DriverGate gate;
        private final Future<?> client;
        private final QueryId queryId;
        private String worker;
        private Optional<CallbackHold> hold = Optional.empty();

        /// Submits the query through a client, as only a client reads the output of a query, without which a successful query never finishes
        private StreamQuery(Scenario scenario, DriverGate gate)
        {
            this.scenario = scenario;
            this.gate = gate;
            Session session = streamSession(scenario);
            String sql = scenario.kind().query(gate);
            client = executorService.submit(() -> getDistributedQueryRunner().execute(session, sql));
            try {
                queryId = awaitSubmitted(sql);
            }
            catch (Throwable e) {
                // The query may be running already, with a driver waiting on the gate
                gate.open();
                throw e;
            }
        }

        public QueryId queryId()
        {
            return queryId;
        }

        /// Waits until a driver of the query reaches the gate, and records the worker it runs on
        void place()
        {
            awaitArrival(gate, queryId);
            // The first attempt of a retried query fails at once, so its second attempt is the one that runs
            int attempt = switch (scenario.kind()) {
                case FINISHED, FAILED, CANCELED -> 0;
                case RETRIED -> 1;
            };
            worker = awaitPlacement(queryId, attempt);
        }

        /// Ends the query, while its listeners wait under `PULL` and, for a canceled query, while its task holds the gate
        void end()
                throws Exception
        {
            hold = switch (scenario.release()) {
                case PUSH -> Optional.empty();
                case PULL -> Optional.of(switch (scenario.kind()) {
                    case FINISHED, FAILED, CANCELED -> holdCallbacks(queryId);
                    // Under the QUERY retry policy, the client learns that it has read all the output from a callback that runs once
                    // the scheduler is done, so the callbacks are held from the finishing state on, which the query reaches once the gate opens
                    case RETRIED -> holdCallbacksOnceFinishing(queryId);
                });
            };
            switch (scenario.kind()) {
                case FINISHED, FAILED, RETRIED -> gate.open();
                case CANCELED -> getDistributedQueryRunner().getCoordinator().getQueryManager().cancelQuery(queryId);
            }
            hold.ifPresent(CallbackHold::awaitHeld);
            switch (scenario.kind()) {
                case FINISHED, RETRIED -> client.get(60, SECONDS);
                case FAILED -> assertThatThrownBy(() -> client.get(60, SECONDS)).hasMessageContaining("Division by zero");
                case CANCELED -> assertThatThrownBy(() -> client.get(60, SECONDS)).hasMessageContaining("Query was canceled");
            }
        }

        /// Opens the gate, which the query's task may still wait on, and releases its held callbacks
        void close()
        {
            gate.open();
            hold.ifPresent(CallbackHold::release);
        }

        /// Where the done query ran, from the creation of its first task until it was done
        QueryRun toQueryRun()
        {
            QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId);
            List<TaskInfo> tasks = queryInfo.getStages().orElseThrow().getStages().stream()
                    .filter(stage -> stage.plan().getPartitioning().equals(SINGLE_DISTRIBUTION))
                    .flatMap(stage -> stage.tasks().stream())
                    .collect(toImmutableList());
            assertThat(tasks.stream().map(task -> task.taskStatus().nodeId()).distinct())
                    .describedAs("%s ran on more than one worker", queryId)
                    .containsExactly(worker);
            Instant start = tasks.stream().map(task -> task.stats().createTime()).min(Comparator.naturalOrder()).orElseThrow();
            return new QueryRun(queryId, worker, start, queryInfo.getQueryStats().getEndTime());
        }
    }

    /// The session a stream query runs under: a retried query fails its first attempt, and a queued query runs under the resource group limit
    private Session streamSession(Scenario scenario)
    {
        Session session = switch (scenario.submission()) {
            case ONE_PER_WORKER -> newQuerySession();
            case TWO_PER_WORKER_QUEUED -> limitedSession();
        };
        return switch (scenario.kind()) {
            case FINISHED, FAILED, CANCELED -> session;
            case RETRIED -> {
                Session retried = retriedQuerySession(session);
                failFirstAttempt(retried);
                yield retried;
            }
        };
    }

    /// Where `query` ran, from the creation of its first task until it was done
    private record QueryRun(QueryId query, String worker, Instant start, Instant end) {}

    /// Blocks the only callback thread of a query from its current state on, so its state change listeners wait as on a saturated executor.
    private CallbackHold holdCallbacks(QueryId queryId)
    {
        return holdCallbacks(queryId, _ -> true);
    }

    /// Blocks the only callback thread of a query once it is finishing, so the listeners of its end wait as on a saturated executor.
    private CallbackHold holdCallbacksOnceFinishing(QueryId queryId)
    {
        return holdCallbacks(queryId, state -> state == FINISHING || state.isDone());
    }

    private CallbackHold holdCallbacks(QueryId queryId, Predicate<QueryState> heldStates)
    {
        return new CallbackHold(getDistributedQueryRunner().getCoordinator().getQueryManager(), queryId, heldStates);
    }

    private static final class CallbackHold
    {
        private final QueryId queryId;
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private CallbackHold(QueryManager queryManager, QueryId queryId, Predicate<QueryState> heldStates)
        {
            this.queryId = queryId;
            queryManager.addStateChangeListener(queryId, state -> {
                if (!heldStates.test(state)) {
                    return;
                }
                held.countDown();
                awaitUninterruptibly(released);
            });
        }

        /// Returns once the callback thread is blocked
        void awaitHeld()
        {
            assertThat(awaitUninterruptibly(held, 60, SECONDS)).describedAs("the listener of %s ran", queryId).isTrue();
        }

        void release()
        {
            released.countDown();
        }
    }

    private void assertConcurrentQueriesRunOnDistinctWorkers(String query, List<String> workerNodeIds)
    {
        List<QueryId> queries = new ArrayList<>();
        try {
            for (int i = 0; i < WORKER_COUNT; i++) {
                queries.add(submitQuery(query));
            }
            assertThat(queries.stream().map(this::awaitPlacement))
                    .containsExactlyInAnyOrderElementsOf(workerNodeIds);
        }
        finally {
            cancelAndAwaitDone(queries);
        }
    }

    private void assertQueriesWithoutSplitsRunOnTheirOwnWorkers(int round, List<String> workerNodeIds)
    {
        // A gate holds the literal query on its worker, and the page processing delay holds the insert until it is canceled
        DriverGate gate = newDriverGate();
        String literals = (round % 2 == 0 ? "SELECT await_gate('%s')" : "VALUES await_gate('%s')").formatted(gate.name());
        List<String> withoutSplits = ImmutableList.of(literals, "INSERT INTO blackhole.default.slow VALUES (1)");
        List<String> withSplits = nCopies(WORKER_COUNT - withoutSplits.size(), SLOW_QUERY);
        // Alternate which kind of query takes its workers first
        List<String> queries = round / 2 % 2 == 0
                ? ImmutableList.<String>builder().addAll(withoutSplits).addAll(withSplits).build()
                : ImmutableList.<String>builder().addAll(withSplits).addAll(withoutSplits).build();

        List<QueryId> queryIds = new ArrayList<>();
        try {
            for (String query : queries) {
                QueryId queryId = submitQuery(query);
                queryIds.add(queryId);
                awaitPlacement(queryId);
            }
            awaitArrival(gate, queryIds.get(queries.indexOf(literals)));
            assertThat(queryIds.stream().map(this::awaitPlacement))
                    .containsExactlyInAnyOrderElementsOf(workerNodeIds);
        }
        finally {
            // The driver at the gate waits uninterruptibly, so the cancel ends its task only once the gate is open
            gate.open();
            cancelAndAwaitDone(queryIds);
        }
    }

    /// Holds the driver of a query calling `await_gate(name)` on its worker until the test opens the gate.
    public static final class DriverGates
    {
        private static final Map<String, DriverGate> GATES = new ConcurrentHashMap<>();

        private DriverGates() {}

        /// Opens and forgets every gate, so the drivers a failed test left waiting on one end
        static void clear()
        {
            GATES.values().forEach(DriverGate::open);
            GATES.clear();
        }

        @ScalarFunction(value = "await_gate", deterministic = false)
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean awaitGate(@SqlType(StandardTypes.VARCHAR) Slice name)
        {
            DriverGate gate = GATES.get(name.toStringUtf8());
            gate.arrived().countDown();
            // Canceling a task interrupts its driver, which must keep waiting to hold the task
            awaitUninterruptibly(gate.opened());
            return true;
        }

        /// Waits on the gate `name` like `await_gate` in the first driver calling it, and returns at once in later ones.
        @ScalarFunction(value = "await_gate_once", deterministic = false)
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean awaitGateOnce(@SqlType(StandardTypes.VARCHAR) Slice name)
        {
            if (GATES.get(name.toStringUtf8()).passed().compareAndSet(false, true)) {
                return awaitGate(name);
            }
            return true;
        }

        /// Waits on the gate `name` like `await_gate`, then fails the first driver calling it with a retryable error, so the `QUERY` retry policy runs the query again.
        @ScalarFunction(value = "fail_at_gate_once", deterministic = false)
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean failAtGateOnce(@SqlType(StandardTypes.VARCHAR) Slice name)
        {
            awaitGate(name);
            if (GATES.get(name.toStringUtf8()).passed().compareAndSet(false, true)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Injected failure at " + name.toStringUtf8());
            }
            return true;
        }
    }

    /// A gate drivers wait on until a test opens it, with `passed` recording the first driver through a once-only function.
    private record DriverGate(String name, CountDownLatch arrived, CountDownLatch opened, AtomicBoolean passed)
    {
        String query()
        {
            return "SELECT count_if(await_gate('%s')) FROM blackhole.default.quick".formatted(name);
        }

        boolean hasArrived()
        {
            return arrived.getCount() == 0;
        }

        void open()
        {
            opened.countDown();
        }
    }

    private DriverGate newDriverGate()
    {
        DriverGate gate = new DriverGate("gate_" + randomNameSuffix(), new CountDownLatch(1), new CountDownLatch(1), new AtomicBoolean());
        DriverGates.GATES.put(gate.name(), gate);
        return gate;
    }

    /// Waits until a driver of `queryId` reaches `gate`, or until the query ends, which fails at once rather than after a timeout
    private void awaitArrival(DriverGate gate, QueryId queryId)
    {
        assertEventually(new Duration(60, SECONDS), () -> assertThat(gate.hasArrived() || isDone(queryId))
                .describedAs("a driver of %s reached %s or the query, which is %s, ended", queryId, gate.name(), stateOf(queryId))
                .isTrue());
        assertThat(gate.hasArrived())
                .describedAs("a driver of %s reached %s before the query ended as %s", queryId, gate.name(), stateOf(queryId))
                .isTrue();
    }

    private boolean isDone(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator().getDispatchManager().getQueryInfo(queryId).getState().isDone();
    }

    /// The state of `queryId`, with its error code once it has failed
    private String stateOf(QueryId queryId)
    {
        BasicQueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getDispatchManager().getQueryInfo(queryId);
        if (queryInfo.getErrorCode() == null) {
            return queryInfo.getState().toString();
        }
        return queryInfo.getState() + " with " + queryInfo.getErrorCode().getName();
    }

    /// The query a client submitted as `query`, found by its text, once the coordinator has it
    private QueryId awaitSubmitted(String query)
    {
        assertEventually(() -> findQuery(query));
        return findQuery(query);
    }

    /// The query a client runs as `query`, found by its text.
    private QueryId findQuery(String query)
    {
        return getDistributedQueryRunner().getCoordinator().getDispatchManager().getQueries().stream()
                .filter(info -> info.getQuery().equals(query))
                .map(BasicQueryInfo::getQueryId)
                .collect(onlyElement());
    }

    /// Submits `query` without a client and returns once it is registered.
    private QueryId submitQuery(String query)
    {
        return submitQuery(newQuerySession(), query);
    }

    private QueryId submitQuery(Session session, String query)
    {
        return QueryRunnerUtil.createQuery(getQueryRunner(), session, query);
    }

    /// `session` under the `QUERY` retry policy, with a trace token for the failure injector to target
    private static Session retriedQuerySession(Session session)
    {
        return Session.builder(session)
                .setSystemProperty(RETRY_POLICY, "QUERY")
                .setSystemProperty(RETRY_INITIAL_DELAY, "0s")
                .setTraceToken(Optional.of(UUID.randomUUID().toString()))
                .build();
    }

    /// Fails the first task of attempt 0, so the `QUERY` retry policy runs the query again as attempt 1.
    private void failFirstAttempt(Session session)
    {
        getDistributedQueryRunner().injectTaskFailure(session.getTraceToken().orElseThrow(), 0, 0, 0, TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR));
    }

    /// The default session under a fresh query id, which a query submitted without a client runs as
    private Session newQuerySession()
    {
        return Session.builder(getSession())
                .setQueryId(getDistributedQueryRunner().getCoordinator().getDispatchManager().createQueryId())
                .build();
    }

    /// A session whose queries the resource group with one slot per worker admits
    private Session limitedSession()
    {
        return Session.builder(newQuerySession()).setSource(LIMITED_SOURCE).build();
    }

    /// Cancels `queries`, whose gates must be open, and waits until all their tasks are done.
    private void cancelAndAwaitDone(List<QueryId> queries)
    {
        DispatchManager dispatchManager = getDistributedQueryRunner().getCoordinator().getDispatchManager();
        queries.forEach(dispatchManager::cancelQuery);
        assertEventually(() -> {
            for (QueryId queryId : queries) {
                assertThat(dispatchManager.getFullQueryInfo(queryId).orElseThrow().isFinalQueryInfo()).isTrue();
            }
        });
    }

    private List<String> workerNodeIds()
    {
        return getDistributedQueryRunner().getServers().stream()
                .filter(server -> !server.isCoordinator())
                .map(TestingTrinoServer::getCurrentNode)
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableList());
    }

    private String awaitPlacement(QueryId queryId)
    {
        return awaitPlacement(queryId, 0);
    }

    /// Waits until an attempt of `queryId` is placed, or until the query ends, which fails at once rather than after a timeout
    private String awaitPlacement(QueryId queryId, int attemptId)
    {
        assertEventually(() -> assertThat(findPlacement(queryId, attemptId).isPresent() || isDone(queryId))
                .describedAs("attempt %s of %s, which is %s, is placed or the query ended", attemptId, queryId, stateOf(queryId))
                .isTrue());
        Optional<String> placement = findPlacement(queryId, attemptId);
        assertThat(placement).describedAs("attempt %s of %s, which is %s, is placed", attemptId, queryId, stateOf(queryId)).isPresent();
        return placement.orElseThrow();
    }

    /// The worker running the single node stage of an attempt, which for an insert is below the output stage.
    private Optional<String> findPlacement(QueryId queryId, int attemptId)
    {
        return getDistributedQueryRunner().getCoordinator().getDispatchManager().getFullQueryInfo(queryId)
                .flatMap(QueryInfo::getStages)
                .flatMap(stages -> stages.getStages().stream()
                        .filter(stage -> stage.plan().getPartitioning().equals(SINGLE_DISTRIBUTION))
                        .flatMap(stage -> stage.tasks().stream())
                        .filter(task -> task.taskStatus().taskId().attemptId() == attemptId)
                        .findFirst())
                .map(task -> task.taskStatus().nodeId());
    }
}
