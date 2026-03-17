/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingTrinoClient;
import org.assertj.core.api.Condition;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.troubleshooting.DistributedTroubleshootingTestHelper.getNodesProcessingQuery;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findConfigZips;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findWorkerConfigDirectoryName;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.zipInputStreamToMap;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.client.AdditionalClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@ExtendWith(SoftAssertionsExtension.class)
// run in the same thread to avoid multiple big JFR profiles causing OOM
@Execution(SAME_THREAD)
public abstract class AbstractQueryTroubleshootingTest
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(AbstractQueryTroubleshootingTest.class);

    protected static final String AUTHORIZED_USER = "bob";
    protected static final String NOT_AUTHORIZED_USER = "john";

    protected static final Map<String, String> POSTGRES_CATALOG_PROPERTIES = ImmutableMap.of(
            "connection-url", "jdbc:postgresql://localhost:5432/postgres",
            "connection-user", "root",
            "connection-password", "secret");

    protected static final Session SESSION = testSessionBuilder()
            .build();

    private static final Session TROUBLESHOOTED_SESSION_AUTHORIZED_TEMPLATE = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "10MB")
            .setCatalog("tpch")
            .build();

    private static final Session TROUBLESHOOTED_SESSION_UNAUTHORIZED_TEMPLATE = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1MB")
            .build();

    private TroubleshootingContextManager troubleshootingContextManager;
    private QueryManager queryManager;
    @TempDir
    private Path tmpDir;

    @BeforeAll
    public void localInit()
    {
        troubleshootingContextManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(TroubleshootingContextManager.class));
        queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
    }

    @Override
    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    protected List<Identity> getIdentitiesOfAuthorizedUsers()
    {
        return ImmutableList.of(Identity.ofUser(AUTHORIZED_USER));
    }

    protected List<Identity> getIdentitiesOfUnauthorizedUsers()
    {
        return ImmutableList.of(Identity.ofUser(NOT_AUTHORIZED_USER));
    }

    protected abstract void assertAccessControlConfig(Unzipped coordinatorConfig);

    @Test
    public void testTroubleshootingDataNotAvailableWithoutCapability()
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        TroubleshootingData data = getTroubleshootingDataForQuery(SESSION, troubleshootedQuery);
        assertThat(data.getStream()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("getIdentitiesOfUnauthorizedUsers")
    public void testTroubleshootingDataNotAvailableForUnauthorizedUser(Identity unauthorizedUserIdentity)
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        Session session = unauthorizedSession(unauthorizedUserIdentity);
        TroubleshootingData data = getTroubleshootingDataForQuery(session, troubleshootedQuery);
        assertThat(data.getStream()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("getIdentitiesOfAuthorizedUsers")
    public void testTroubleshootingDataAvailableForAuthorizedUser(Identity authorizedUserIdentity, SoftAssertions softly)
            throws Exception
    {
        String troubleshootedQuery = "select linenumber, count(*) from tpch.tiny.lineitem l group by 1";
        Session session = authorizedSession(authorizedUserIdentity);
        TroubleshootingData data = getTroubleshootingDataForQuery(session, troubleshootedQuery);
        assertCompleteTroubleshootingData(softly, data, troubleshootedQuery);

        TroubleshootingData dataReadAgain = awaitForTroubleshootingData(data.getQueryId());
        assertCompleteTroubleshootingData(softly, dataReadAgain, troubleshootedQuery);
    }

    private void assertCompleteTroubleshootingData(SoftAssertions softly, TroubleshootingData data, String troubleshootedQuery)
            throws IOException
    {
        assertThat(data.getStream()).isPresent();
        Unzipped inputsMap = zipInputStreamToMap(data.getRequiredStreams().get(), tmpDir);

        softly.assertThat(inputsMap.contents())
                .hasEntrySatisfying(getPath(data, "version.txt"), value -> softly.assertThat(byteToString(value)).contains("testversion"))
                .hasEntrySatisfying(getPath(data, "query_plan.txt"), value -> assertThat(value).isNotEmpty())
                .hasEntrySatisfying(getPath(data, "recordings/coordinator.jfr"), value -> softly.assertThat(value).isNotEmpty())
                .hasEntrySatisfying(getPath(data, "traces/opentelemetry-coordinator.grpc.gz"), value -> softly.assertThat(value).isNotEmpty());

        ObjectMapper mapper = new ObjectMapper();
        mapper.readValue(inputsMap.contents().get(getPath(data, "jmx/metrics-before.json")), new TypeReference<>() {});
        mapper.readValue(inputsMap.contents().get(getPath(data, "jmx/metrics-after.json")), new TypeReference<>() {});

        JsonNode queryInfo = mapper.readTree(inputsMap.contents().get(getPath(data, "query.json")));
        softly.assertThat(queryInfo.get("query").asText()).isEqualTo(troubleshootedQuery);
        softly.assertThat(queryInfo.get("session").get("systemProperties").get("query_max_memory_per_node").asText()).isEqualTo("10MB");
        JsonNode outputStageId = queryInfo.get("stages").get("outputStageId");
        softly.assertThat(outputStageId).isNotNull();
        Optional<JsonNode> planNodeMaybe = queryInfo.get("stages").get("stages").valueStream()
                .filter(stage -> outputStageId.equals(stage.get("stageId")))
                .map(stage -> stage.get("plan"))
                .findAny();
        softly.assertThat(planNodeMaybe).hasValueSatisfying(planNode -> softly.assertThat(planNode).isNotEmpty());

        Set<String> nodesProcessingQuery = getNodesProcessingQuery(getDistributedQueryRunner(), data.getQueryId());
        for (String workerId : nodesProcessingQuery) {
            softly.assertThat(inputsMap.contents()).hasEntrySatisfying(
                    getPath(data, "recordings/worker-%s.jfr").formatted(workerId),
                    value -> softly.assertThat(value)
                            .describedAs("worker %s recording", workerId)
                            .isNotEmpty());
        }

        assertCompleteConfigDirectory(softly, inputsMap, nodesProcessingQuery);

        try (TestingJaegerService testingJaegerService = TestingJaegerService.createStarted()) {
            boolean exportSuccessful = testingJaegerService.exportOpenTelemetryData(inputsMap.contents().get(getPath(data, "traces/opentelemetry-coordinator.grpc.gz")));
            assertThat(exportSuccessful).isTrue();
            assertThat(inputsMap.contents().keySet()).areExactly(getWorkerCount(), new Condition<>(key -> key.contains("traces/opentelemetry-worker"), "worker trace"));
            List<String> workerSpans = inputsMap.contents()
                    .keySet()
                    .stream()
                    .filter(key -> key.contains("traces/opentelemetry-worker"))
                    .collect(toImmutableList());
            for (String workerSpan : workerSpans) {
                boolean workerExportSuccessful = testingJaegerService.exportOpenTelemetryData(inputsMap.contents().get(workerSpan));
                assertThat(workerExportSuccessful).isTrue();
            }

            JsonNode traceSpans = testingJaegerService.getTraceSpans(data.getQueryId());
            assertThat(traceSpans).isNotNull();
            assertThat(traceSpans.size()).isGreaterThan(70);
            boolean workerSpansIncluded = stream(spliteratorUnknownSize(traceSpans.elements(), ORDERED), false)
                    // split (leaf) span is executed on a worker
                    .anyMatch(span -> "split (leaf)".equals(span.get("operationName").asText()));
            assertThat(workerSpansIncluded).isTrue();
        }
    }

    private void assertCompleteConfigDirectory(SoftAssertions softly, Unzipped inputsMap, Set<String> nodesProcessingQuery)
    {
        List<Unzipped> coordinatorConfigs = findConfigZips(inputsMap, "coordinator", tmpDir);
        assertThat(coordinatorConfigs.size()).isEqualTo(1);
        assertThat(coordinatorConfigs.getFirst().contents())
                .hasEntrySatisfying("coordinator/config.properties", value -> assertPropertyExists(value, "coordinator=true"))
                .hasEntrySatisfying("coordinator/jvm.config", TroubleshootingTestHelper::assertJvmConfig)
                .hasEntrySatisfying("coordinator/catalog/tpch.properties", value -> softly.assertThat(value).isEmpty())
                .hasEntrySatisfying("coordinator/catalog/postgres.properties", value -> {
                    assertPropertyExists(value, "connection-url=jdbc:postgresql://localhost:5432/postgres");
                    assertPropertyExists(value, "connection-user=root");
                    assertPropertyExists(value, "connection-password=[REDACTED]");
                });
        assertAccessControlConfig(coordinatorConfigs.getFirst());

        List<Unzipped> workerConfigs = findConfigZips(inputsMap, "worker-", tmpDir);
        if (!nodesProcessingQuery.isEmpty()) {
            softly.assertThat(workerConfigs.size()).isEqualTo(1);
            String workerConfigDirectory = findWorkerConfigDirectoryName(workerConfigs.getFirst());
            assertThat(workerConfigs.getFirst().contents())
                    .hasEntrySatisfying(workerConfigDirectory + "config.properties", value -> assertPropertyExists(value, "coordinator=false"))
                    .hasEntrySatisfying(workerConfigDirectory + "jvm.config", TroubleshootingTestHelper::assertJvmConfig)
                    .doesNotContainKey(workerConfigDirectory + "catalog/tpch.properties")
                    .doesNotContainKey(workerConfigDirectory + "catalog/postgres.properties");
        }
        else {
            softly.assertThat(workerConfigs.size()).isEqualTo(0);
        }
    }

    @ParameterizedTest
    @MethodSource("getIdentitiesOfAuthorizedUsers")
    public void testTroubleshootingDataAvailableForFailedQuery(Identity authorizedUserIdentity, SoftAssertions softly)
            throws IOException
    {
        String troubleshootedQuery = "SELECT * FROM table_does_not_exist";
        QueryId queryId = null;

        Session session = authorizedSession(authorizedUserIdentity);
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
            client.execute(troubleshootedQuery);
            fail("Query should fail");
        }
        catch (QueryFailedException e) {
            queryId = e.getQueryId();
        }

        Unzipped inputsMap = zipInputStreamToMap(awaitForTroubleshootingData(queryId).getStream().get(), tmpDir);
        softly.assertThat(inputsMap.contents())
                .hasEntrySatisfying(getPath(queryId, "version.txt"), value -> assertThat(byteToString(value)).contains("testversion"))
                .doesNotContainKey(getPath(queryId, "query_plan.txt"));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode queryInfo = mapper.readTree(inputsMap.contents().get(getPath(queryId, "query.json")));
        softly.assertThat(queryInfo.get("query").asText()).isEqualTo(troubleshootedQuery);
        softly.assertThat(queryInfo.get("session").get("systemProperties").get("query_max_memory_per_node").asText()).isEqualTo("10MB");
        softly.assertThat(queryInfo.get("failureInfo").get("errorCode").get("code").asText()).isEqualTo("45");
        softly.assertThat(queryInfo.get("failureInfo").get("errorCode").get("name").asText()).isEqualTo("SCHEMA_NOT_FOUND");
        softly.assertThat(queryInfo.get("failureInfo").get("errorLocation").get("lineNumber").asText()).isEqualTo("1");
        softly.assertThat(queryInfo.get("failureInfo").get("errorLocation").get("columnNumber").asText()).isEqualTo("15");
        softly.assertThat(queryInfo.get("failureInfo").get("message").asText()).isEqualTo("line 1:15: Schema 'schema' does not exist");
        softly.assertThat(queryInfo.get("failureInfo").get("stack").size()).isEqualTo(34);
    }

    @ParameterizedTest
    @MethodSource("getIdentitiesOfAuthorizedUsers")
    public void testTroubleshootingQueryCollectsOnlyItsOwnTrace(Identity authorizedUserIdentity, SoftAssertions softly)
            throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Session session = testSessionBuilder()
                .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "100MB")
                .setIdentity(authorizedUserIdentity)
                .setCatalog("tpch")
                .build();
        // random number added to SELECT clause, to have a unique query string
        // which won't interfere with other test cases run in other threads
        String slowQuery = String.format("SELECT *, %d FROM tpch.sf10.orders", ThreadLocalRandom.current().nextLong());
        String fastQuery = "SHOW CATALOGS";
        AtomicReference<QueryId> slowQueryId = new AtomicReference<>();

        try {
            Future<QueryId> slowQueryIdFuture = runQueryOnExecutor(executorService, session, slowQuery);
            assertEventually(Duration.valueOf("10s"), () -> {
                slowQueryId.set(getQueryId(slowQuery));
                assertThat(slowQueryId.get()).isNotNull();
            });
            QueryId fastQueryId = runQueryOnExecutor(executorService, session, fastQuery).get();

            assertThat(slowQueryIdFuture.isDone()).isFalse();
            queryManager.cancelQuery(slowQueryId.get());

            TroubleshootingData fastQueryTroubleshootingData = awaitForTroubleshootingData(fastQueryId);
            assertThat(fastQueryTroubleshootingData.getStream()).isPresent();

            Unzipped fastQueryInputsMap = zipInputStreamToMap(fastQueryTroubleshootingData.getRequiredStreams().get(), tmpDir);
            softly.assertThat(fastQueryInputsMap.contents())
                    .hasEntrySatisfying(getPath(fastQueryTroubleshootingData, "traces/opentelemetry-coordinator.grpc.gz"), value -> softly.assertThat(value).isNotEmpty());

            try (TestingJaegerService testingJaegerService = TestingJaegerService.createStarted()) {
                boolean fastQueryExportSuccessful = testingJaegerService.exportOpenTelemetryData(fastQueryInputsMap.contents().get(getPath(fastQueryTroubleshootingData, "traces/opentelemetry-coordinator.grpc.gz")));
                assertThat(fastQueryExportSuccessful).isTrue();

                JsonNode traces = testingJaegerService.getTraces();
                assertThat(traces.size()).isEqualTo(1);

                TroubleshootingData slowQueryTroubleshootingData = awaitForTroubleshootingData(slowQueryId.get());
                assertThat(slowQueryTroubleshootingData.getStream()).isPresent();

                Unzipped slowQueryInputsMap = zipInputStreamToMap(slowQueryTroubleshootingData.getRequiredStreams().get(), tmpDir);
                softly.assertThat(slowQueryInputsMap.contents())
                        .hasEntrySatisfying(getPath(slowQueryTroubleshootingData, "traces/opentelemetry-coordinator.grpc.gz"), value -> softly.assertThat(value).isNotEmpty());

                boolean slowQueryExportSuccessful = testingJaegerService.exportOpenTelemetryData(slowQueryInputsMap.contents().get(getPath(slowQueryTroubleshootingData, "traces/opentelemetry-coordinator.grpc.gz")));
                assertThat(slowQueryExportSuccessful).isTrue();

                JsonNode slowQueryTraceSpans = testingJaegerService.getTraceSpans(slowQueryId.get());
                assertThat(slowQueryTraceSpans).isNotNull();

                traces = testingJaegerService.getTraces();
                assertThat(traces.size()).isEqualTo(2);
            }
        }
        finally {
            if (slowQueryId.get() != null && !queryManager.getFullQueryInfo(slowQueryId.get()).getState().isDone()) {
                queryManager.cancelQuery(slowQueryId.get());
            }
            executorService.shutdownNow();
        }
    }

    @ParameterizedTest
    @MethodSource("getIdentitiesOfAuthorizedUsers")
    public void testTroubleshootingIsRemovedAfterDuration(Identity authorizedUserIdentity)
    {
        String exampleQuery = "SELECT count(comment) FROM tpch.tiny.lineitem";
        Session session = authorizedSession(authorizedUserIdentity);
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
            ResultWithQueryId<MaterializedResult> result = client.execute(exampleQuery);
            assertThat(awaitForTroubleshootingData(result.getQueryId()).getStream()).isPresent();

            assertEventually(Duration.valueOf("30s"), () -> {
                assertThat(awaitForTroubleshootingData(result.getQueryId()).getStream()).isEmpty();
            });
        }
    }

    private int getWorkerCount()
    {
        return toIntExact(getDistributedQueryRunner().getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .count());
    }

    private TroubleshootingData getTroubleshootingDataForQuery(Session session, @Language("SQL") String query)
    {
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
            return awaitForTroubleshootingData(client.execute(query).getQueryId());
        }
    }

    private TroubleshootingData awaitForTroubleshootingData(QueryId queryId)
    {
        return new TroubleshootingData(queryId, troubleshootingContextManager.getArchive(queryId)
                .map(future -> {
                    try {
                        return future.get(10, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException | TimeoutException e) {
                        log.error(e, "Awaiting troubleshooting data failed");
                        return null;
                    }
                }));
    }

    private Session authorizedSession(Identity identity)
    {
        return Session.builder(TROUBLESHOOTED_SESSION_AUTHORIZED_TEMPLATE)
                .setIdentity(identity)
                .build();
    }

    private Session unauthorizedSession(Identity identity)
    {
        return Session.builder(TROUBLESHOOTED_SESSION_UNAUTHORIZED_TEMPLATE)
                .setIdentity(identity)
                .build();
    }

    private static class TroubleshootingData
    {
        private final QueryId queryId;
        private final Optional<InputStream> stream;

        private TroubleshootingData(QueryId queryId, Optional<InputStream> stream)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.stream = requireNonNull(stream, "stream is null");
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Optional<InputStream> getStream()
        {
            return stream;
        }

        public Optional<InputStream> getRequiredStreams()
        {
            return stream;
        }
    }

    private static String getPath(TroubleshootingData data, String suffix)
    {
        return getPath(data.getQueryId(), suffix);
    }

    private static String getPath(QueryId queryId, String suffix)
    {
        return String.format("%s/%s", queryId.id(), suffix);
    }

    private static String byteToString(byte[] input)
    {
        return new String(input, UTF_8);
    }

    private Future<QueryId> runQueryOnExecutor(ExecutorService executorService, Session session, @Language("SQL") String query)
    {
        return executorService.submit(() -> {
            try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
                return client.execute(query).getQueryId();
            }
        });
    }

    private QueryId getQueryId(String query)
    {
        return queryManager.getQueries().stream()
                .filter(queryInfo -> queryInfo.getQuery().equals(query))
                .findFirst()
                .map(BasicQueryInfo::getQueryId)
                .orElse(null);
    }
}
