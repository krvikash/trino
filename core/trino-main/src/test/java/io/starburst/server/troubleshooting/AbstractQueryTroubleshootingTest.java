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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.server.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.zipInputStreamToMap;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class AbstractQueryTroubleshootingTest
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(AbstractQueryTroubleshootingTest.class);

    protected static final String AUTHORIZED_USER = "bob";
    protected static final String NOT_AUTHORIZED_USER = "john";

    protected static final Session SESSION = testSessionBuilder()
            .build();

    private final Session troubleshootedSession = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "10MB")
            .setIdentity(getIdentityOfAuthorizedUser())
            .setCatalog("tpch")
            .build();

    private static final Session TROUBLESHOOTED_SESSION_UNAUTHORIZED_TEMPLATE = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "256kB")
            .build();

    private TroubleshootingContextManager troubleshootingContextManager;
    private QueryManager queryManager;
    private String coordinatorId;

    @BeforeClass
    public void localInit()
    {
        troubleshootingContextManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(TroubleshootingContextManager.class));
        queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        coordinatorId = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(InternalNodeManager.class)).getCurrentNode().getNodeIdentifier();
    }

    @Override
    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    protected Identity getIdentityOfAuthorizedUser()
    {
        return Identity.ofUser(AUTHORIZED_USER);
    }

    protected List<Identity> getIdentitiesOfUnauthorizedUsers()
    {
        return ImmutableList.of(Identity.ofUser(NOT_AUTHORIZED_USER));
    }

    @Test
    public void testTroubleshootingDataNotAvailableWithoutCapability()
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        TroubleshootingData data = getTroubleshootingDataForQuery(SESSION, troubleshootedQuery);
        assertThat(data.getStream()).isEmpty();
    }

    @Test(dataProvider = "unauthorizedSessionsProvider")
    public void testTroubleshootingDataNotAvailableForUnauthorizedUser(Session sessionUnauthorized)
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        TroubleshootingData data = getTroubleshootingDataForQuery(sessionUnauthorized, troubleshootedQuery);
        assertThat(data.getStream()).isEmpty();
    }

    @DataProvider
    public Object[][] unauthorizedSessionsProvider()
    {
        return getIdentitiesOfUnauthorizedUsers().stream()
                .map(unauthorizedIdentity -> Session.builder(TROUBLESHOOTED_SESSION_UNAUTHORIZED_TEMPLATE)
                        .setIdentity(unauthorizedIdentity)
                        .build())
                .collect(toDataProvider());
    }

    @Test
    public void testTroubleshootingDataAvailableForAuthorizedUser()
            throws Exception
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        TroubleshootingData data = getTroubleshootingDataForQuery(troubleshootedSession, troubleshootedQuery);
        assertThat(data.getStream()).isPresent();
        Unzipped inputsMap = zipInputStreamToMap(data.getRequiredStreams().get());

        assertThat(inputsMap.zipEntryContents)
                .hasEntrySatisfying(getPath(data, "version.txt"), value -> assertThat(byteToString(value)).contains("testversion"))
                .hasEntrySatisfying(getPath(data, "recordings/coordinator.jfr"), value -> assertThat(value).isNotEmpty());

        ObjectMapper mapper = new ObjectMapper();
        mapper.readValue(inputsMap.zipEntryContents.get(getPath(data, "jmx/metrics-before.json")), new TypeReference<>() {});
        mapper.readValue(inputsMap.zipEntryContents.get(getPath(data, "jmx/metrics-after.json")), new TypeReference<>() {});

        JsonNode queryInfo = mapper.readTree(inputsMap.zipEntryContents.get(getPath(data, "query.json")));
        assertThat(queryInfo.get("query").asText()).isEqualTo(troubleshootedQuery);
        assertThat(queryInfo.get("session").get("systemProperties").get("query_max_memory_per_node").asText()).isEqualTo("10MB");
        assertThat(queryInfo.get("outputStage").get("plan")).isNotEmpty();

        for (String workerId : getNodesProcessingQuery(data.getQueryId())) {
            assertThat(inputsMap.zipEntryContents).hasEntrySatisfying(
                    getPath(data, "recordings/worker-%s.jfr").formatted(workerId),
                    value -> assertThat(value)
                            .describedAs("worker %s recording", workerId)
                            .isNotEmpty());
        }
    }

    @Test
    public void testTroubleshootingDataAvailableForFailedQuery()
            throws IOException
    {
        String troubleshootedQuery = "SELECT * FROM table_does_not_exist";
        QueryId queryId = null;

        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), troubleshootedSession)) {
            client.execute(troubleshootedQuery);
            fail("Query should fail");
        }
        catch (QueryFailedException e) {
            queryId = e.getQueryId();
        }

        Optional<InputStream> inputs = awaitForTroubleshootingData(queryId);
        assertThat(inputs).isPresent();
        Unzipped inputsMap = zipInputStreamToMap(inputs.get());
        assertThat(inputsMap.zipEntryContents)
                .hasEntrySatisfying(getPath(queryId, "version.txt"), value -> assertThat(byteToString(value)).contains("testversion"))
                .doesNotContainKey(getPath(queryId, "query_plan.txt"));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode queryInfo = mapper.readTree(inputsMap.zipEntryContents.get(getPath(queryId, "query.json")));
        assertThat(queryInfo.get("query").asText()).isEqualTo(troubleshootedQuery);
        assertThat(queryInfo.get("session").get("systemProperties").get("query_max_memory_per_node").asText()).isEqualTo("10MB");
        assertThat(queryInfo.get("failureInfo").get("errorCode").get("code").asText()).isEqualTo("45");
        assertThat(queryInfo.get("failureInfo").get("errorCode").get("name").asText()).isEqualTo("SCHEMA_NOT_FOUND");
        assertThat(queryInfo.get("failureInfo").get("errorLocation").get("lineNumber").asText()).isEqualTo("1");
        assertThat(queryInfo.get("failureInfo").get("errorLocation").get("columnNumber").asText()).isEqualTo("15");
        assertThat(queryInfo.get("failureInfo").get("message").asText()).isEqualTo("line 1:15: Schema 'schema' does not exist");
        assertThat(queryInfo.get("failureInfo").get("stack").size()).isEqualTo(34);
    }

    @Test
    public void testTroubleshootingIsRemovedAfterDuration()
    {
        String exampleQuery = "SELECT count(comment) FROM tpch.tiny.lineitem";
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), troubleshootedSession)) {
            ResultWithQueryId<MaterializedResult> result = client.execute(exampleQuery);
            assertThat(awaitForTroubleshootingData(result.getQueryId())).isPresent();

            assertEventually(Duration.valueOf("30s"), () -> assertThat(awaitForTroubleshootingData(result.getQueryId())).isEmpty());
        }
    }

    private TroubleshootingData getTroubleshootingDataForQuery(Session session, @Language("SQL") String query)
    {
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
            ResultWithQueryId<MaterializedResult> result = client.execute(query);
            return new TroubleshootingData(result.getQueryId(), awaitForTroubleshootingData(result.getQueryId()));
        }
    }

    private Optional<InputStream> awaitForTroubleshootingData(QueryId queryId)
    {
        try {
            return Optional.of(troubleshootingContextManager.getArchive(queryId).get(10, TimeUnit.SECONDS));
        }
        catch (ExecutionException | TimeoutException e) {
            log.error(e, "Awaiting troubleshooting data failed");
            return Optional.empty();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Set<String> getNodesProcessingQuery(QueryId queryId)
    {
        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            return queryInfo.getOutputStage().map(this::getNodeIdsProcessingQuery).orElse(ImmutableSet.of());
        }
        catch (Exception e) {
            return Set.of();
        }
    }

    private List<TaskInfo> gatherAllTasks(StageInfo stageInfo)
    {
        ImmutableList.Builder<TaskInfo> builder = ImmutableList.builder();
        builder.addAll(stageInfo.getTasks());
        for (StageInfo subStage : stageInfo.getSubStages()) {
            builder.addAll(gatherAllTasks(subStage));
        }
        return builder.build();
    }

    private Set<String> getNodeIdsProcessingQuery(StageInfo outputStage)
    {
        List<TaskInfo> tasks = gatherAllTasks(outputStage);

        return tasks.stream()
                .map(TaskInfo::getTaskStatus)
                .map(TaskStatus::getNodeId)
                .filter(this::isNotCoordinator)
                .collect(toImmutableSet());
    }

    private boolean isNotCoordinator(String nodeId)
    {
        return !nodeId.equalsIgnoreCase(coordinatorId);
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
        return String.format("%s/%s", queryId.getId(), suffix);
    }

    private static String byteToString(byte[] input)
    {
        return new String(input, UTF_8);
    }
}
