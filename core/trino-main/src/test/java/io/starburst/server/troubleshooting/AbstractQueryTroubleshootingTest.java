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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
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

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.server.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
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

    private TroubleshootingManager troubleshootingManager;
    private QueryManager queryManager;
    private String coordinatorId;

    @BeforeClass
    public void localInit()
    {
        troubleshootingManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(TroubleshootingManager.class));
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
        assertThat(data.getStreams()).isEmpty();
    }

    @Test(dataProvider = "unauthorizedSessionsProvider")
    public void testTroubleshootingDataNotAvailableForUnauthorizedUser(Session sessionUnauthorized)
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        TroubleshootingData data = getTroubleshootingDataForQuery(sessionUnauthorized, troubleshootedQuery);
        assertThat(data.getStreams()).isEmpty();
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
        assertThat(data.getStreams()).isPresent();
        Map<String, InputStream> inputsMap = data.getRequiredStreams();

        assertThat(inputsMap)
                .hasEntrySatisfying("session.txt", value -> assertThat(value).hasContent("query_max_memory_per_node = 10MB\n"))
                .hasEntrySatisfying("version.txt", value -> assertThat(value).hasContent("testversion"))
                .hasEntrySatisfying("query.sql", value -> assertThat(value).hasContent(troubleshootedQuery))
                .hasEntrySatisfying("query_plan.txt", value -> assertThat(value).isNotEmpty())
                .hasEntrySatisfying("recordings/coordinator.jfr", value -> assertThat(value).isNotEmpty());

        ObjectMapper mapper = new ObjectMapper();
        mapper.readValue(inputsMap.get("jmx-before.json"), new TypeReference<>() {});
        mapper.readValue(inputsMap.get("jmx-after.json"), new TypeReference<>() {});

        for (String workerId : getNodesProcessingQuery(data.getQueryId())) {
            assertThat(inputsMap).hasEntrySatisfying(
                    "recordings/worker-%s.jfr".formatted(workerId),
                    value -> assertThat(value)
                            .describedAs("worker %s recording", workerId)
                            .isNotEmpty());
        }
    }

    @Test
    public void testTroubleshootingDataAvailableForFailedQuery()
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

        Optional<Map<String, InputStream>> inputs = awaitForTroubleshootingData(queryId);
        assertThat(inputs).isPresent();
        Map<String, InputStream> inputsMap = inputs.orElseThrow();
        assertThat(inputsMap)
                .hasEntrySatisfying("session.txt", value -> assertThat(value).hasContent("query_max_memory_per_node = 10MB\n"))
                .hasEntrySatisfying("version.txt", value -> assertThat(value).hasContent("testversion"))
                .hasEntrySatisfying("query.sql", value -> assertThat(value).hasContent(troubleshootedQuery))
                .doesNotContainKey("query_plan.txt")
                .hasEntrySatisfying("failure_info.txt", value -> assertThat(value).hasContent("""
                        Error code: SCHEMA_NOT_FOUND:45
                        Error message: line 1:15: Schema 'schema' does not exist
                        Error location: ErrorLocation{lineNumber=1, columnNumber=15}
                        Remote host: null"""))
                .hasEntrySatisfying("failure_stack_trace.txt", value -> assertThat(value).isNotEmpty());
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

    private Optional<Map<String, InputStream>> awaitForTroubleshootingData(QueryId queryId)
    {
        try {
            return Optional.of(troubleshootingManager.getInputStreams(queryId).get(10, TimeUnit.SECONDS));
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
        private final Optional<Map<String, InputStream>> streams;

        private TroubleshootingData(QueryId queryId, Optional<Map<String, InputStream>> streams)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.streams = requireNonNull(streams, "streams is null");
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Optional<Map<String, InputStream>> getStreams()
        {
            return streams;
        }

        public Map<String, InputStream> getRequiredStreams()
        {
            return streams.orElseThrow();
        }
    }
}
