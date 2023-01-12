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

import com.google.inject.Key;
import com.starburstdata.presto.server.StarburstQueryRunner;
import com.starburstdata.presto.server.StarburstServerExtensionsModule;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.starburstdata.presto.server.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

public class TestQueryTroubleshooting
        extends AbstractTestQueryFramework
{
    private static final String AUTHORIZED_USER = "bob";
    private static final String NOT_AUTHORIZED_USER = "john";

    protected static final Session SESSION = testSessionBuilder()
            .build();

    protected static final Session TROUBLESHOOTED_SESSION = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "256kB")
            .setIdentity(Identity.forUser(AUTHORIZED_USER).build())
            .build();

    protected static final Session TROUBLESHOOTED_SESSION_UNAUTHORIZED = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "256kB")
            .setIdentity(Identity.forUser(NOT_AUTHORIZED_USER).build())
            .build();

    private TroubleshootingManager troubleshootingManager;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                .setAdditionalModule(new StarburstServerExtensionsModule())
                .setCoordinatorProperties(Map.of("insights.authorized-users", AUTHORIZED_USER, "troubleshooting.max-access-duration", "500ms"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        troubleshootingManager = queryRunner.getCoordinator().getInstance(Key.get(TroubleshootingManager.class));

        return queryRunner;
    }

    @Test
    public void testTroubleshootingDataNotAvailableWithoutCapability()
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        Optional<Map<String, InputStream>> inputs = getTroubleshootingDataForQuery(SESSION, troubleshootedQuery);
        assertThat(inputs).isEmpty();
    }

    @Test
    public void testTroubleshootingDataNotAvailableForUnauthorizedUser()
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        Optional<Map<String, InputStream>> inputs = getTroubleshootingDataForQuery(TROUBLESHOOTED_SESSION_UNAUTHORIZED, troubleshootedQuery);
        assertThat(inputs).isEmpty();
    }

    @Test
    public void testTroubleshootingDataAvailableForAuthorizedUser()
    {
        String troubleshootedQuery = "SHOW CATALOGS";
        Optional<Map<String, InputStream>> inputs = getTroubleshootingDataForQuery(TROUBLESHOOTED_SESSION, troubleshootedQuery);
        assertThat(inputs).isPresent();
        Map<String, InputStream> inputsMap = inputs.get();
        assertThat(inputsMap.get("session.txt")).hasContent("query_max_memory_per_node = 256kB\n");
        assertThat(inputsMap.get("version.txt")).hasContent("testversion");
        assertThat(inputsMap.get("query.sql")).hasContent(troubleshootedQuery);
        assertThat(inputsMap.get("query_plan.txt")).isNotEmpty();
    }

    @Test
    public void testTroubleshootingDataAvailableForFailedQuery()
    {
        String troubleshootedQuery = "SELECT * FROM not valid query";
        QueryId queryId = null;

        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), TROUBLESHOOTED_SESSION)) {
            client.execute(troubleshootedQuery);
            fail("Query should fail");
        }
        catch (QueryFailedException e) {
            queryId = e.getQueryId();
        }

        Optional<Map<String, InputStream>> inputs = awaitForTroubleshootingData(queryId);
        assertThat(inputs).isPresent();

        Map<String, InputStream> inputsMap = inputs.get();
        assertThat(inputsMap.get("session.txt")).hasContent("query_max_memory_per_node = 256kB\n");
        assertThat(inputsMap.get("version.txt")).hasContent("testversion");
        assertThat(inputsMap.get("query.sql")).hasContent(troubleshootedQuery);
        assertThat(inputsMap.get("failure_info.txt")).hasContent("""
                Error code: SYNTAX_ERROR:1
                Failure message: line 1:15: mismatched input 'not'. Expecting: '(', 'LATERAL', 'TABLE', 'UNNEST', <identifier>
                Failure type: io.trino.sql.parser.ParsingException""");
    }

    @Test(timeOut = 10_000)
    public void testTroubleshootingIsRemovedAfterDuration()
            throws InterruptedException
    {
        String exampleQuery = "SELECT count(comment) FROM tpch.tiny.lineitem";
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), TROUBLESHOOTED_SESSION)) {
            ResultWithQueryId<MaterializedResult> result = client.execute(exampleQuery);
            assertThat(awaitForTroubleshootingData(result.getQueryId())).isPresent();
            Thread.sleep(700);
            assertThat(awaitForTroubleshootingData(result.getQueryId())).isEmpty();
        }
    }

    private Optional<Map<String, InputStream>> getTroubleshootingDataForQuery(Session session, @Language("sql") String query)
    {
        try (TestingTrinoClient client = new TestingTrinoClient(getDistributedQueryRunner().getCoordinator(), session)) {
            ResultWithQueryId<MaterializedResult> result = client.execute(query);
            return awaitForTroubleshootingData(result.getQueryId());
        }
    }

    private Optional<Map<String, InputStream>> awaitForTroubleshootingData(QueryId queryId)
    {
        try {
            return Optional.of(troubleshootingManager.getInputStreams(queryId).get(10, TimeUnit.SECONDS));
        }
        catch (ExecutionException | TimeoutException e) {
            if (e.getCause() instanceof NoSuchElementException) {
                return Optional.empty();
            }
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
