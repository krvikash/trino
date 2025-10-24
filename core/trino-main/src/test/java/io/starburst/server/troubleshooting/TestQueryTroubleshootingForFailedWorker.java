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
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.node.InternalNodeManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.protocol.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.getNodesProcessingQuery;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.getTroubleshootingDataForQuery;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTroubleshootingForFailedWorker
{
    private static final String AUTHORIZED_USER = "bob";
    private static final Session SESSION = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setIdentity(Identity.ofUser(AUTHORIZED_USER))
            .setCatalog("tpch")
            .build();

    @TempDir
    private Path tmpDir;

    @Test
    public void testWorkerFailure()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(SESSION, 2);
                TestingTrinoClient client = new TestingTrinoClient(queryRunner.getCoordinator(), SESSION)) {
            QueryId queryId = client.execute("select linenumber, count(*) from tpch.tiny.lineitem l group by 1;").getQueryId();

            TestingTrinoServer workerToTerminate = queryRunner.getServers().stream()
                    .filter(server -> !server.isCoordinator())
                    .findFirst()
                    .orElseThrow();
            String terminatedNodeId = workerToTerminate.getCurrentNode().getNodeIdentifier();
            workerToTerminate.close();
            queryRunner.getCoordinator().getInstance(Key.get(InternalNodeManager.class)).refreshNodes(true);

            Set<String> expectedWorkerIds = getNodesProcessingQuery(queryRunner, queryId).stream()
                    .filter(workerId -> !workerId.equals(terminatedNodeId))
                    .collect(toImmutableSet());
            Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, SESSION, "SHOW CATALOGS", tmpDir);
            assertThat(extractWorkerIds(inputsMap, ".*recordings/worker-(.*).jfr"))
                    .containsExactlyElementsOf(expectedWorkerIds);
            assertThat(extractWorkerIds(inputsMap, ".*traces/opentelemetry-worker-(.*).grpc.gz"))
                    .containsExactlyElementsOf(expectedWorkerIds);
            assertThat(extractWorkerIds(inputsMap, ".*configs/worker-(.*).zip"))
                    .containsExactlyElementsOf(expectedWorkerIds);
        }
    }

    private static List<String> extractWorkerIds(Unzipped inputsMap, String pattern)
    {
        return inputsMap.contents().keySet()
                .stream()
                .map(path -> extractWorkerId(pattern, path))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private static Optional<String> extractWorkerId(String pattern, String path)
    {
        Matcher matcher = Pattern.compile(pattern).matcher(path);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }

    private static DistributedQueryRunner createQueryRunner(Session session, int workerCount)
            throws Exception
    {
        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(session)
                .setCoordinatorProperties(Map.of(
                        "insights.authorized-users", AUTHORIZED_USER))
                .setWorkerCount(workerCount)
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }
}
