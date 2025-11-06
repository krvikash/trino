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
import com.starburstdata.presto.plugin.ai.StarburstAiPlugin;
import com.starburstdata.presto.server.StarburstServerExtensionsModule;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.node.AnnounceNodeInventory;
import io.trino.node.InternalNodeManager;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.TestingTrinoClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findConfigZips;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findWorkerConfigDirectoryName;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.zipInputStreamToMap;
import static io.trino.client.AdditionalClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.connector.CatalogManagerConfig.CatalogMangerKind.STATIC;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTroubleshootingForStaticCatalogs
{
    private static final String DISABLED_CATALOGS = "cassandra";
    private static final String AUTHORIZED_USER = "bob";
    private static final Session SESSION = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setIdentity(Identity.ofUser(AUTHORIZED_USER))
            .setCatalog("tpch")
            .build();

    @TempDir
    private Path tmpDir;

    @Test
    public void testConfigDirectoryIsComplete()
            throws Exception
    {
        // DistributedQueryRunner doesn't allow to change the default 'catalog.management'.
        // That's why we need to build the cluster using TestingTrinoServers.
        try (TestingTrinoServer coordinator = createCoordinator();
                TestingTrinoServer worker = createWorker();
                TestingTrinoClient client = new TestingTrinoClient(coordinator, SESSION)) {
            joinCluster(coordinator, worker);
            TroubleshootingContextManager troubleshootingContextManager = coordinator.getInstance(Key.get(TroubleshootingContextManager.class));
            QueryId queryId = client.execute("SHOW CATALOGS").getQueryId();
            InputStream inputStream = troubleshootingContextManager.getArchive(queryId).orElseThrow().get(10, TimeUnit.SECONDS);
            Unzipped inputsMap = zipInputStreamToMap(inputStream, tmpDir);

            List<Unzipped> coordinatorConfigs = findConfigZips(inputsMap, "coordinator", tmpDir);
            assertThat(coordinatorConfigs.size()).isEqualTo(1);
            assertThat(coordinatorConfigs.getFirst().contents())
                    .hasEntrySatisfying("coordinator/config.properties", value -> assertPropertyExists(value, "coordinator=true"))
                    .hasEntrySatisfying("coordinator/jvm.config", TroubleshootingTestHelper::assertJvmConfig)
                    .hasEntrySatisfying("coordinator/catalog/tpch.properties", value -> assertPropertyExists(value, "connector.name=tpch"))
                    .hasEntrySatisfying("coordinator/catalog/postgres.properties", value -> {
                        assertPropertyExists(value, "connector.name=postgresql");
                        assertPropertyExists(value, "connection-url=jdbc:postgresql://localhost:5432/postgres");
                        assertPropertyExists(value, "connection-user=root");
                        assertPropertyExists(value, "connection-password=[REDACTED]");
                    })
                    .doesNotContainKey("coordinator/catalog/cassandra.properties");

            List<Unzipped> workerConfigs = findConfigZips(inputsMap, "worker-", tmpDir);
            assertThat(workerConfigs.size()).isEqualTo(1);
            String workerConfigDirectory = findWorkerConfigDirectoryName(workerConfigs.getFirst());
            assertThat(workerConfigs.getFirst().contents())
                    .hasEntrySatisfying(workerConfigDirectory + "config.properties", value -> assertPropertyExists(value, "coordinator=false"))
                    .hasEntrySatisfying(workerConfigDirectory + "jvm.config", TroubleshootingTestHelper::assertJvmConfig)
                    .hasEntrySatisfying(workerConfigDirectory + "catalog/tpch.properties", value -> assertPropertyExists(value, "connector.name=tpch"))
                    .hasEntrySatisfying(workerConfigDirectory + "catalog/postgres.properties", value -> {
                        assertPropertyExists(value, "connector.name=postgresql");
                        assertPropertyExists(value, "connection-url=jdbc:postgresql://localhost:5432/postgres");
                        assertPropertyExists(value, "connection-user=root");
                        assertPropertyExists(value, "connection-password=[REDACTED]");
                    })
                    .doesNotContainKey(workerConfigDirectory + "catalog/cassandra.properties");
        }
    }

    private TestingTrinoServer createCoordinator()
    {
        return createNode(nodeBuilder ->
                        nodeBuilder.setCoordinator(true)
                                .addProperty("web-ui.enabled", "true")
                                .addProperty("insights.authorized-users", AUTHORIZED_USER)
                                .addProperty("node-scheduler.include-coordinator", "false"));
    }

    private TestingTrinoServer createWorker()
    {
        return createNode(nodeBuilder -> nodeBuilder.setCoordinator(false));
    }

    private TestingTrinoServer createNode(Consumer<TestingTrinoServer.Builder> nodeModifier)
    {
        String catalogConfigDir = TestQueryTroubleshootingForStaticCatalogs.class.getClassLoader().getResource("catalogs").getFile();
        TestingTrinoServer.Builder nodeBuilder = TestingTrinoServer.builder()
                .setEnvironment("testing")
                .setAdditionalModule(new StarburstServerExtensionsModule())
                .setCatalogMangerKind(STATIC)
                .addProperty("catalog.config-dir", catalogConfigDir)
                .addProperty("catalog.disabled-catalogs", DISABLED_CATALOGS);
        nodeModifier.accept(nodeBuilder);
        TestingTrinoServer node = nodeBuilder.build();
        node.installPlugin(new TpchPlugin());
        node.installPlugin(new GeoPlugin());
        node.installPlugin(new PostgreSqlPlugin());
        node.installPlugin(new StarburstAiPlugin(NOOP_LICENSE_MANAGER));
        node.getInstance(Key.get(ConnectorServicesProvider.class)).loadInitialCatalogs();
        return node;
    }

    private static void joinCluster(TestingTrinoServer coordinator, TestingTrinoServer worker)
    {
        coordinator.getInstance(Key.get(AnnounceNodeInventory.class)).announce(worker.getBaseUrl());
        coordinator.getInstance(Key.get(InternalNodeManager.class)).refreshNodes(true);
    }
}
