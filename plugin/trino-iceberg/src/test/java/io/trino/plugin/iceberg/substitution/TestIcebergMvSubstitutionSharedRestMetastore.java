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
package io.trino.plugin.iceberg.substitution;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.starburst.materialization.metastore.client.MaterializationMetastoreClientConfig;
import io.starburst.materialization.metastore.client.RequestAuthenticator;
import io.starburst.materialization.metastore.server.TestingMaterializationMetastoreServer;
import io.trino.Session;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Verifies that two clusters sharing the same Iceberg file metastore <b>and</b> the same REST-backed
 * materialization metastore converge: an MV created, refreshed, dropped or replaced on one cluster becomes
 * visible for substitution on the other once that cluster's materialization index is rebuilt from the shared
 * metastore.
 * <p>
 * The primary cluster's DDL write-throughs the change to the shared metastore immediately, while the secondary
 * cluster only learns about it through the periodic index rebuild scheduled every
 * {@code materialized-view-substitution.metastore-refresh-interval} (set to a small value here). Because that
 * rebuild is asynchronous, the cross-cluster assertions are wrapped in {@link
 * io.trino.testing.assertions.Assert#assertEventually}; the convergence timeout is generous while the refresh
 * interval is short, so the test is deterministic rather than timing-sensitive. Query results are additionally
 * asserted to be correct at every point — even in the window where the secondary's index is stale — because a
 * stale index makes the secondary fall back to the base table instead of scanning an outdated storage snapshot.
 * <p>
 * Both clusters point their {@code local://} root at the same on-disk path and use the {@code
 * TESTING_FILE_METASTORE} catalog type against the same {@code local:///iceberg-catalog} directory, so their
 * {@code FileHiveMetastore} instances share state through the filesystem; {@code iceberg.unique-table-location=false}
 * and TTL-0 metastore caching mirror {@link TestIcebergMvSubstitutionExternalCluster} so each cluster sees the
 * other's Iceberg changes.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionSharedRestMetastore
        extends AbstractTestQueryFramework
{
    private static final String MV_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";
    private static final Duration CONVERGENCE_TIMEOUT = new Duration(30, SECONDS);

    private static final Map<String, String> CATALOG_PROPERTIES = Map.of(
            "iceberg.catalog.type", "TESTING_FILE_METASTORE",
            "hive.metastore.catalog.dir", "local:///iceberg-catalog",
            "iceberg.hive-catalog-name", "hive",
            "iceberg.unique-table-location", "false",
            // Both clusters' CachingHiveMetastore must read fresh on every call so each cluster sees the
            // on-disk state written by the other. TTL=0 plus disabling negative caching prevents either side
            // from holding a stale or not-found-cached entry for the MV storage table.
            "hive.metastore-cache-ttl", "0s",
            "hive.metastore-cache.cache-missing", "false");

    private DistributedQueryRunner secondaryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Shared REST materialization metastore backing both clusters. Registered with closeAfterClass so it is
        // torn down after the query runners (LIFO), keeping their background index-refresh threads from hitting a
        // dead server during shutdown.
        PostgreSQLContainer metastoreDb = closeAfterClass(new PostgreSQLContainer("postgres:16"));
        metastoreDb.start();
        TestingMaterializationMetastoreServer metastoreServer = closeAfterClass(new TestingMaterializationMetastoreServer(
                metastoreDb.getJdbcUrl(),
                metastoreDb.getUsername(),
                metastoreDb.getPassword()));

        // Both coordinators must use the same baseDataDir so their TestingIcebergPlugin instances resolve
        // `local://` to the same on-disk root, giving each cluster a FileHiveMetastore pointed at the same
        // metastore directory.
        Path sharedDataDir = Files.createTempDirectory("iceberg-mv-substitution-shared-rest");
        sharedDataDir.toFile().deleteOnExit();

        DistributedQueryRunner primary = createCluster(sharedDataDir, metastoreServer.baseUri());
        // Schema location must live outside the metastore catalog dir (see TestIcebergMvSubstitutionExternalCluster).
        primary.execute("CREATE SCHEMA tpch WITH (location = 'local:///iceberg-data/tpch')");

        secondaryRunner = closeAfterClass(createCluster(sharedDataDir, metastoreServer.baseUri()));

        return primary;
    }

    private DistributedQueryRunner createCluster(Path sharedDataDir, URI metastoreUri)
            throws Exception
    {
        DistributedQueryRunner runner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_CATALOG)
                        .setSchema("tpch")
                        .build())
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        binder.bind(RequestAuthenticator.class).toInstance(_ -> {});
                        configBinder(binder).bindConfigDefaults(MaterializationMetastoreClientConfig.class, config -> config.setMetastoreId("id"));
                    }
                })
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addCoordinatorProperty("materialization.metastore.type", "REST")
                .addCoordinatorProperty("materialization.metastore.base-uri", metastoreUri.toString())
                // Small interval so the secondary's index rebuilds quickly; assertEventually below tolerates the delay.
                .addCoordinatorProperty("materialized-view-substitution.metastore-refresh-interval", "1s")
                .setBaseDataDir(Optional.of(sharedDataDir))
                .build();
        runner.installPlugin(new TestingIcebergPlugin(sharedDataDir));
        runner.createCatalog(ICEBERG_CATALOG, "iceberg", CATALOG_PROPERTIES);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch");
        return runner;
    }

    @BeforeAll
    public void setUpData()
    {
        // Writable copy seeded from TPCH; same shape as AbstractIcebergMvSubstitutionTest.
        assertUpdate("CREATE TABLE orders AS SELECT " +
                "orderkey, " +
                "custkey, " +
                "orderdate, " +
                "CAST(totalprice AS DECIMAL(12, 2)) AS totalprice, " +
                "CAST(orderstatus AS VARCHAR) AS orderstatus " +
                "FROM tpch.tiny.orders", 15000);
    }

    @Test
    public void testNewMaterializedViewPropagatesToOtherCluster()
    {
        CatalogSchemaTableName mvName = mvName("mv_shared_new_");
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            // Primary: create + refresh. Write-through publishes the definition to the shared metastore and the
            // primary substitutes immediately.
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);
            assertSubstituted(getDistributedQueryRunner(), sessionWithSubstitution(), "SELECT * FROM orders");

            // Secondary: its index starts without the MV (stale). Once it rebuilds from the shared metastore it
            // sees the definition, whose pinned storage snapshot matches the shared Iceberg table, so substitution
            // fires and returns correct data.
            assertEventually(CONVERGENCE_TIMEOUT, () -> {
                assertSubstituted(secondaryRunner, secondarySessionWithSubstitution(), "SELECT * FROM orders");
                assertThat(secondaryRunner.execute(secondarySessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue())
                        .isEqualTo(baseCount);
            });
        }
        finally {
            getDistributedQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMaterializedViewDataUpdatePropagatesToOtherCluster()
    {
        CatalogSchemaTableName mvName = mvName("mv_shared_update_");
        // Synthetic orderkey outside TPCH's value range so cleanup only removes this test's row.
        long syntheticOrderKey = 99_999_997L;
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);
            // Both clusters converge on substituting the initial snapshot.
            assertEventually(CONVERGENCE_TIMEOUT, () ->
                    assertSubstituted(secondaryRunner, secondarySessionWithSubstitution(), "SELECT * FROM orders"));

            // Primary: add a row to the base table and refresh, advancing the storage table to a new snapshot and
            // publishing the updated definition (new ConnectorStorageTableId) to the shared metastore.
            assertUpdate("INSERT INTO orders VALUES (" + syntheticOrderKey + ", 300, DATE '1995-01-20', DECIMAL '50.00', 'N')", 1);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);

            // Secondary: even while its index still pins the old snapshot, the result stays correct — the pinned
            // storage id no longer matches the advanced Iceberg snapshot, so the secondary falls back to the base
            // table rather than scanning stale MV data.
            assertThat(secondaryRunner.execute(secondarySessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue())
                    .as("Secondary must return correct data even while its materialization index is stale")
                    .isEqualTo(baseCount + 1);

            // Once the secondary rebuilds its index it re-pins the new snapshot and substitution resumes against it.
            assertEventually(CONVERGENCE_TIMEOUT, () -> {
                assertSubstituted(secondaryRunner, secondarySessionWithSubstitution(), "SELECT * FROM orders");
                assertThat(secondaryRunner.execute(secondarySessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue())
                        .isEqualTo(baseCount + 1);
            });
        }
        finally {
            getDistributedQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            getDistributedQueryRunner().execute("DELETE FROM orders WHERE orderkey = " + syntheticOrderKey);
        }
    }

    @Test
    public void testMaterializedViewDropAndRecreatePropagatesToOtherCluster()
    {
        CatalogSchemaTableName mvName = mvName("mv_shared_recreate_");
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);
            assertEventually(CONVERGENCE_TIMEOUT, () ->
                    assertSubstituted(secondaryRunner, secondarySessionWithSubstitution(), "SELECT * FROM orders"));

            // Primary: drop the MV and recreate it under the same name with the same query, then refresh. The
            // recreated storage table starts a fresh snapshot history, so its ConnectorStorageTableId differs from
            // the one both clusters previously pinned; the new definition is written through to the shared metastore.
            assertUpdate("DROP MATERIALIZED VIEW " + mvName);
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // Secondary: result stays correct throughout (stale pinned snapshot no longer matches, so it falls back
            // to the base table), and once its index rebuilds it substitutes against the recreated MV's snapshot.
            assertThat(secondaryRunner.execute(secondarySessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue())
                    .as("Secondary must return correct data across a drop+recreate on the other cluster")
                    .isEqualTo(baseCount);
            assertEventually(CONVERGENCE_TIMEOUT, () -> {
                assertSubstituted(secondaryRunner, secondarySessionWithSubstitution(), "SELECT * FROM orders");
                assertThat(secondaryRunner.execute(secondarySessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue())
                        .isEqualTo(baseCount);
            });
        }
        finally {
            getDistributedQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    private Session sessionWithSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "true")
                .build();
    }

    private Session secondarySessionWithSubstitution()
    {
        return Session.builder(secondaryRunner.getDefaultSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "true")
                .build();
    }

    private CatalogSchemaTableName mvName(String prefix)
    {
        return new CatalogSchemaTableName(
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                prefix + randomNameSuffix());
    }

    private static void assertSubstituted(DistributedQueryRunner runner, Session session, String query)
    {
        List<String> scannedTableNames = scannedTableNames(runner, runner.executeWithPlan(session, query));
        assertThat(scannedTableNames)
                .as("Expected at least one scan on MV storage table for query: %s", query)
                .anyMatch(name -> name.contains("materialized_view_storage"));
        assertThat(scannedTableNames)
                .as("Expected no scan on base table 'orders' for substituted query: %s", query)
                .doesNotContain("orders");
    }

    private static List<String> scannedTableNames(DistributedQueryRunner runner, MaterializedResultWithPlan result)
    {
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getInputs().stream()
                .map(input -> input.table())
                .collect(toImmutableList());
    }
}
