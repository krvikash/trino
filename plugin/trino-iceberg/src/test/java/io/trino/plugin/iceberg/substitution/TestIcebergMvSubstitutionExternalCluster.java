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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Verifies that a primary cluster correctly rejects substitution when an external cluster, which
 * does not share the primary's in-memory {@code MaterializationIndex}, mutates the MV's storage
 * table. Substitution identity is the storage table's snapshot id: the primary's index pins the
 * snapshot captured at its own index time, so any divergent change made by the external cluster —
 * whether a drop+recreate or a plain refresh that advances the snapshot — makes the current
 * snapshot differ from the indexed one and stops substitution until the primary re-indexes.
 * <p>
 * Both query runners point their {@code local://} root at the same on-disk path and use the
 * {@code TESTING_FILE_METASTORE} catalog type pointed at the same {@code local:///iceberg-catalog}
 * directory, so their {@code FileHiveMetastore} instances share state via the filesystem while
 * each coordinator maintains its own in-memory {@code MaterializationIndex}.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionExternalCluster
        extends AbstractTestQueryFramework
{
    private static final String MV_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";

    // iceberg.unique-table-location=false is deliberate: the recreated storage table reuses
    // the dropped table's on-disk location, so any name- or location-based identity check
    // would falsely accept it as the same physical table. The test only passes because the
    // ConnectorStorageTableId pins the storage table's snapshot id, which differs across the
    // recreate.
    private static final Map<String, String> CATALOG_PROPERTIES = Map.of(
            "iceberg.catalog.type", "TESTING_FILE_METASTORE",
            "hive.metastore.catalog.dir", "local:///iceberg-catalog",
            "iceberg.hive-catalog-name", "hive",
            "iceberg.unique-table-location", "false",
            // Both clusters' CachingHiveMetastore must read fresh on every call so each cluster
            // sees the on-disk state written by the other. TTL=0 plus disabling negative caching
            // prevents either side from holding a stale or not-found-cached entry for the MV.
            "hive.metastore-cache-ttl", "0s",
            "hive.metastore-cache.cache-missing", "false");

    private QueryRunner externalRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Both coordinators must use the same baseDataDir so their TestingIcebergPlugin instances
        // resolve `local://` to the same on-disk root, giving each cluster a FileHiveMetastore
        // pointed at the same metastore directory.
        Path sharedDataDir = Files.createTempDirectory("iceberg-mv-substitution-external");
        sharedDataDir.toFile().deleteOnExit();

        QueryRunner primary = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_CATALOG)
                        .setSchema("tpch")
                        .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .setBaseDataDir(Optional.of(sharedDataDir))
                .build();
        primary.installPlugin(new TestingIcebergPlugin(sharedDataDir));
        primary.createCatalog(ICEBERG_CATALOG, "iceberg", CATALOG_PROPERTIES);
        primary.installPlugin(new TpchPlugin());
        primary.createCatalog("tpch", "tpch");
        // Schema location must live outside the metastore catalog dir. With
        // iceberg.unique-table-location=false, storage data goes to <schema-location>/<mvName>/.
        // If schema-location were under the metastore catalog dir, that path would also be the
        // metastore entry directory for the MV (containing .trinoSchema), and
        // dropMaterializedViewStorage's recursive directory delete would wipe the metastore entry
        // along with the storage data.
        primary.execute("CREATE SCHEMA tpch WITH (location = 'local:///iceberg-data/tpch')");

        // External cluster: substitution is explicitly off in its default session, modelling a
        // standalone cluster that is unaware of the substitution feature. Its DDL touches the
        // same on-disk metastore directory but its in-memory MaterializationIndex is separate
        // from the primary's, so primary's index does not see the drop+recreate.
        externalRunner = closeAfterClass(DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_CATALOG)
                        .setSchema("tpch")
                        .setSystemProperty(MV_SUBSTITUTION_ENABLED, "false")
                        .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "false")
                .setBaseDataDir(Optional.of(sharedDataDir))
                .build());
        externalRunner.installPlugin(new TestingIcebergPlugin(sharedDataDir));
        externalRunner.createCatalog(ICEBERG_CATALOG, "iceberg", CATALOG_PROPERTIES);
        // Schema already exists on disk — do not recreate it from the external cluster.

        return primary;
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
    public void testStaleIndexAfterExternalDropAndRecreate()
    {
        CatalogSchemaTableName mvName = mvName("mv_external_recreate_");
        Session substitutionSession = sessionWithSubstitution();
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            // Primary: create MV, refresh, query — populates primary's MaterializationIndex
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);
            assertSubstituted(substitutionSession, "SELECT * FROM orders");

            // External: drop the MV. This updates the shared on-disk metastore (and the external
            // cluster's own index) but does NOT touch the primary cluster's in-memory
            // MaterializationIndex.
            externalRunner.execute("DROP MATERIALIZED VIEW " + mvName);

            // External: create a new MV under the same name, with the SAME projected columns as
            // the original but a filter that excludes all rows. This is the critical part of the
            // test: with the same columns, the candidate's column mapping would succeed against
            // the new storage, so the only thing that can stop a wrong substitution is
            // the ConnectorStorageTableId comparison rejecting the new storage's snapshot.
            // Because of the hidden-storage naming scheme + iceberg.unique-table-location=false,
            // the new storage reuses the dropped table's on-disk location — a tableLocation-only
            // identity check would falsely match. The recreated storage table starts a fresh
            // snapshot history, so its snapshot id differs from the one the primary indexed.
            externalRunner.execute("CREATE MATERIALIZED VIEW " + mvName +
                    " AS SELECT * FROM orders WHERE custkey < 0");
            externalRunner.execute("REFRESH MATERIALIZED VIEW " + mvName);

            // Primary: its index still pins the OLD storage table's snapshot id. The name now
            // resolves to the recreated storage table whose snapshot id differs, so the stored and
            // current ConnectorStorageTableId are not equal, and the stale candidate must be
            // evicted, so the original "SELECT * FROM orders" pattern must NOT be substituted.
            assertNotSubstituted(substitutionSession, "SELECT * FROM orders");

            // And the query must still return the correct base-table result, not stale MV data.
            // If the ConnectorStorageTableId comparison falsely matched and substitution proceeded against external's
            // new storage table (which is empty by construction), this count would be 0 instead
            // of baseCount.
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM orders").getOnlyValue())
                    .isEqualTo(baseCount);
        }
        finally {
            try {
                externalRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
        }
    }

    @Test
    public void testStaleIndexAfterExternalSourceDropAndRecreate()
    {
        // External-cluster drop+recreate of the SOURCE table (not the MV's storage). The MV
        // and its storage are untouched, so the storage snapshot id still matches —
        // the only signal that the source has been replaced is the source table's own
        // identity. IcebergTableId.hash hashes schemaName+tableName+tableLocation;
        // with iceberg.unique-table-location=false the recreated table reuses the same on-disk
        // location, so hash collides and the index lookup returns the stale candidate.
        // canSubstitute must additionally compare the source table's Iceberg table-uuid so
        // drop+recreate is rejected even when location is reused.
        String sourceName = "src_drop_recreate_" + randomNameSuffix();
        CatalogSchemaTableName mvName = mvName("mv_source_recreate_");
        Session substitutionSession = sessionWithSubstitution();
        try {
            assertUpdate("CREATE TABLE " + sourceName + " AS SELECT 1 AS id", 1);
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM " + sourceName);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);

            // Sanity: substitution fires while the source is unchanged.
            assertSubstituted(substitutionSession, "SELECT * FROM " + sourceName);

            // External cluster drops and recreates the source table with same name + reused
            // location but different content.
            externalRunner.execute("DROP TABLE " + sourceName);
            externalRunner.execute("CREATE TABLE " + sourceName + " AS SELECT 2 AS id UNION ALL SELECT 3 AS id");

            // After recreate the source has 2 rows. Substitution must NOT fire: the captured
            // identity references the OLD source. If substitution proceeds it would scan the
            // stale MV storage and return the OLD single row (id=1).
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                    substitutionSession,
                    "SELECT * FROM " + sourceName);
            assertThat(scannedTableNames(result))
                    .as("Substitution must not fire after source drop+recreate")
                    .contains(sourceName)
                    .noneMatch(name -> name.contains("materialized_view_storage"));
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM " + sourceName).getOnlyValue())
                    .as("Query must return data from the recreated source, not stale MV data")
                    .isEqualTo(2L);
        }
        finally {
            try {
                externalRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
            try {
                externalRunner.execute("DROP TABLE IF EXISTS " + sourceName);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
        }
    }

    @Test
    public void testExternalRefreshStopsSubstitutionUntilLocalReindex()
    {
        // Same MV across two clusters — only the storage table's snapshot advances (NOT a
        // drop/recreate). Because substitution identity is the storage snapshot id, an external
        // refresh advances the snapshot beyond the one the primary indexed, so the primary's pinned
        // ConnectorStorageTableId no longer matches and substitution stops. The query must then fall
        // back to the base table (still correct), and a local refresh must re-index the new snapshot
        // and let substitution resume.
        CatalogSchemaTableName mvName = mvName("mv_external_refresh_");
        Session substitutionSession = sessionWithSubstitution();
        // Synthetic orderkeys outside TPCH's value range so cleanup only removes this test's rows.
        long firstSyntheticOrderKey = 99_999_999L;
        long secondSyntheticOrderKey = 99_999_998L;
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            // Primary: create MV, refresh, query — pins the storage table's initial snapshot.
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);
            assertSubstituted(substitutionSession, "SELECT * FROM orders");
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM orders").getOnlyValue())
                    .as("Substituted result must match base-table count before any external refresh")
                    .isEqualTo(baseCount);

            // Insert a new row into the base table. Primary's MaterializationIndex still pins the
            // pre-insert snapshot of the storage table.
            assertUpdate("INSERT INTO orders VALUES (" + firstSyntheticOrderKey + ", 300, DATE '1995-01-20', DECIMAL '50.00', 'N')", 1);

            // External: refresh the MV. The storage table advances to a new snapshot reflecting
            // the post-insert base-table state.
            externalRunner.execute("REFRESH MATERIALIZED VIEW " + mvName);

            // Primary: substitution must STOP — the current storage snapshot no longer equals the
            // snapshot pinned in the primary's index.
            assertNotSubstituted(substitutionSession, "SELECT * FROM orders");

            // The query still returns correct data because it falls back to the base table.
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM orders").getOnlyValue())
                    .as("Query must return correct base-table data when substitution is skipped")
                    .isEqualTo(baseCount + 1);

            // Make the MV stale again so the primary's own refresh does real work and commits a new
            // storage snapshot (a no-op refresh of an already-fresh MV would neither advance the
            // snapshot nor re-index).
            assertUpdate("INSERT INTO orders VALUES (" + secondSyntheticOrderKey + ", 300, DATE '1995-01-20', DECIMAL '50.00', 'N')", 1);

            // Primary: a local refresh re-indexes the snapshot it just committed, so substitution
            // resumes against that snapshot. The refresh is incremental, writing only the new row.
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);
            assertSubstituted(substitutionSession, "SELECT * FROM orders");
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM orders").getOnlyValue())
                    .as("Substitution must resume against the re-indexed snapshot")
                    .isEqualTo(baseCount + 2);
        }
        finally {
            try {
                externalRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
            try {
                assertUpdate("DELETE FROM orders WHERE orderkey IN (" + firstSyntheticOrderKey + ", " + secondSyntheticOrderKey + ")", 2);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
        }
    }

    @Test
    public void testMvWithSubstitutionEnabledRemainsUsableOnClusterWithoutSubstitutionSupport()
    {
        // The MV is created on the primary cluster with substitution_enabled=true. The external
        // cluster has materialized-view-substitution.support.enabled=false, which strips the
        // substitution_enabled property metadata engine-side on that cluster. The MV must
        // remain fully usable from the external cluster: SELECT, SHOW CREATE, REFRESH, and
        // ALTER paths that do not touch substitution_enabled all work.
        CatalogSchemaTableName mvName = mvName("mv_use_from_disabled_cluster_");
        Session substitutionSession = sessionWithSubstitution();
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // SELECT from MV directly returns its rows.
            assertThat(externalRunner.execute("SELECT count(*) FROM " + mvName).getOnlyValue())
                    .as("MV must be readable from a cluster with substitution support disabled")
                    .isEqualTo(baseCount);

            // SHOW CREATE renders without referencing the stripped property.
            String externalShowCreate = (String) externalRunner.execute("SHOW CREATE MATERIALIZED VIEW " + mvName).getOnlyValue();
            assertThat(externalShowCreate)
                    .as("SHOW CREATE on the disabled cluster must omit substitution_enabled (property is not registered there)")
                    .doesNotContain("substitution_enabled");

            // REFRESH works.
            externalRunner.execute("REFRESH MATERIALIZED VIEW " + mvName);

            // Attempting to ALTER substitution_enabled from the disabled cluster fails cleanly:
            // engine-side property metadata for this catalog doesn't include the property.
            assertThat(externalRunner.execute("SHOW CREATE MATERIALIZED VIEW " + mvName)).isNotNull();
            assertThatThrownBy(() -> externalRunner.execute(
                    "ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES substitution_enabled = false"))
                    .hasMessageContaining("Catalog 'iceberg' materialized view property 'substitution_enabled' does not exist");

            // The MV is still fully usable from the primary cluster, and substitution still fires.
            assertSubstituted(substitutionSession, "SELECT * FROM orders");
            assertThat(computeActual(substitutionSession, "SELECT count(*) FROM orders").getOnlyValue())
                    .isEqualTo(baseCount);

            // SHOW CREATE on the primary cluster does include the property (the engine flag is on here).
            String primaryShowCreate = (String) computeActual("SHOW CREATE MATERIALIZED VIEW " + mvName).getOnlyValue();
            assertThat(primaryShowCreate).contains("substitution_enabled = true");
        }
        finally {
            try {
                externalRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            }
            catch (RuntimeException ignored) {
                // best-effort cleanup
            }
        }
    }

    private Session sessionWithSubstitution()
    {
        return Session.builder(getSession())
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

    private void assertSubstituted(Session session, String query)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<String> scannedTableNames = scannedTableNames(result);
        assertThat(scannedTableNames)
                .as("Expected at least one scan on MV storage table for query: %s", query)
                .anyMatch(name -> name.contains("materialized_view_storage"));
        assertThat(scannedTableNames)
                .as("Expected no scan on base table 'orders' for substituted query: %s", query)
                .doesNotContain("orders");
    }

    private void assertNotSubstituted(Session session, String query)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<String> scannedTableNames = scannedTableNames(result);
        assertThat(scannedTableNames)
                .as("Expected scan on base table 'orders' (no substitution) for query: %s", query)
                .contains("orders");
        assertThat(scannedTableNames)
                .as("Expected no scan on MV storage table for query: %s", query)
                .noneMatch(name -> name.contains("materialized_view_storage"));
    }

    private List<String> scannedTableNames(MaterializedResultWithPlan result)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getInputs().stream()
                .map(input -> input.table())
                .collect(toImmutableList());
    }
}
