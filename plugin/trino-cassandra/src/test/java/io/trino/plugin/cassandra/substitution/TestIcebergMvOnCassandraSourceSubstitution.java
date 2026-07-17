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
package io.trino.plugin.cassandra.substitution;

import io.trino.Session;
import io.trino.plugin.cassandra.CassandraPlugin;
import io.trino.plugin.cassandra.CassandraServer;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * MV substitution where the base table lives in Cassandra and the materialized view is
 * stored in Iceberg. Exercises the connector's {@code getSubstitutionMetadata()} path.
 * <p>
 * A few inherited scenarios rely on base-table operations the Cassandra connector does not
 * support ({@code ALTER ADD COLUMN}, time travel, complex-type DDL); those are {@code @Disabled}
 * with a reason. The complex-type sub-field case is covered instead by
 * {@link #testMapSubFieldProjectionOverMv()}, which creates a Cassandra {@code map} column
 * via raw CQL (read back as a JSON string) since Cassandra cannot create such a column
 * through Trino DDL.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnCassandraSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private static final String KEYSPACE = "tpch";

    private CassandraServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new CassandraServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("cassandra")
                                .setSchema(KEYSPACE)
                                .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(ICEBERG_CATALOG, KEYSPACE));

            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog("cassandra", "cassandra", Map.of(
                    "cassandra.contact-points", server.getHost(),
                    "cassandra.native-protocol-port", Integer.toString(server.getPort()),
                    "cassandra.load-policy.use-dc-aware", "true",
                    "cassandra.load-policy.dc-aware.local-dc", "datacenter1",
                    "cassandra.allow-drop-table", "true"));
            createKeyspace(server.getSession(), KEYSPACE);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected CatalogSchemaName getMvCatalogSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, KEYSPACE);
    }

    @Test
    public void testMapSubFieldProjectionOverMv()
    {
        // Cassandra cannot create a map/row column through Trino DDL, so the table is created
        // via raw CQL. Trino reads a Cassandra map<text,text> as a JSON string (VARCHAR); the MV
        // captures that whole column and json_extract_scalar runs above the substituted scan.
        String tableName = "customers_with_map_" + randomNameSuffix();
        CatalogSchemaTableName mvName = new CatalogSchemaTableName(ICEBERG_CATALOG, KEYSPACE, "mv_map_sub_field_" + randomNameSuffix());
        try {
            server.getSession().execute("CREATE TABLE %s.%s (id bigint PRIMARY KEY, info map<text, text>)".formatted(KEYSPACE, tableName));
            server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (1, {'name': 'Alice', 'age': '30'})".formatted(KEYSPACE, tableName));
            server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (2, {'name': 'Bob', 'age': '25'})".formatted(KEYSPACE, tableName));
            server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (3, {'name': 'Carol', 'age': '40'})".formatted(KEYSPACE, tableName));

            assertUpdate("CREATE MATERIALIZED VIEW %s WITH (substitution_enabled = true) AS SELECT id, info FROM %s".formatted(mvName, tableName));
            assertUpdate("REFRESH MATERIALIZED VIEW %s".formatted(mvName), 3);

            Session session = sessionWithSubstitution();
            String nameQuery = "SELECT json_extract_scalar(info, '$.name') FROM " + tableName;
            assertSubstituted(session, nameQuery, tableName, mvName);
            assertSameResults(session, nameQuery);

            String ageQuery = "SELECT json_extract_scalar(info, '$.age') FROM " + tableName;
            assertSubstituted(session, ageQuery, tableName, mvName);
            assertSameResults(session, ageQuery);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            server.getSession().execute("DROP TABLE IF EXISTS %s.%s".formatted(KEYSPACE, tableName));
        }
    }

    @Test
    @Disabled("Cassandra does not support ALTER TABLE ADD COLUMN")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}

    @Test
    @Disabled("Cassandra does not support time travel")
    @Override
    public void testForVersionAsOfNotSubstituted() {}

    @Test
    @Disabled("Cassandra cannot create a ROW/map column through Trino DDL; covered by testMapSubFieldProjectionOverMv")
    @Override
    public void testScanWithSubFieldProjectionOverMv() {}
}
