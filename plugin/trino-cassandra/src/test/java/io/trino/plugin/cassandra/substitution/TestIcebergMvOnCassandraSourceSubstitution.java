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
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * MV substitution where the base table lives in Cassandra and the materialized view is
 * stored in Iceberg. Exercises the connector's {@code getSubstitutionMetadata()} path.
 * <p>
 * A few inherited scenarios rely on base-table operations the Cassandra connector does not
 * support ({@code ALTER ADD COLUMN}, time travel, complex-type DDL); those are {@code @Disabled}
 * with a reason. The complex-type sub-field case is covered by overriding
 * {@link #createNestedTypeTable(CatalogSchemaTableName)} to create a Cassandra {@code map} column via raw CQL
 * (read back as a JSON string) since Cassandra cannot create such a column through Trino DDL.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnCassandraSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private static final String KEYSPACE = "tpch";

    private CassandraServer server;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        server = closeAfterClass(new CassandraServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog(sourceSchema.getCatalogName(), "cassandra", Map.of(
                    "cassandra.contact-points", server.getHost(),
                    "cassandra.native-protocol-port", Integer.toString(server.getPort()),
                    "cassandra.load-policy.use-dc-aware", "true",
                    "cassandra.load-policy.dc-aware.local-dc", "datacenter1",
                    "cassandra.allow-drop-table", "true"));
            createKeyspace(server.getSession(), sourceSchema.getSchemaName());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("cassandra", KEYSPACE);
    }

    // The Cassandra keyspace is created via raw CQL (createKeyspace), not Trino DDL.
    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, KEYSPACE);
    }

    @Override
    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        String tableName = table.getSchemaTableName().getTableName();
        server.getSession().execute("CREATE TABLE %s.%s (id bigint PRIMARY KEY, info map<text, text>)".formatted(KEYSPACE, tableName));
        server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (1, {'name': 'Alice', 'age': '30', 'gender' : 'W'})".formatted(KEYSPACE, tableName));
        server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (2, {'name': 'Bob', 'age': '25', 'gender' : 'M'})".formatted(KEYSPACE, tableName));
        server.getSession().execute("INSERT INTO %s.%s (id, info) VALUES (3, {'name': 'Carol', 'age': '40', 'gender' : 'W'})".formatted(KEYSPACE, tableName));
    }

    @Override
    protected String subFieldExpression(String column, String field)
    {
        return "json_extract_scalar(%s, '$.%s')".formatted(column, field);
    }

    @Test
    @Disabled("Cassandra does not support ALTER TABLE ADD COLUMN")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}
}
