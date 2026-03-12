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
package io.trino.plugin.warp.it.proxiedconnector.iceberg;

import io.trino.Session;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestIcebergProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "iceberg");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(new WarpStubsStorageEngineModule(),
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Collections.emptyMap());
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());
        return queryRunner;
    }

    @Test
    public void testDynamicCatalog()
    {
        String catalogDir = "\"hive.metastore.catalog.dir\"='file://" + hiveDir.toAbsolutePath() + "'";
        String createCatalogSql = """
                CREATE CATALOG IF NOT EXISTS iceberg_read_warp USING warp_speed
                WITH (
                "warp-speed.proxied-connector"='iceberg',
                "warp-speed.cluster-uuid"='1234567e-89ee-123e-456e-567891234567',
                "hive.metastore.disable-location-checks"='true',
                "warp-speed.enable.import-export"='true',
                "fs.hadoop.enabled"='true',
                "iceberg.security"='system',
                "iceberg.register-table-procedure.enabled"='true',
                "iceberg.hive-catalog-name"='hive_read_warp',
                "hive.metastore"='file',
                "iceberg.catalog.type"='TESTING_FILE_METASTORE',
                """;
        createCatalogSql += catalogDir + ")";
        computeActual(createCatalogSql);
        Session catalogSession = Session.builder(getSession())
                .setCatalog("iceberg_read_warp")
                .build();
        computeActual(catalogSession, "CREATE SCHEMA dynamic_schema_test");
        assertSchema(catalogSession, "dynamic_schema_test");
        computeActual(catalogSession, format("CREATE TABLE %s.%s %s", "dynamic_schema_test", "table_name", format("(%s integer, %s varchar(20))", C1, C2)));
        assertUpdate(catalogSession, format("DROP TABLE %s.%s", "dynamic_schema_test", "table_name"));
        assertUpdate(catalogSession, format("DROP SCHEMA %s", "dynamic_schema_test"));
        computeActual("DROP CATALOG iceberg_read_warp");
    }

    @Test
    public void testSimple_withoutWarm_ReturnIceberg()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(String.format("SELECT * FROM t WHERE %s = 1", C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testDuplicateSourceIdPartition()
    {
        String table = "duplicate_source_id_partition";
        String aCol = "a";
        String dateIntCol = "date_int";
        String dateDateCol = "date_date";
        createTable(DEFAULT_SCHEMA,
                table,
                "(%s varchar, %s integer, %s date) WITH (format='PARQUET', partitioning = ARRAY['bucket(%s, 1)', 'truncate(%s, 1)'])"
                        .formatted(aCol, dateIntCol, dateDateCol, aCol, aCol));
        int partitionValue = 20190315;
        @Language("SQL") String sql = format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', %d, CAST('2020-04-%d%d' AS date))",
                table, aCol, dateIntCol, dateDateCol, 1, partitionValue, 1, 2);
        assertUpdate(sql, 1);
        computeActual(format("SELECT * FROM %s", table));
    }

    @Test
    public void testTimestamp6WithTimezone()
    {
        createTable(DEFAULT_SCHEMA, "timestamp_6", "(timestamp_col TIMESTAMP(6) with time zone, another_column TIMESTAMP(6) with time zone) WITH (format='PARQUET')");
        computeActual("INSERT INTO timestamp_6 VALUES (CAST('1969-12-31 15:03:00.123456 +01:00' as TIMESTAMP), CAST('1969-12-31 15:03:00.123456 +02:00' as TIMESTAMP))");
        computeActual("SELECT * FROM timestamp_6 WHERE timestamp_col = TIMESTAMP '1969-12-31 15:03:00.123456 +01:00' OR another_column = TIMESTAMP '1969-12-31 15:03:00.123456 +01:00'");
    }
}
