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
package io.trino.tests.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnJdbcSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("postgres")
                                .setSchema("tpch2")
                                .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive",
                    // MV storage tables must be v3 to hold a JSON column, mapped through Iceberg
                    // variant; exercised by testScanWithSubFieldProjectionOverMv when the source
                    // is PostgreSQL.
                    "iceberg.format-version", "3",
                    "iceberg.legacy-variant-type-mapping", "JSON"));
            queryRunner.execute("CREATE SCHEMA %s.tpch".formatted(ICEBERG_CATALOG));

            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog("postgres", "postgresql", ImmutableMap.of(
                    "connection-url", postgreSqlServer.getJdbcUrl(),
                    "connection-user", postgreSqlServer.getUser(),
                    "connection-password", postgreSqlServer.getPassword(),
                    // Enables reading/writing PostgreSQL array columns as Trino ARRAY, so the coercion
                    // test can exercise array(smallint) -> array(integer) storage normalization.
                    "postgresql.array-mapping", "AS_ARRAY"));
            queryRunner.execute("CREATE SCHEMA postgres.tpch2");

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
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }

    @Override
    protected String subFieldColumnType()
    {
        return "JSON";
    }

    @Override
    protected String subFieldInsertValues()
    {
        return "(1, JSON '{\"name\":\"Alice\",\"age\":30, \"gender\":\"W\"}'), " +
                "(2, JSON '{\"name\":\"Bob\",\"age\":25, \"gender\":\"M\"}'), " +
                "(3, JSON '{\"name\":\"Carol\",\"age\":40, \"gender\":\"W\"}')";
    }

    @Override
    protected String subFieldExpression(String column, String field)
    {
        return "json_extract_scalar(" + column + ", '$." + field + "')";
    }

    @Override
    protected List<CoercionColumn> coercionColumns()
    {
        return List.of(
                new CoercionColumn("c_varchar", "varchar(65535)", "'Alice'", "upper(%s)"),
                new CoercionColumn("c_char", "char(10)", "'Alice'", "upper(%s)"),
                new CoercionColumn("c_smallint", "smallint", "42", "abs(%s)"),
                new CoercionColumn("c_time", "time(3)", "TIME '12:34:56.123'", "hour(%s)"),
                new CoercionColumn("c_timestamp", "timestamp(3)", "TIMESTAMP '2020-01-01 12:34:56.123'", "year(%s)"),
                new CoercionColumn("c_array", "array(smallint)", "ARRAY[smallint '1', smallint '2']", "cardinality(%s)"));
    }
}
