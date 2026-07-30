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
import io.trino.Session;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnJdbcSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog(sourceSchema.getCatalogName(), "postgresql", ImmutableMap.of(
                    "connection-url", postgreSqlServer.getJdbcUrl(),
                    "connection-user", postgreSqlServer.getUser(),
                    "connection-password", postgreSqlServer.getPassword(),
                    // Enables reading/writing PostgreSQL array columns as Trino ARRAY, so the coercion
                    // test can exercise array(smallint) -> array(integer) storage normalization.
                    "postgresql.array-mapping", "AS_ARRAY"));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
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
