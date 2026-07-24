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
package io.trino.plugin.snowflake.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.snowflake.SnowflakePlugin;
import io.trino.plugin.snowflake.TestingSnowflakeServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Gated by the {@code cloud-tests} Maven profile; requires the {@code snowflake.test.server.*}
 * system properties (URL, user, password, database, warehouse, and optionally role).
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnSnowflakeSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private final String schema = "test_mv_sub_" + randomNameSuffix();

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new SnowflakePlugin());
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", TestingSnowflakeServer.TEST_URL)
                    .put("connection-user", TestingSnowflakeServer.TEST_USER)
                    .put("connection-password", TestingSnowflakeServer.TEST_PASSWORD)
                    .put("snowflake.database", TestingSnowflakeServer.TEST_DATABASE)
                    .put("snowflake.warehouse", TestingSnowflakeServer.TEST_WAREHOUSE);
            TestingSnowflakeServer.TEST_ROLE.ifPresent(role -> properties.put("snowflake.role", role));
            queryRunner.createCatalog(sourceSchema.getCatalogName(), "snowflake", properties.buildOrThrow());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    // A shared Snowflake account can host concurrent runs, so keep a per-run source schema.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("snowflake", schema);
    }
}
