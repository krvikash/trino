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
package io.trino.plugin.oracle.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.oracle.OraclePlugin;
import io.trino.plugin.oracle.TestingOracleServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnOracleSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingOracleServer oracleServer = closeAfterClass(new TestingOracleServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog(sourceSchema.getCatalogName(), "oracle", ImmutableMap.of(
                    "connection-url", oracleServer.getJdbcUrl(),
                    "connection-user", TEST_USER,
                    "connection-password", TEST_PASS));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    // Oracle equates schema with user; use the pre-provisioned TEST_SCHEMA rather than creating one.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("oracle", TEST_SCHEMA);
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }
}
