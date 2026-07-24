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
package io.trino.plugin.clickhouse.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.clickhouse.ClickHousePlugin;
import io.trino.plugin.clickhouse.TestingClickHouseServer;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnClickHouseSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingClickHouseServer clickHouseServer = closeAfterClass(new TestingClickHouseServer());
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new ClickHousePlugin());
            queryRunner.createCatalog(sourceSchema.getCatalogName(), "clickhouse", ImmutableMap.of(
                    "connection-url", clickHouseServer.getJdbcUrl(),
                    "connection-user", clickHouseServer.getUsername(),
                    "connection-password", clickHouseServer.getPassword(),
                    // ClickHouse maps its String type to Trino VARBINARY unless told otherwise; the
                    // abstract test's base tables use VARCHAR columns, so read them back as VARCHAR.
                    "clickhouse.map-string-as-varchar", "true"));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Test
    @Disabled("ClickHouse Log storage engine does not support ALTER ADD COLUMN")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}
}
