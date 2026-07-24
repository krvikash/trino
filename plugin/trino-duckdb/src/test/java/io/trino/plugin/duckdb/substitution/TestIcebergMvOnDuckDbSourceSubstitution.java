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
package io.trino.plugin.duckdb.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.duckdb.DuckDbPlugin;
import io.trino.plugin.duckdb.TestingDuckDb;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnDuckDbSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingDuckDb duckDb = closeAfterClass(new TestingDuckDb());
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();

        queryRunner.installPlugin(new GeoPlugin());
        queryRunner.installPlugin(new DuckDbPlugin());
        queryRunner.createCatalog(sourceSchema.getCatalogName(), "duckdb", ImmutableMap.of(
                "connection-url", duckDb.getJdbcUrl()));
        return queryRunner;
    }
}
