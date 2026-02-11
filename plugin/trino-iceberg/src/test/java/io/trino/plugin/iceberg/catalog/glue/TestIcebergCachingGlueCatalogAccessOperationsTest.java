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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;

final class TestIcebergCachingGlueCatalogAccessOperationsTest
        extends BaseIcebergCachingGlueCatalogAccessOperationsTest
{
    private final String testSchema = "test_schema_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder(testSchema)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addIcebergProperty("iceberg.catalog.type", "glue")
                .addIcebergProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .addIcebergProperty("iceberg.glue.metastore-cache.ttl", "30m")
                .setSchemaInitializer(SchemaInitializer.builder().withSchemaName(testSchema).build())
                .build();
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema);
    }

    @Override
    protected List<GlueMetastoreStats> getGlueStats(QueryRunner queryRunner)
    {
        return ImmutableList.of(
                ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector().getInstance(GlueMetastoreStats.class));
    }
}
