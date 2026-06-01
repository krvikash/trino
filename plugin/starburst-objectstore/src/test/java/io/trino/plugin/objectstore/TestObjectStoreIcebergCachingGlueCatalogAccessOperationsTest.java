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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeConnector;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.catalog.glue.BaseIcebergCachingGlueCatalogAccessOperationsTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class TestObjectStoreIcebergCachingGlueCatalogAccessOperationsTest
        extends BaseIcebergCachingGlueCatalogAccessOperationsTest
{
    private static final String CATALOG_NAME = "objectstore";

    private final String testSchema = "test_schema_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(CATALOG_NAME)
                                .setSchema(testSchema)
                                .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "false")
                .build();
        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore_iceberg_glue_cache");
        verify(dataDir.toFile().mkdirs());

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.installPlugin(new ObjectStorePlugin());
        queryRunner.createCatalog(CATALOG_NAME, STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                .put("great-lakes.table-type", TableType.ICEBERG.name())
                .put("hive.metastore", "glue")
                .put("hive.metastore.glue.default-warehouse-dir", "local://" + dataDir)
                .put("iceberg.glue.metastore-cache.ttl", "30m")
                .put("hive.metastore-cache-ttl", "30m")
                .put("fs.local.enabled", "true")
                .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + testSchema);

        return queryRunner;
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema);
    }

    @Override
    protected List<GlueMetastoreStats> getGlueStats(QueryRunner queryRunner)
    {
        DelegateConnectors connectors = getConnectorService(queryRunner, DelegateConnectors.class);
        return ImmutableList.of(
                ((HiveConnector) connectors.hiveConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                ((IcebergConnector) connectors.icebergConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                ((DeltaLakeConnector) connectors.deltaConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                ((HudiConnector) connectors.hudiConnector()).getInjector().getInstance(GlueMetastoreStats.class));
    }
}
