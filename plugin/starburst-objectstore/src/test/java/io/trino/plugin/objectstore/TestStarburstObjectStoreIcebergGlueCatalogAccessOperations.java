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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeConnector;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.getConnectorService;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestStarburstObjectStoreIcebergGlueCatalogAccessOperations
        extends BaseTestObjectStoreIcebergGlueCatalogAccessOperations
{
    public TestStarburstObjectStoreIcebergGlueCatalogAccessOperations()
    {
        super(false);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(CATALOG_NAME)
                                .setSchema(testSchema)
                                .build())
                .build();
        try {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("test_iceberg");
            verify(dataDir.toFile().mkdirs());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog(CATALOG_NAME, STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.ICEBERG.name())
                    .put("hive.metastore", "glue")
                    .put("hive.metastore.glue.default-warehouse-dir", "local://" + dataDir)
                    .put("fs.local.enabled", "true")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA " + testSchema);

            DelegateConnectors connectors = getConnectorService(queryRunner, DelegateConnectors.class);
            glueStats = List.of(
                    ((HiveConnector) connectors.hiveConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                    ((IcebergConnector) connectors.icebergConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                    ((DeltaLakeConnector) connectors.deltaConnector()).getInjector().getInstance(GlueMetastoreStats.class),
                    ((HudiConnector) connectors.hudiConnector()).getInjector().getInstance(GlueMetastoreStats.class));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }
}
