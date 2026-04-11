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
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.catalog.hms.BaseIcebergCachingHiveCatalogAccessOperationsTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class TestObjectStoreIcebergCachingHiveCatalogAccessOperationsTest
        extends BaseIcebergCachingHiveCatalogAccessOperationsTest
{
    private static final String CATALOG_NAME = "objectstore";
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(CATALOG_NAME)
                                .setSchema(TEST_SCHEMA)
                                .build())
                .build();
        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore_iceberg_hms_cache");
        verify(dataDir.toFile().mkdirs());

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.installPlugin(new ObjectStorePlugin());
        queryRunner.createCatalog(CATALOG_NAME, STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                .put("great-lakes.table-type", TableType.ICEBERG.name())
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", "file://" + dataDir)
                .put("hive.metastore-cache-ttl", "30m")
                .put("fs.local.enabled", "true")
                .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + TEST_SCHEMA);

        return queryRunner;
    }
}
