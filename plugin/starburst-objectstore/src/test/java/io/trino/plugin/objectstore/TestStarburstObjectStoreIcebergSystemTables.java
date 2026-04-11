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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.BaseIcebergSystemTables;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.getConnectorService;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestStarburstObjectStoreIcebergSystemTables
        extends BaseIcebergSystemTables
{
    public TestStarburstObjectStoreIcebergSystemTables()
    {
        super(PARQUET);
    }

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .build();
        try {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg");
            verify(dataDir.toFile().mkdirs());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.ICEBERG.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "local://" + dataDir)
                    .put("fs.local.enabled", "true")
                    .put("iceberg.file-format", "PARQUET")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        DelegateConnectors delegateConnectors = getConnectorService(queryRunner, DelegateConnectors.class);
        IcebergConnector icebergConnector = (IcebergConnector) delegateConnectors.icebergConnector();
        fileSystemFactory = icebergConnector.getInjector().getInstance(TrinoFileSystemFactory.class);
        metastore = icebergConnector.getInjector().getInstance(HiveMetastoreFactory.class).createMetastore(Optional.empty());

        return queryRunner;
    }
}
