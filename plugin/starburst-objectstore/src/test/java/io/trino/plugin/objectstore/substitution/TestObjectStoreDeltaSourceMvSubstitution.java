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
package io.trino.plugin.objectstore.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.objectstore.TableType;
import io.trino.plugin.objectstore.TestingObjectStorePlugin;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;

/**
 * Runs the MV substitution suite against an ObjectStore catalog whose source tables are Delta Lake
 * format and whose materialization storage is Iceberg, exercising
 * {@link ObjectStoreSubstitutionMetadata} routing of Delta Lake handles to the Delta Lake delegate.
 * The catalog runs on the {@code local} file system: {@link TestingObjectStorePlugin} binds it for the
 * Hive delegate and, together with the Delta Lake transaction log synchronizer, for the Delta Lake
 * delegate, which a Delta Lake write requires.
 */
final class TestObjectStoreDeltaSourceMvSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore");
            verify(dataDir.toFile().mkdirs());
            TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(dataDir);
            HiveMetastore metastore = new FileHiveMetastore(
                    new NodeVersion("testversion"),
                    fileSystemFactory,
                    false,
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory("local:///")
                            .setMetastoreUser("test")
                            .setDisableLocationChecks(true));

            queryRunner.installPlugin(new TestingObjectStorePlugin(
                    STARBURST_OBJECTSTORE,
                    Optional.of(metastore),
                    Optional.of(binder -> newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                            .addBinding("local")
                            .toInstance(fileSystemFactory)),
                    Optional.of(dataDir)));
            queryRunner.createCatalog(sourceSchema.getCatalogName(), STARBURST_OBJECTSTORE, ImmutableMap.of(
                    "great-lakes.table-type", TableType.DELTA.name()));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }
}
