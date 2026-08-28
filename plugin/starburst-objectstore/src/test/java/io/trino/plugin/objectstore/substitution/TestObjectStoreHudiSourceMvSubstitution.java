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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.plugin.objectstore.TableType;
import io.trino.plugin.objectstore.TestingObjectStorePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Runs the read-only MV substitution suite against an ObjectStore catalog whose source tables are Hudi
 * and whose materialization storage is Iceberg, exercising {@link ObjectStoreSubstitutionMetadata}
 * routing of Hudi handles to the Hudi delegate. Hudi tables cannot be created through Trino, so they
 * are loaded with the Hudi Java client against this catalog's metastore and file system.
 */
final class TestObjectStoreHudiSourceMvSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    private static final String CATALOG = "objectstore";
    private static final String SCHEMA = "tests";

    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore");
            verify(dataDir.toFile().mkdirs());
            LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(dataDir);
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
                    Optional.empty(),
                    Optional.empty()));
            queryRunner.createCatalog(CATALOG, STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.HUDI.name())
                    .put("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                    .put("fs.local.enabled", "true")
                    .put("local.location", dataDir.toString())
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(CATALOG, SCHEMA));

            new ObjectStoreHudiTablesInitializer(metastore, fileSystemFactory.create(ConnectorIdentity.ofUser("test")))
                    .initializeTables(queryRunner, Location.of("local:///"), SCHEMA);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName(CATALOG, SCHEMA, "orders");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName(CATALOG, SCHEMA, "customer");
    }

    @Override
    protected String materializedColumn()
    {
        return "orderkey";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "orderstatus";
    }

    /**
     * Loads the TPCH-backed Hudi tables against an explicit metastore and file system, since the
     * catalog is an ObjectStore catalog rather than the Hudi connector the loader resolves by default.
     */
    private static class ObjectStoreHudiTablesInitializer
            extends TpchHudiTablesInitializer
    {
        private final HiveMetastore metastore;
        private final TrinoFileSystem fileSystem;

        ObjectStoreHudiTablesInitializer(HiveMetastore metastore, TrinoFileSystem fileSystem)
        {
            super(ImmutableList.of(TpchTable.ORDERS, TpchTable.CUSTOMER));
            this.metastore = requireNonNull(metastore, "metastore is null");
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        }

        @Override
        protected HiveMetastore getMetastore(QueryRunner queryRunner)
        {
            return metastore;
        }

        @Override
        protected TrinoFileSystem getTrinoFileSystem(QueryRunner queryRunner)
        {
            return fileSystem;
        }
    }
}
