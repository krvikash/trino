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
package io.trino.plugin.iceberg.catalog.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergIncrementalMvRefreshConfig;
import io.trino.plugin.iceberg.IcebergScheduledMvRefreshConfig;
import io.trino.plugin.iceberg.NoopWorkScheduler;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.INCREMENTAL_COLUMN;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoHiveCatalogWithFileMetastore
        extends BaseTrinoCatalogTest
{
    private static final Logger log = Logger.get(TestTrinoHiveCatalogWithFileMetastore.class);

    private Path tempDir;
    private TrinoFileSystemFactory fileSystemFactory;
    private HiveMetastore metastore;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        tempDir = Files.createTempDirectory("test_trino_hive_catalog");
        File metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastoreDir.mkdirs();
        fileSystemFactory = new LocalFileSystemFactory(metastoreDir.toPath());
        metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///"));
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected void createNamespaceWithProperties(TrinoCatalog catalog, String namespace, Map<String, String> properties)
    {
        metastore.createDatabase(Database.builder()
                .setDatabaseName(namespace)
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .setParameters(properties)
                .build());
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        return new TrinoHiveCatalog(
                new CatalogName("catalog"),
                new NoopWorkScheduler(),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY),
                useUniqueTableLocations,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                new IcebergScheduledMvRefreshConfig().isScheduledMaterializedViewRefreshEnabled(),
                new IcebergIncrementalMvRefreshConfig().isMaterializedViewIncrementalColumnRefreshEnabled(),
                directExecutor());
    }

    @Test
    @Disabled
    public void testDropMaterializedView()
    {
        testDropMaterializedView(false);
    }

    @Test
    public void testDropMaterializedViewWithUniqueTableLocation()
    {
        testDropMaterializedView(true);
    }

    private void testDropMaterializedView(boolean useUniqueTableLocations)
    {
        TrinoCatalog catalog = createTrinoCatalog(useUniqueTableLocations);
        String namespace = "test_create_mv_" + randomNameSuffix();
        String materializedViewName = "materialized_view_name";
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createMaterializedView(
                    SESSION,
                    new SchemaTableName(namespace, materializedViewName),
                    new ConnectorMaterializedViewDefinition(
                            "SELECT * FROM tpch.tiny.nation",
                            Optional.empty(),
                            Optional.of("catalog_name"),
                            Optional.of("schema_name"),
                            ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("col1", INTEGER.getTypeId(), Optional.empty())),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableList.of()),
                    ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1),
                    false,
                    false);

            catalog.dropMaterializedView(SESSION, new SchemaTableName(namespace, materializedViewName));
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                log.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    public void testIncrementalColumnPropertyRoundTrip()
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
                new NoopWorkScheduler(),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY),
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                new IcebergScheduledMvRefreshConfig().isScheduledMaterializedViewRefreshEnabled(),
                true, // incrementalColumnMvRefreshEnabled
                directExecutor());

        String namespace = "test_incremental_col_roundtrip_" + randomNameSuffix();
        SchemaTableName mvWithProperty = new SchemaTableName(namespace, "mv_with_incremental");
        SchemaTableName mvWithoutProperty = new SchemaTableName(namespace, "mv_without_incremental");
        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                "SELECT id, ts FROM source",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorMaterializedViewDefinition.Column("id", INTEGER.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("ts", INTEGER.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("owner"),
                ImmutableList.of());

        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));

            // MV with incremental_column: parameter must round-trip through the metastore.
            catalog.createMaterializedView(SESSION, mvWithProperty, definition, ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1, INCREMENTAL_COLUMN, "ts"), false, false);
            Optional<ConnectorMaterializedViewDefinition> readBack = catalog.getMaterializedView(SESSION, mvWithProperty);
            assertThat(readBack).isPresent();
            Map<String, Object> propertiesWithColumn = catalog.getMaterializedViewProperties(SESSION, mvWithProperty, readBack.get());
            assertThat(propertiesWithColumn).containsEntry(INCREMENTAL_COLUMN, "ts");

            // MV without incremental_column: property must be absent.
            catalog.createMaterializedView(SESSION, mvWithoutProperty, definition, ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1), false, false);
            Optional<ConnectorMaterializedViewDefinition> readBack2 = catalog.getMaterializedView(SESSION, mvWithoutProperty);
            assertThat(readBack2).isPresent();
            Map<String, Object> propertiesWithoutColumn = catalog.getMaterializedViewProperties(SESSION, mvWithoutProperty, readBack2.get());
            assertThat(propertiesWithoutColumn).doesNotContainKey(INCREMENTAL_COLUMN);
        }
        finally {
            try {
                catalog.dropMaterializedView(SESSION, mvWithProperty);
            }
            catch (Exception e) {
                log.warn("Failed to clean up materialized view: %s", mvWithProperty);
            }
            try {
                catalog.dropMaterializedView(SESSION, mvWithoutProperty);
            }
            catch (Exception e) {
                log.warn("Failed to clean up materialized view: %s", mvWithoutProperty);
            }
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                log.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    @Override
    public void testListTables()
    {
        // the test actually works but when cleanup up the materialized view the error is thrown
        assertThatThrownBy(super::testListTables).hasMessageMatching("Table 'ns2.*.mv' not found");
    }
}
