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
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestHiveMetadataListing;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class BaseTestObjectStoreMetadataListing
        extends TestHiveMetadataListing
{
    private final String connectorName;
    private final Map<String, String> objectStoreProperties;

    public BaseTestObjectStoreMetadataListing(String connectorName, Map<String, String> objectStoreProperties)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.objectStoreProperties = ImmutableMap.copyOf(objectStoreProperties);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalog = HiveQueryRunner.HIVE_CATALOG;
        String schema = "tpch";
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(catalog)
                                .setSchema(schema)
                                .build())
                // This is needed for e2e scale writers test otherwise 50% threshold of
                // bufferSize won't get exceeded for scaling to happen (synced from BaseHiveConnectorTest)
                .addExtraProperty("task.max-local-exchange-buffer-size", "32MB")
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            queryRunner.installPlugin(new IcebergPlugin());
            Path localFileSystemRootPath = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore_data");
            verify(localFileSystemRootPath.toFile().mkdirs());
            TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(localFileSystemRootPath);
            HiveMetastore metastore = new TestingHiveMetastore();
            queryRunner.installPlugin(new TestingObjectStorePlugin(
                    connectorName,
                    Optional.of(metastore),
                    Optional.of(binder -> newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                            .addBinding("local")
                            .toInstance(fileSystemFactory)),
                    Optional.empty()));

            queryRunner.createCatalog(catalog, connectorName, objectStoreProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    // override because in objectstore we actually get results ignoring the faulty objects
    @Test
    @Override
    public void testTableColumnsWithOnlySchemaFilter()
    {
        String withSchemaFilter = format("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '%s'", DATABASE_NAME);
        assertQuery(withSchemaFilter,
                "VALUES " +
                        "('correct_view', 'expr$0'), " + // since translateHiveViews is enabled. would show column names otherwise
                        "('correct_table', 'column'), " +
                        "('correct_table', 'partition_column')");
    }

    // overriden because objectstore ignores faulty tables and views
    @Test
    @Override
    public void testTableCommentsListing()
    {
        String withSchemaFilterForComments = format(
                "SELECT comment FROM system.metadata.table_comments WHERE schema_name = '%s'",
                DATABASE_NAME);
        assertQuery(withSchemaFilterForComments,
                "VALUES " +
                        "('this is a test table comment')," +
                        "(null)");
    }
}
