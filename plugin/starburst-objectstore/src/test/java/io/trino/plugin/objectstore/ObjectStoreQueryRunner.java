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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.warp2.TpchWarpSpeedObjectStoreHudiTablesInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;

public final class ObjectStoreQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        // Iceberg library logs a lot at INFO level.
        logging.setLevel("org.apache.iceberg", Level.WARN);
    }

    public static final String CATALOG = "objectstore";
    public static final String TPCH_SCHEMA = "tpch";

    private ObjectStoreQueryRunner() {}

    public static void initializeTpchTables(DistributedQueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        copyTpchTables(
                queryRunner,
                TPCH_SCHEMA,
                TINY_SCHEMA_NAME,
                queryRunner.getDefaultSession(),
                tables);
    }

    public static void initializeTpchTablesHudi(DistributedQueryRunner queryRunner, List<TpchTable<?>> tables, Location externalLocation)
            throws Exception
    {
        TpchObjectStoreHudiTablesInitializer loader = new TpchObjectStoreHudiTablesInitializer(tables);
        loader.initializeTables(queryRunner, externalLocation, TPCH_SCHEMA);
    }

    public static void initializeWarpSpeedTpchTablesHudi(DistributedQueryRunner queryRunner, TrinoFileSystem trinoFileSystem, HiveMetastore hiveMetastore, List<TpchTable<?>> tables, Location externalLocation)
            throws Exception
    {
        // TpchObjectStoreHudiTablesInitializer wont work here because it cannot cast CoordinatorDispatcherConnector to ObjectStoreConnector.
        // It is not-trivial to expose the injector or any other internal component of the connector (even just for testing). So TpchWarpSpeedObjectStoreHudiTablesInitializer stays with using the old approach of getting a metastore
        TpchWarpSpeedObjectStoreHudiTablesInitializer loader = new TpchWarpSpeedObjectStoreHudiTablesInitializer(hiveMetastore, trinoFileSystem, tables);
        loader.initializeTables(queryRunner, externalLocation, TPCH_SCHEMA);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("great_lakes")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new ObjectStorePlugin());
                queryRunner.createCatalog("great_lakes", "great_lakes", connectorProperties);
                queryRunner.execute("CREATE SCHEMA great_lakes." + TPCH_SCHEMA);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        //noinspection resource
        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addConnectorProperty("hive.metastore", "file")
                .addConnectorProperty("hive.metastore.catalog.dir", "local://" + createTempDirectory(null).toFile().getPath())
                .addConnectorProperty("fs.local.enabled", "true")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(ObjectStoreQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
