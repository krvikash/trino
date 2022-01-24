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

import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.warp2.TpchWarpSpeedObjectStoreHudiTablesInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.List;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;

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
}
