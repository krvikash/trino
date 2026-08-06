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
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

class TestObjectStoreSmokeGpuQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .configureGpuDistributedExecution()
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore");
            verify(dataDir.toFile().mkdirs());

            // Register the Iceberg connector functions (e.g. the theta-sketch statistics aggregation used on write)
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "local://" + dataDir)
                    .put("fs.local.enabled", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    // delta.parquet.time-zone defaults to the JVM zone; pin UTC so the Delta scan stays GPU-eligible
                    .put("delta.parquet.time-zone", "UTC")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch");

            // Hudi tables cannot be created through Trino, so load one directly
            new TpchObjectStoreHudiTablesInitializer(List.of(NATION))
                    .initializeTables(queryRunner, Location.of("local://" + dataDir.resolve("tpch")), "tpch");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testGpuScanDelegatedPerTableType()
    {
        for (TableType tableType : TableType.values()) {
            switch (tableType) {
                case HIVE, ICEBERG, DELTA -> {
                    String table = "nation_" + tableType.name().toLowerCase(ENGLISH);
                    // Hive writes ORC by default, which is not read on the GPU; force Parquet. Iceberg and Delta already use Parquet.
                    String properties = tableType == TableType.HIVE
                            ? "type = 'HIVE', format = 'PARQUET'"
                            : "type = '" + tableType.name() + "'";
                    assertUpdate("CREATE TABLE " + table + " WITH (" + properties + ") AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", 25);
                    assertThat(query("SELECT nationkey, name, regionkey FROM " + table))
                            .executesWithGpu(TableScanNode.class);
                    assertUpdate("DROP TABLE " + table);
                }
                // Hudi has no GPU support; the scan runs on the CPU. Its table is created in the runner setup.
                case HUDI -> assertThat(query("SELECT nationkey, name FROM nation"))
                        .executesWithoutGpu();
            }
        }
    }
}
