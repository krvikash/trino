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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.LocalSpoolingManager.LocalSpoolingPlugin;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.lang.String.format;
import static java.util.Base64.getEncoder;

public class DataframeQueryRunner
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(CUSTOMER, NATION);

    private DataframeQueryRunner() {}

    private static void runTestServer(Map<String, String> coordinatorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(getDefaultSession())
                .setCoordinatorProperties(ImmutableMap.copyOf(coordinatorProperties))
                // Extra properties are common for the coordinator and workers
                .addExtraProperties(spoolingProperties())
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        metastore.createDatabase(
                Database.builder()
                        .setDatabaseName("default")
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        queryRunner.installPlugin(new TestingHivePlugin(baseDir.toPath(), metastore));
        ImmutableMap<String, String> hiveProperties = ImmutableMap.<String, String>builder().buildOrThrow();
        queryRunner.createCatalog("hive", "hive", hiveProperties);
        queryRunner.execute("CREATE SCHEMA hive.tpch");

        for (TpchTable<?> table : REQUIRED_TPCH_TABLES) {
            queryRunner.execute(
                    format("CREATE TABLE hive.tpch.%s AS SELECT * FROM tpch.tiny.%s",
                            table.getTableName(),
                            table.getTableName()));
        }

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = new HashMap<>(ImmutableMap.<String, String>builder().buildOrThrow());
        String catalogType = icebergProperties.get("iceberg.catalog.type");
        Optional<File> metastoreDirectory = Optional.empty();
        Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
        if (catalogType == null) {
            icebergProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
            icebergProperties.put("hive.metastore.catalog.dir", dataDir.toString());
            icebergProperties.put("fs.hadoop.enabled", "true");
        }

        queryRunner.createCatalog("iceberg", "iceberg", icebergProperties);
        queryRunner.execute("CREATE SCHEMA iceberg.default");

        // Segments are spooled to a temporary directory, so no object storage is needed
        queryRunner.installPlugin(new LocalSpoolingPlugin());
        queryRunner.loadSpoolingManager("test-local", ImmutableMap.of());
    }

    static void main()
            throws Exception
    {
        Logging.initialize();
        runTestServer(ImmutableMap.of("http-server.http.port", "8080"));

        Logger log = Logger.get(DataframeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
    }

    // Enables the arrow-preview{,+zstd,+lz4} spooled encodings
    private static Map<String, String> spoolingProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("protocol.spooling.enabled", "true")
                .put("protocol.spooling.shared-secret-key", getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded()))
                // Fetch segments through the coordinator, LocalSpoolingManager doesn't support direct access
                .put("protocol.spooling.retrieval-mode", "coordinator_proxy")
                .put("protocol.spooling.inlining.enabled", "true")
                .put("protocol.spooling.inlining.max-size", "1MB")
                .put("protocol.spooling.inlining.max-rows", "1000")
                .put("protocol.spooling.initial-segment-size", "1MB")
                .put("protocol.spooling.encoding.arrow.enabled", "true")
                .put("protocol.spooling.encoding.arrow+zstd.enabled", "true")
                .put("protocol.spooling.encoding.arrow+lz4.enabled", "true")
                .buildOrThrow();
    }

    private static Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog("test")
                .setSchema("tpch")
                .build();
    }
}
