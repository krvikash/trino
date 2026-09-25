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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.spi.NodeVersion;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCopyOnWriteConcurrentCommit
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA_NAME = "test_schema";

    private final AtomicReference<Runnable> onBeginMergeTableLoad = new AtomicReference<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path dataDirectory = Files.createTempDirectory("test_copy_on_write_concurrent_commit");
        FileHiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                new LocalFileSystemFactory(dataDirectory),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local://"))
        {
            @Override
            public Optional<Table> getTable(String databaseName, String tableName)
            {
                if (isCalledFromBeginMerge()) {
                    Runnable action = onBeginMergeTableLoad.getAndSet(null);
                    if (action != null) {
                        action.run();
                    }
                }
                return super.getTable(databaseName, tableName);
            }
        };

        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory, () -> Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore))));
        queryRunner.createCatalog("iceberg", "iceberg");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA_NAME);
        return queryRunner;
    }

    @Test
    void testUpdateConflictingWithFileRewriteCommittedBeforeBeginMerge()
    {
        String tableName = "test_update_conflicting_with_file_rewrite_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x) WITH (merge_mode = 'copy-on-write') AS VALUES 1, 2", 2);
        assertThat(computeActual("SELECT file_path FROM \"" + tableName + "$files\"").getRowCount()).isEqualTo(1);

        onBeginMergeTableLoad.set(() -> assertUpdate("UPDATE " + tableName + " SET x = x + 10 WHERE x = 1", 1));
        assertThat(query("UPDATE " + tableName + " SET x = x + 20 WHERE x = 2"))
                .failure().hasMessageMatching("Failed to commit (the transaction )?during write: .*");
        assertThat(onBeginMergeTableLoad.get()).isNull();

        assertThat(query("SELECT x FROM " + tableName))
                .matches("VALUES 11, 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    private static boolean isCalledFromBeginMerge()
    {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                frame.getClassName().equals(IcebergMetadata.class.getName()) && frame.getMethodName().equals("beginMerge")));
    }
}
