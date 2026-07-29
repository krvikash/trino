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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.concurrent.MoreFutures;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import jakarta.annotation.Nullable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCopyOnWrite
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder().build();

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data")));
        return queryRunner;
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testWriteModeProperty(IcebergFileFormat format)
    {
        testWriteModeProperty(format, false);
        testWriteModeProperty(format, true);
    }

    private void testWriteModeProperty(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_write_mode_property",
                "(x int, part int) WITH (" + partitioning + " format='" + format + "', merge_mode = 'copy-on-write')")) {
            assertThat(getMergeMode(table.getName())).isEqualTo("copy-on-write");

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("merge_mode = 'copy-on-write'");
        }

        try (TestTable table = newTrinoTable(
                "test_write_mode_property",
                "(x int, part int) WITH (" + partitioning + " format='" + format + "'" + ", merge_mode = 'merge-on-read')")) {
            assertThat(getMergeMode(table.getName())).isEqualTo("merge-on-read");
        }

        try (TestTable table = newTrinoTable("test_write_mode_property", "(x int, part int) WITH (" + partitioning + " format='" + format + "')")) {
            assertThat(getMergeMode(table.getName())).isNull();
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES merge_mode = 'copy-on-write'");
            assertThat(getMergeMode(table.getName())).isEqualTo("copy-on-write");
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES merge_mode = 'merge-on-read'");
            assertThat(getMergeMode(table.getName())).isEqualTo("merge-on-read");

            assertThat(query("ALTER TABLE " + table.getName() + " SET PROPERTIES merge_mode = 'invalid'"))
                    .failure()
                    .hasMessage("line 1:63: Unable to set catalog 'iceberg' table property 'merge_mode' to ['invalid']: Unknown row-level operation mode: invalid");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteDelete(IcebergFileFormat format)
    {
        testCopyOnWriteDelete(format, false);
        testCopyOnWriteDelete(format, true);
    }

    private void testCopyOnWriteDelete(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write",
                "(x int, part int) WITH (" + partitioning + " format='" + format + "'" + ", merge_mode = 'copy-on-write')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .isNotEqualTo(files);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (2, 1), (3, 1)");

            // multiple deletes on multiple files
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 2)", 1);
            assertThat(getActiveFiles(table.getName())).hasSize(2);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 3", 2);
            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(files);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (2, 1)");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteUpdate(IcebergFileFormat format)
    {
        testCopyOnWriteUpdate(format, false);
        testCopyOnWriteUpdate(format, true);
    }

    private void testCopyOnWriteUpdate(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "'" + ", merge_mode = 'copy-on-write')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("UPDATE " + table.getName() + " SET x = 10 WHERE x = 1", 1);
            assertThat(getActiveFiles(table.getName()))
                    .hasSize(2) // Ideally, this should be 1. Delta Lake connector has the same behavior.
                    .doesNotContainAnyElementsOf(files);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 10, 2, 3");

            // multiple updates on multiple files
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 2)", 1);
            assertThat(getActiveFiles(table.getName())).hasSize(3);
            assertUpdate("UPDATE " + table.getName() + " SET x = 20 WHERE x = 3", 2);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 10, 2, 20, 20");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteMerge(IcebergFileFormat format)
    {
        testCopyOnWriteMerge(format, false);
        testCopyOnWriteMerge(format, true);
    }

    private void testCopyOnWriteMerge(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "'" + ", merge_mode = 'copy-on-write')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("MERGE INTO " + table.getName() + " t USING (VALUES 42) dummy(x) ON true " +
                    "WHEN MATCHED AND t.x = 1 THEN UPDATE SET x = 10 " +
                    "WHEN MATCHED AND t.x = 2 THEN DELETE", 2);
            assertThat(getActiveFiles(table.getName()))
                    .hasSize(2) // Ideally, this should be 1. Delta Lake connector has the same behavior.
                    .doesNotContainAnyElementsOf(files);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 10, 3");

            // multiple merges on multiple files
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 2)", 1);
            assertThat(getActiveFiles(table.getName())).hasSize(3);
            assertUpdate("MERGE INTO " + table.getName() + " t USING (VALUES 42) dummy(x) ON true " +
                    "WHEN MATCHED AND t.x = 3 THEN UPDATE SET x = 20 " +
                    "WHEN MATCHED AND t.x = 10 THEN DELETE", 3);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 20, 20");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteWithExistingPositionDeleteFiles(IcebergFileFormat format)
    {
        testCopyOnWriteWithExistingPositionDeleteFiles(format, false);
        testCopyOnWriteWithExistingPositionDeleteFiles(format, true);
    }

    private void testCopyOnWriteWithExistingPositionDeleteFiles(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write_existing_position_deletes",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET properties merge_mode = 'copy-on-write'");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);

            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .isNotEqualTo(files);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteWithExistingEqualityDeleteFiles(IcebergFileFormat format)
            throws Exception
    {
        testCopyOnWriteWithExistingEqualityDeleteFiles(format, false);
        testCopyOnWriteWithExistingEqualityDeleteFiles(format, true);
    }

    private void testCopyOnWriteWithExistingEqualityDeleteFiles(IcebergFileFormat format, boolean partitioned)
            throws Exception
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write_existing_equality_delete_",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET properties merge_mode = 'copy-on-write'");
            Table icebergTable = loadTable(table.getName());
            if (partitioned) {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.of(icebergTable.spec()),
                        Optional.of(new PartitionData(new Object[] {1})),
                        ImmutableMap.of("x", 2),
                        Optional.empty());
            }
            else {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("x", 2),
                        Optional.empty());
            }

            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(files); // We do not write equality delete files by engine, so the active files still remain unchanged
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteWithBothEqualityAndPositionDeleteFiles(IcebergFileFormat format)
            throws Exception
    {
        testCopyOnWriteWithBothEqualityAndPositionDeleteFiles(format, false);
        testCopyOnWriteWithBothEqualityAndPositionDeleteFiles(format, true);
    }

    private void testCopyOnWriteWithBothEqualityAndPositionDeleteFiles(IcebergFileFormat format, boolean partitioned)
            throws Exception
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write_existing_equality_delete_",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "'" + ", merge_mode = 'copy-on-write')",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            Table icebergTable = loadTable(table.getName());

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            if (partitioned) {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.of(icebergTable.spec()),
                        Optional.of(new PartitionData(new Object[] {1})),
                        ImmutableMap.of("x", 2),
                        Optional.empty());
            }
            else {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("x", 2),
                        Optional.empty());
            }

            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(files); // We do not write equality delete files by engine, so the active files remain unchanged
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testCopyOnWriteWithExistingDeletionVectors(IcebergFileFormat format)
    {
        testCopyOnWriteWithExistingDeletionVectors(format, false);
        testCopyOnWriteWithExistingDeletionVectors(format, true);
    }

    private void testCopyOnWriteWithExistingDeletionVectors(IcebergFileFormat format, boolean partitioned)
    {
        String partitioning = partitioned ? "partitioning = ARRAY['part']," : "";
        try (TestTable table = newTrinoTable(
                "test_copy_on_write_existing_deletion_vectors",
                "(x int, part int) WITH (" + partitioning + "format='" + format + "'," + "format_version = 3)",
                List.of("(1, 1)", "(2, 1)", "(3, 1)"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 1, 3");
            List<String> files = getActiveFiles(table.getName());
            assertThat(files).hasSize(1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET properties merge_mode = 'copy-on-write'");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertThat(getActiveFiles(table.getName()))
                    .hasSize(1)
                    .isNotEqualTo(files);
            assertThat(query("SELECT x FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }

    @Test
    void testCopyOnWriteWithPartitionEvolution()
    {
        try (TestTable table = newTrinoTable(
                "test_copy_on_write_partition_evolution",
                "(x int, part int) WITH (partitioning = ARRAY['part'], merge_mode = 'copy-on-write')",
                List.of("(1, 1)", "(2, 1)"))) {
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['x']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 2), (4, 2)", 2);
            assertThat(getActiveFiles(table.getName())).hasSize(3);

            // rewrites a file written under the old partition spec and removes a file written under the new one
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x IN (1, 3)", 2);
            assertThat(getActiveFiles(table.getName())).hasSize(2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (2, 1), (4, 2)");

            assertUpdate("UPDATE " + table.getName() + " SET x = x + 10", 2);
            assertThat(getActiveFiles(table.getName())).hasSize(2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (12, 1), (14, 2)");
        }
    }

    // Repeat test since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentNonOverlappingUpdate()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_non_overlapping_updates_copy_on_write_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioning = ARRAY['part'], merge_mode = 'copy-on-write') " +
                "AS VALUES (1, 10), (11, 20), (21, NULL), (31, 40)", 4);

        try {
            // update data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part IS NULL");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (2, 10), (12, 20), (22, NULL), (31, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // Repeat test since the tested aspect is inherently non-deterministic.
    @RepeatedTest(3)
    void testConcurrentOverlappingUpdate()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_overlapping_updates_copy_on_write_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioning = ARRAY['part'], merge_mode = 'copy-on-write') " +
                "AS VALUES (1, 10), (11, 20), (21, NULL), (31, 40)", 4);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE a > 11");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessageMatching("Failed to commit the transaction during write.*|" +
                                        "Failed to commit during write.*");
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successes = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successes).isGreaterThanOrEqualTo(1);
            // There can be different possible results depending on query order execution.
            switch ((int) successes) {
                case 1 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (22, NULL), (32, 40)");
                case 2 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (23, NULL), (33, 40)");
                case 3 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (24, NULL), (34, 40)");
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\" WHERE content = %d", tableName, FileContent.DATA.id())).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    @Nullable
    private String getMergeMode(String tableName)
    {
        MaterializedResult result = computeActual("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'write.merge.mode'");
        return result.getRowCount() == 0 ? null : (String) result.getOnlyValue();
    }
}
