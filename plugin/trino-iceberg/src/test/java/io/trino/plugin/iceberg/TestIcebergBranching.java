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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergBranching
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
        return queryRunner;
    }

    @Test
    void testCreateBranch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "(x int)")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH \"" + "test-branch" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "test-branch");

            assertUpdate("CREATE BRANCH \"" + "TEST-BRANCH" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "test-branch", "TEST-BRANCH");
        }
    }

    @Test
    void testCreateBranchAlreadyExist()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "(x int)")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("CREATE BRANCH main IN TABLE " + table.getName(), ".* Branch 'main' already exists");
        }
    }

    @Test
    void testReplaceBranchFail()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "AS SELECT 1 x")) {
            assertQueryFails("CREATE OR REPLACE BRANCH \"" + "audit" + "\" IN TABLE " + table.getName(), "The connector does not support replacing branches");
        }
    }

    @Test
    void testDropBranch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int)")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH \"" + "test-branch" + "\" IN TABLE " + table.getName());
            assertUpdate("CREATE BRANCH \"" + "TEST-BRANCH" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "test-branch", "TEST-BRANCH");

            assertUpdate("DROP BRANCH \"" + "test-branch" + "\" IN TABLE " + table.getName());
            assertUpdate("DROP BRANCH \"" + "TEST-BRANCH" + "\" IN TABLE " + table.getName());

            assertQueryFails("DROP BRANCH \"" + "test-branch" + "\" IN TABLE " + table.getName(), ".*Branch 'test-branch' does not exist");
            assertQueryFails("DROP BRANCH \"" + "TEST-BRANCH" + "\" IN TABLE " + table.getName(), ".*Branch 'TEST-BRANCH' does not exist");

            assertBranch(table.getName(), "main");
        }
    }

    @Test
    void testDropTagFail()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int)")) {
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.manageSnapshots()
                    .createTag("tag", icebergTable.currentSnapshot().snapshotId())
                    .commit();
            assertBranch(table.getName(), "main");

            assertQueryFails("DROP BRANCH tag IN TABLE " + table.getName(), ".*Branch 'tag' does not exist");
        }
    }

    @Test
    void testDropNonExistentBranchFail()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int)")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("DROP BRANCH dev IN TABLE " + table.getName(), ".* Branch 'dev' does not exist");
        }
    }

    @Test
    void testDropMainBranchFail()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int)")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("DROP BRANCH main IN TABLE " + table.getName(), "Cannot drop 'main' branch");
            assertBranch(table.getName(), "main");
        }
    }

    @Test
    void testFastForward()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "AS SELECT 1 x")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH dev IN TABLE " + table.getName());
            loadTable(table.getName()).newDelete()
                    .deleteFromRowFilter(alwaysTrue())
                    .toBranch("dev")
                    .commit();

            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 1");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'");

            assertUpdate("ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO dev");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'");
        }
    }

    @Test
    void testFastForwardSameBranch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "AS SELECT 1 x")) {
            assertBranch(table.getName(), "main");

            assertUpdate("ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO main");
            assertBranch(table.getName(), "main");
        }
    }

    @Test
    void testFastForwardNotExistent()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "(x int)")) {
            assertQueryFails(
                    "ALTER BRANCH \"non-existing-branch\" IN TABLE " + table.getName() + " FAST FORWARD TO main",
                    ".* Branch 'non-existing-branch' does not exist");
            assertQueryFails(
                    "ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO \"non-existing-branch\"",
                    ".* Branch 'non-existing-branch' does not exist");
        }
    }

    @Test
    void testFastForwardNotAncestor()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH dev IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            loadTable(table.getName()).newDelete()
                    .deleteFromRowFilter(alwaysTrue())
                    .toBranch("dev")
                    .commit();

            assertQueryFails(
                    "ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO dev",
                    "Branch 'main' is not an ancestor of 'dev'");
        }
    }

    @Test
    void testInsert()
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            // insert into main (default) branch
            assertUpdate("INSERT INTO " + table.getName() + " @ main VALUES (1, 2)", 1);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .isEqualTo(0L);

            // insert into another branch
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (10, 20), (30, 40)", 2);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .isEqualTo(2L);

            // insert into another branch with a partial column
            assertUpdate("INSERT INTO " + table.getName() + " @ dev (x) VALUES 50", 1);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .isEqualTo(3L);
        }
    }

    @Test
    void testInsertAfterSchemaEvolution()
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 2)", 1);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN z int");

            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ dev VALUES (1, 2, 3)",
                    "\\Qline 1:1: Insert query has mismatched column types: Table: [integer, integer], Query: [integer, integer, integer]");

            assertUpdate("INSERT INTO " + table.getName() + " @ dev SELECT x + 10, y + 10 FROM " + table.getName(), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 2, CAST(NULL AS integer))");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES (11, 12, CAST(NULL AS integer))");
        }
    }

    @Test
    void testInsertIntoNonExistentBranchFail()
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int)")) {
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ non_existing VALUES (1, 2)",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @Test
    void testInsertIntoTagFail()
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int)")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ tag VALUES (1, 2)",
                    ".* Branch 'tag' does not exist");
        }
    }

    @Test
    void testDelete()
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (1, 10), (2, 20), (3, 30)", 3);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .isEqualTo(3L);

            assertUpdate("DELETE FROM " + table.getName() + " @ dev");
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .isEqualTo(0L);
        }
    }

    @Test
    void testDeleteAfterSchemaEvolution()
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (1, 10), (2, 20), (3, 30)", 3);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");

            // TODO This should be fixed after once https://github.com/trinodb/trino/issues/23601 is resolved
            assertThat(query("DELETE FROM " + table.getName() + " @ dev WHERE y = 30")).nonTrinoExceptionFailure()
                    .hasMessageContaining("Invalid metadata file")
                    .hasStackTraceContaining("Cannot find field 'y'");

            // branch returns the latest schema once a new snapshot is created
            assertUpdate("DELETE FROM " + table.getName() + " @ dev WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 2, 3");
        }
    }

    @Test
    void testDeleteFromNonExistentBranchFail()
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int)")) {
            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ non_existing",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @Test
    void testDeleteFromTagFail()
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int)")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ tag",
                    ".* Branch 'tag' does not exist");
        }
    }

    @Test
    void testUpdate()
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES 1, 2, 3", 3);

            assertUpdate("UPDATE " + table.getName() + " @ dev SET x = x * 2", 3);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 2, 4, 6");
        }
    }

    @Test
    void testUpdateAfterSchemaEvolution()
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (1, 10), (2, 20), (3, 30)", 3);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertUpdate("UPDATE " + table.getName() + " @ dev SET y = 10", 3);

            // branch returns the latest schema once a new snapshot is created
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 1, 2, 3");
        }
    }

    @Test
    void testUpdateNonExistentBranchFail()
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int)")) {
            assertQueryFails(
                    "UPDATE " + table.getName() + " @ non_existing SET x = x * 2",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @Test
    void testUpdateTagFail()
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int)")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "UPDATE " + table.getName() + " @ tag SET x = 2",
                    ".* Branch 'tag' does not exist");
        }
    }

    @Test
    void testMerge()
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            assertUpdate("MERGE INTO " + table.getName() + " @ dev USING (VALUES 42) t(dummy) ON false " +
                    " WHEN NOT MATCHED THEN INSERT VALUES (1)", 1);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 1");

            assertUpdate("MERGE INTO " + table.getName() + " @ dev USING (VALUES 42) t(dummy) ON true " +
                    " WHEN MATCHED THEN UPDATE SET x = 10", 1);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 10");
        }
    }

    @Test
    void testMergeAfterSchemaEvolution()
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int, y int)")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertUpdate("MERGE INTO " + table.getName() + " @ dev USING (VALUES 42) t(dummy) ON false " +
                    " WHEN NOT MATCHED THEN INSERT VALUES (1, 2)", 1);

            // branch returns the latest schema once a new snapshot is created
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 1");
        }
    }

    @Test
    void testMergeNonExistentBranchFail()
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int)")) {
            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ not_existing USING (VALUES 42) t(dummy) ON false " +
                            " WHEN NOT MATCHED THEN INSERT VALUES (1)",
                    ".* Branch 'not_existing' does not exist");
        }
    }

    @Test
    void testMergeIntoTagFail()
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int)")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ tag USING (VALUES 42) t(dummy) ON false  WHEN NOT MATCHED THEN INSERT VALUES (1, 2)",
                    ".* Branch 'tag' does not exist");
        }
    }

    @Test
    void testDeletionVector()
    {
        try (TestTable table = newTrinoTable("test_deletion_vector", "(x int, y int) WITH (format_version = 3)", List.of("1, 10", "2, 20", "3, 30"))) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            // Repeat DELETE for ensuring that the previous deletion vector is taken into account
            assertUpdate("DELETE FROM " + table.getName() + " @ dev WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES (2, 20), (3, 30)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 10), (2, 20), (3, 30)");

            assertUpdate("DELETE FROM " + table.getName() + " @ dev WHERE x = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES (3, 30)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 10), (2, 20), (3, 30)");
        }
    }

    @Test
    void testCopyOnWrite()
    {
        try (TestTable table = newTrinoTable("test_cow", "(x int, y int) WITH (merge_mode = 'copy-on-write')", List.of("1, 10", "2, 20", "3, 30"))) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());

            assertUpdate("DELETE FROM " + table.getName() + " @ dev WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES (2, 20), (3, 30)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 10), (2, 20), (3, 30)");
        }
    }

    private void createTag(String table, String tag)
    {
        BaseTable icebergTable = loadTable(table);
        icebergTable.manageSnapshots()
                .createTag(tag, icebergTable.currentSnapshot().snapshotId())
                .commit();
    }

    private void assertBranch(String tableName, String... branchNames)
    {
        assertThat(computeActual("SHOW BRANCHES IN TABLE " + tableName).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(branchNames);
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
