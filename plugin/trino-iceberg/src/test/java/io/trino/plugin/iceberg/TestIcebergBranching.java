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
