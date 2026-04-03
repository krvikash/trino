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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergBranchingTest
        extends AbstractTestQueryFramework
{
    protected HiveMetastore metastore;
    protected TrinoFileSystemFactory fileSystemFactory;

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCreateBranch(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH \"" + "test-branch" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "test-branch");

            assertUpdate("CREATE BRANCH \"" + "TEST-BRANCH" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "test-branch", "TEST-BRANCH");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCreateBranchFromOtherBranch(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH \"" + "tmp" + "\" IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "tmp");

            assertUpdate("INSERT INTO " + table.getName() + " @ tmp VALUES 2", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'main'")).matches("VALUES 1");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'tmp'")).matches("VALUES 1, 2");

            assertUpdate("CREATE BRANCH \"" + "audit" + "\" IN TABLE " + table.getName() + " FROM tmp");
            assertBranch(table.getName(), "main", "tmp", "audit");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'main'")).matches("VALUES 1");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'tmp'")).matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'")).matches("VALUES 1, 2");

            assertUpdate("INSERT INTO " + table.getName() + " @ audit VALUES 3", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'main'")).matches("VALUES 1");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'tmp'")).matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'")).matches("VALUES 1, 2, 3");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCreateBranchFromNonExistentOtherBranchFail(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertQueryFails(
                    "CREATE BRANCH \"" + "tmp" + "\" IN TABLE " + table.getName() + " FROM not_found",
                    "line 1:1: Branch 'not_found' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCreateBranchAlreadyExist(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("CREATE BRANCH main IN TABLE " + table.getName(), ".* Branch 'main' already exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testReplaceBranchFail(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertQueryFails("CREATE OR REPLACE BRANCH \"" + "audit" + "\" IN TABLE " + table.getName(), "The connector does not support replacing branches");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTag(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG', retention = '90d') IN TABLE " + table.getName());

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
            assertThat(computeScalar("SELECT type FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo("TAG");
            assertThat(computeScalar("SELECT max_reference_age_in_ms FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo(7_776_000_000L);
            assertBranch(table.getName(), "main", "audit");

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagForSpecificSnapshot(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag_snapshot_id", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long firstSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG', snapshot_id = %d) IN TABLE %s".formatted(firstSnapshotId, table.getName()));

            assertThat(computeScalar("SELECT snapshot_id FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo(firstSnapshotId);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagFromBranch(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag_from_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName() + " FROM source");

            assertThat(computeScalar("SELECT type FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo("TAG");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testSourceBranchDontAffectTag(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag_from_branch_immutable", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " @ source VALUES 2", 1);

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName() + " FROM source");

            // Updating 'source' branch shouldn't affect 'audit' tag
            assertUpdate("INSERT INTO " + table.getName() + " @ source VALUES 3", 1);

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'source'"))
                    .matches("VALUES 1, 2, 3");

            // Dropping 'source' branch shouldn't affect 'audit' tag
            assertUpdate("DROP BRANCH source IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "audit");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1, 2");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagSnapshotIdAndFromBranchFails(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag_snapshot_and_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long snapshotId = getCurrentSnapshotId(table.getName());
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertQueryFails(
                    "CREATE BRANCH audit WITH (type = 'TAG', snapshot_id = %d) IN TABLE %s FROM source".formatted(snapshotId, table.getName()),
                    ".*Cannot specify both snapshot_id and FROM branch");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testReplaceTag(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_replace_tag", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertUpdate("CREATE OR REPLACE BRANCH audit WITH (type = 'TAG', retention = '1d') IN TABLE " + table.getName());

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1, 2");
            assertThat(computeScalar("SELECT max_reference_age_in_ms FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo(86_400_000L);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testReplaceTagToSpecificSnapshotId(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_replace_tag_to_snapshot_id", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long firstSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1, 2");

            assertUpdate("CREATE OR REPLACE BRANCH audit WITH (type = 'TAG', snapshot_id = " + firstSnapshotId + ") IN TABLE " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testReplaceTagFromBranchToSpecificSnapshotId(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_replace_tag_from_branch_to_snapshot_id", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            long secondSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName() + " FROM source");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");

            assertUpdate("CREATE OR REPLACE BRANCH audit WITH (type = 'TAG', snapshot_id = " + secondSnapshotId + ") IN TABLE " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1, 2");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagAlreadyExists(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_existing_tag", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());

            assertQueryFails(
                    "CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName(),
                    ".*Branch 'audit' already exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagIfNotExists(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_tag_if_exists", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());
            long taggedSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            assertUpdate("CREATE BRANCH IF NOT EXISTS audit WITH (type = 'TAG') IN TABLE " + table.getName());

            assertThat(computeScalar("SELECT snapshot_id FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo(taggedSnapshotId);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testReplaceTagCreatesWhenNoExistingTag(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_replace_tag", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE OR REPLACE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testShowBranches(int formatVersion)
    {
        // SHOW BRANCHES returns both branches and tags because tags are managed through branch syntax
        try (TestTable table = newTrinoTable("test_show_branches", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertBranch(table.getName(), "main");

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "audit");

            assertUpdate("CREATE BRANCH dev IN TABLE " + table.getName());
            assertBranch(table.getName(), "main", "audit", "dev");
        }
    }

    // The following tests document known limitations of routing tags through branch syntax.
    // branchExists() returns true for any ref (including tags) so that DROP BRANCH can drop tags.
    // As a side effect, the engine-level CreateBranchTask intercepts FAIL/IGNORE save modes
    // before the connector is called, producing branch-oriented error messages for tags.

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateBranchFailsWhenTagWithSameNameExists(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_branch_tag_name_conflict", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());

            // Engine intercepts with "Branch already exists" because branchExists() returns true for tags
            assertQueryFails(
                    "CREATE BRANCH audit IN TABLE " + table.getName(),
                    ".*Branch 'audit' already exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateBranchIfNotExistsNoOpsWhenTagWithSameNameExists(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_create_branch_if_not_exists_tag_conflict", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName());

            // Engine silently no-ops because branchExists() returns true for the tag
            assertUpdate("CREATE BRANCH IF NOT EXISTS audit IN TABLE " + table.getName());

            // The ref is still a tag, no branch was created
            assertThat(computeScalar("SELECT type FROM \"" + table.getName() + "$refs\" WHERE name = 'audit'"))
                    .isEqualTo("TAG");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagRequiresSnapshotForEmptyTable(int formatVersion)
    {
        TrinoCatalog catalog = getTrinoCatalog(metastore, fileSystemFactory, "iceberg");
        SchemaTableName tableName = new SchemaTableName("tpch", "test_create_tag_no_snapshot_" + formatVersion);
        catalog.newCreateTableTransaction(
                        SESSION,
                        tableName,
                        new Schema(Types.NestedField.required(1, "x", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, tableName)),
                        ImmutableMap.of("format_version", Integer.toString(formatVersion)))
                .commitTransaction();

        try {
            assertThat(catalog.loadTable(SESSION, tableName).currentSnapshot()).isNull();
            assertQueryFails(
                    "CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + tableName,
                    ".*Cannot create tag for a table with no snapshots");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagRetentionProtectsSnapshotFromExpiration(int formatVersion)
    {
        Session shortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .build();

        try (TestTable table = newTrinoTable("test_create_tag_retention_blocks_expire", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long taggedSnapshotId = getCurrentSnapshotId(table.getName());
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            long expirableSnapshotId = getCurrentSnapshotId(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);
            long currentSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate(
                    "CREATE BRANCH audit WITH (type = 'TAG', retention = '1d') IN TABLE " + table.getName() + " FROM source");

            assertUpdate(shortRetentionUnlocked, "ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '0s')");

            assertThat(getSnapshotIds(table.getName()))
                    .containsExactlyInAnyOrder(taggedSnapshotId, currentSnapshotId)
                    .doesNotContain(expirableSnapshotId);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCreateTagWithoutRetentionProtectsCurrentSnapshotFromExpiration(int formatVersion)
    {
        Session shortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .build();

        try (TestTable table = newTrinoTable("test_create_tag_without_retention_blocks_expire", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long taggedSnapshotId = getCurrentSnapshotId(table.getName());
            assertUpdate("CREATE BRANCH source IN TABLE " + table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            long expirableSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);
            long currentSnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE BRANCH audit WITH (type = 'TAG') IN TABLE " + table.getName() + " FROM source");

            assertUpdate(shortRetentionUnlocked, "ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '0s')");

            assertThat(getSnapshotIds(table.getName()))
                    .containsExactlyInAnyOrder(taggedSnapshotId, currentSnapshotId)
                    .doesNotContain(expirableSnapshotId);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'audit'"))
                    .matches("VALUES 1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDropBranch(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testDropTag(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.manageSnapshots()
                    .createTag("tag", icebergTable.currentSnapshot().snapshotId())
                    .commit();
            assertBranch(table.getName(), "main", "tag");

            assertUpdate("DROP BRANCH tag IN TABLE " + table.getName());
            assertBranch(table.getName(), "main");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDropNonExistentBranchFail(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("DROP BRANCH dev IN TABLE " + table.getName(), ".* Branch 'dev' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDropMainBranchFail(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertBranch(table.getName(), "main");
            assertQueryFails("DROP BRANCH main IN TABLE " + table.getName(), "Cannot drop 'main' branch");
            assertBranch(table.getName(), "main");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testFastForward(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testFastForwardSameBranch(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
            assertBranch(table.getName(), "main");

            assertUpdate("ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO main");
            assertBranch(table.getName(), "main");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testFastForwardNotExistent(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertQueryFails(
                    "ALTER BRANCH \"non-existing-branch\" IN TABLE " + table.getName() + " FAST FORWARD TO main",
                    ".* Branch 'non-existing-branch' does not exist");
            assertQueryFails(
                    "ALTER BRANCH main IN TABLE " + table.getName() + " FAST FORWARD TO \"non-existing-branch\"",
                    ".* Branch 'non-existing-branch' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testFastForwardNotAncestor(int formatVersion)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "WITH (format_version = " + formatVersion + ") AS SELECT 1 x")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testInsert(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testInsertAfterSchemaEvolution(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testInsertIntoNonExistentBranchFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_insert_into_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ non_existing VALUES (1, 2)",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testInsertIntoTagFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ tag VALUES (1, 2)",
                    ".*Branch 'tag' does not exist, but a tag with that name exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDelete(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDeleteAfterSchemaEvolution(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES (1, 10), (2, 20), (3, 30)", 3);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");

            // branch returns the latest schema once a new snapshot is created
            assertUpdate("DELETE FROM " + table.getName() + " @ dev WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 2, 3");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDeleteFromNonExistentBranchFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_delete_from_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ non_existing",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDeleteFromTagFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ tag",
                    ".*Branch 'tag' does not exist, but a tag with that name exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testUpdate(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertUpdate("CREATE BRANCH \"" + "dev" + "\" IN TABLE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " @ dev VALUES 1, 2, 3", 3);

            assertUpdate("UPDATE " + table.getName() + " @ dev SET x = x * 2", 3);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'dev'"))
                    .matches("VALUES 2, 4, 6");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testUpdateAfterSchemaEvolution(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testUpdateNonExistentBranchFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_update_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertQueryFails(
                    "UPDATE " + table.getName() + " @ non_existing SET x = x * 2",
                    ".* Branch 'non_existing' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testUpdateTagFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "UPDATE " + table.getName() + " @ tag SET x = 2",
                    ".*Branch 'tag' does not exist, but a tag with that name exists");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testMerge(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testMergeAfterSchemaEvolution(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testMergeNonExistentBranchFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_merge_branch", "(x int) WITH (format_version = " + formatVersion + ")")) {
            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ not_existing USING (VALUES 42) t(dummy) ON false " +
                            " WHEN NOT MATCHED THEN INSERT VALUES (1)",
                    ".* Branch 'not_existing' does not exist");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testMergeIntoTagFail(int formatVersion)
    {
        try (TestTable table = newTrinoTable("test_tag", "(x int, y int) WITH (format_version = " + formatVersion + ")")) {
            createTag(table.getName(), "tag");
            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ tag USING (VALUES 42) t(dummy) ON false  WHEN NOT MATCHED THEN INSERT VALUES (1, 2)",
                    ".*Branch 'tag' does not exist, but a tag with that name exists");
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

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCopyOnWrite(int formatVersion)
    {
        try (TestTable table = newTrinoTable(
                "test_cow",
                "(x int, y int) WITH (merge_mode = 'copy-on-write', format_version = " + formatVersion + ")",
                List.of("1, 10", "2, 20", "3, 30"))) {
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

    private List<Long> getSnapshotIds(String tableName)
    {
        MaterializedResult result = getQueryRunner().execute("SELECT snapshot_id FROM \"" + tableName + "$snapshots\"");
        return result.getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return loadTable(tableName).currentSnapshot().snapshotId();
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
