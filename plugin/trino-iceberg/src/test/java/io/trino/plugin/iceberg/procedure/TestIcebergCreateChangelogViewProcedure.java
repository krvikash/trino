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
package io.trino.plugin.iceberg.procedure;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableProperties;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergCreateChangelogViewProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .build();
    }

    @BeforeAll
    void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @Test
    void testCreateChangelogViewWithExplicitIdentifierColumns()
    {
        String tableName = "test_changelog_explicit_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(INT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewClassifiesInsert()
    {
        String tableName = "test_changelog_insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES (INT '3', VARCHAR 'c', VARCHAR 'insert')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewClassifiesPureDelete()
    {
        String tableName = "test_changelog_delete_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES (INT '1', VARCHAR 'a', VARCHAR 'delete')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewClassifiesInsertAndDeleteInSingleSnapshot()
    {
        String tableName = "test_changelog_insert_delete_merge_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            // Single MERGE commit that both deletes an existing row (id 1) and inserts a new row (id 3),
            // so the added and removed rows are classified from within one snapshot.
            assertUpdate("MERGE INTO " + tableName + " t USING (VALUES (1, 'x'), (3, 'c')) AS s(id, value) ON t.id = s.id " +
                    "WHEN MATCHED THEN DELETE " +
                    "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)", 2);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a', VARCHAR 'delete'), " +
                            "(INT '3', VARCHAR 'c', VARCHAR 'insert')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsForNonUniqueIdentifier()
    {
        String tableName = "test_changelog_non_unique_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (1, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = value || '_x' WHERE id = 1", 2);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertQueryFails(
                    "SELECT _change_type FROM " + tableName + "_changes",
                    ".*Identifier columns do not uniquely identify a row.*");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenNoIdentifierColumnsProvided()
    {
        String tableName = "test_changelog_no_ids_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSqlWithoutIdentifiers(tableName, startSnapshot, endSnapshot),
                    ".*No identifier_columns provided and table has no identifier field IDs.*");

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY[]"),
                    ".*No identifier_columns provided and table has no identifier field IDs.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenIdentifierColumnIsEmptyOrNull()
    {
        String tableName = "test_changelog_empty_id_entry_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['']"),
                    ".*identifier_columns entries cannot be null or empty.*");

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id', '']"),
                    ".*identifier_columns entries cannot be null or empty.*");

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY[CAST(NULL AS VARCHAR)]"),
                    ".*identifier_columns entries cannot be null or empty.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewSetsGeneratedComment()
    {
        String tableName = "test_changelog_comment_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            String expectedComment = "Generated by create_changelog_view for iceberg.tpch." + tableName
                    + " between snapshots " + startSnapshot + " and " + endSnapshot;
            assertThat(query("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'iceberg' AND schema_name = 'tpch' AND table_name = '" + tableName + "_changes'"))
                    .matches("VALUES VARCHAR '" + expectedComment + "'");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenSnapshotIdMissing()
    {
        String tableName = "test_changelog_missing_snapshot_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    "ALTER TABLE " + tableName + " EXECUTE create_changelog_view(end_snapshot_id => " + endSnapshot + ", identifier_columns => ARRAY['id'])",
                    ".*start_snapshot_id is required.*");

            assertQueryFails(
                    "ALTER TABLE " + tableName + " EXECUTE create_changelog_view(start_snapshot_id => " + endSnapshot + ", identifier_columns => ARRAY['id'])",
                    ".*end_snapshot_id is required.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewAcrossMultipleSnapshots()
    {
        String tableName = "test_changelog_multi_snapshot_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a', VARCHAR 'update_before'), " +
                            "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(INT '2', VARCHAR 'b', VARCHAR 'delete'), " +
                            "(INT '3', VARCHAR 'c', VARCHAR 'insert')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewMultipleUpdatesToSameTuple()
    {
        String tableName = "test_changelog_multi_update_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'b' WHERE id = 1", 1);
            long firstUpdateSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'c' WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_version_id, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a', BIGINT '" + firstUpdateSnapshot + "', VARCHAR 'update_before'), " +
                            "(INT '1', VARCHAR 'b', BIGINT '" + firstUpdateSnapshot + "', VARCHAR 'update_after'), " +
                            "(INT '1', VARCHAR 'b', BIGINT '" + endSnapshot + "', VARCHAR 'update_before'), " +
                            "(INT '1', VARCHAR 'c', BIGINT '" + endSnapshot + "', VARCHAR 'update_after')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenStartNotAncestorOfEnd()
    {
        String tableName = "test_changelog_not_ancestor_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long firstSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long secondSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, secondSnapshot, firstSnapshot, "ARRAY['id']"),
                    ".*is not an ancestor.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenSnapshotMissing()
    {
        String tableName = "test_changelog_missing_snapshot_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long firstSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long secondSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, firstSnapshot, 9999999999L, "ARRAY['id']"),
                    ".*End snapshot 9999999999 not found in table history.*");

            assertQueryFails(
                    createChangelogViewSql(tableName, 9999999998L, secondSnapshot, "ARRAY['id']"),
                    ".*Start snapshot 9999999998 not found in table history.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewReplacesExistingView()
    {
        String tableName = "test_changelog_replace_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));
            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(INT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenSourceTableMissing()
    {
        String missingTable = "this_table_does_not_exist_" + randomNameSuffix();
        assertQueryFails(
                "ALTER TABLE " + missingTable + " EXECUTE create_changelog_view(start_snapshot_id => 1, end_snapshot_id => 2, identifier_columns => ARRAY['id'])",
                ".*does not exist.*");
    }

    @Test
    void testCreateChangelogViewFailsWhenIdentifierColumnNotInSchema()
    {
        String tableName = "test_changelog_bad_col_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['missing_column']"),
                    ".*Identifier column 'missing_column' not found in table schema.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenIdentifierColumnIsReservedName()
    {
        String tableName = "test_changelog_reserved_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['_change_type']"),
                    ".*Identifier column '_change_type' uses a reserved column name.*");
            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['_CHANGE_TYPE']"),
                    ".*Identifier column '_CHANGE_TYPE' uses a reserved column name.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewWithStructNestedIdentifierColumn()
    {
        String tableName = "test_changelog_nested_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (address ROW(zip BIGINT, street VARCHAR) NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            loadTable(tableName).updateSchema().allowIncompatibleChanges().requireColumn("address.zip").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (ROW(BIGINT '1', 'one'), 'a'), (ROW(BIGINT '2', 'two'), 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE address.zip = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['address.zip']"));

            assertThat(query("SELECT address.zip, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(BIGINT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(BIGINT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewUsesStructNestedSchemaIdentifierFields()
    {
        String tableName = "test_changelog_nested_schema_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (address ROW(zip BIGINT, street VARCHAR) NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            loadTable(tableName).updateSchema()
                    .allowIncompatibleChanges()
                    .requireColumn("address.zip")
                    .setIdentifierFields("address.zip")
                    .commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (ROW(BIGINT '1', 'one'), 'a'), (ROW(BIGINT '2', 'two'), 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE address.zip = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSqlWithoutIdentifiers(tableName, startSnapshot, endSnapshot));

            assertThat(query("SELECT address.zip, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(BIGINT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(BIGINT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsIdentifierNestedInList()
    {
        String tableName = "test_changelog_list_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (addresses ARRAY(ROW(zip BIGINT)), value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (ARRAY[ROW(BIGINT '1')], 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (ARRAY[ROW(BIGINT '2')], 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['addresses.zip']"),
                    ".*Identifier column 'addresses.zip': Cannot add field zip as an identifier field: not a required field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsIdentifierNestedInMap()
    {
        String tableName = "test_changelog_map_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (addresses MAP(VARCHAR, ROW(zip BIGINT)), value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (MAP(ARRAY['home'], ARRAY[ROW(BIGINT '1')]), 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (MAP(ARRAY['home'], ARRAY[ROW(BIGINT '2')]), 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['addresses.zip']"),
                    ".*Identifier column 'addresses.zip': Cannot add field zip as an identifier field: not a required field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsIdentifierNestedInOptionalStruct()
    {
        String tableName = "test_changelog_optional_struct_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (address ROW(zip BIGINT), value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            loadTable(tableName).updateSchema().allowIncompatibleChanges().requireColumn("address.zip").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (ROW(BIGINT '1'), 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (ROW(BIGINT '2'), 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['address.zip']"),
                    ".*Identifier column 'address.zip': Cannot add field zip as an identifier field: must not be nested in an optional field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsNullableIdentifierColumn()
    {
        String tableName = "test_changelog_nullable_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*Identifier column 'id': Cannot add field id as an identifier field: not a required field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsFloatIdentifierColumn()
    {
        String tableName = "test_changelog_float_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id REAL NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1.0, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2.0, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*Identifier column 'id': Cannot add field id as an identifier field: must not be float or double field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewRejectsDoubleIdentifierColumn()
    {
        String tableName = "test_changelog_double_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id DOUBLE NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1.0, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2.0, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*Identifier column 'id': Cannot add field id as an identifier field: must not be float or double field.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsWhenSchemaMissing()
    {
        String missingSchema = "nonexistent_schema_" + randomNameSuffix();
        assertQueryFails(
                "ALTER TABLE " + missingSchema + ".some_table EXECUTE create_changelog_view(start_snapshot_id => 1, end_snapshot_id => 2, identifier_columns => ARRAY['id'])",
                ".*does not exist.*");
    }

    @Test
    void testCreateChangelogViewFailsWhenSourceColumnCollidesWithReservedName()
    {
        String tableName = "test_changelog_reserved_col_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, _change_type VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*Source table contains columns with reserved names: \\[_change_type\\].*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewAccessControl()
    {
        String tableName = "test_changelog_access_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertAccessDenied(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    "Cannot create view .*",
                    privilege(tableName + "_changes", CREATE_VIEW));

            assertAccessDenied(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    "View owner .* cannot create view that selects from .*",
                    privilege(tableName, CREATE_VIEW_WITH_SELECT_COLUMNS));

            assertUpdate(createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"));
            assertAccessDenied(
                    "SELECT * FROM " + tableName + "_changes",
                    "Cannot select from columns .* in table or view .*",
                    privilege(tableName, SELECT_COLUMN));
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsForMergeOnReadTable()
    {
        String tableName = "test_changelog_mor_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'merge-on-read')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*create_changelog_view requires a copy-on-write table.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsForDeleteModeMergeOnRead()
    {
        String tableName = "test_changelog_delete_mor_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            // write.delete.mode is not exposed as a Trino table property, so set it directly via the Iceberg API.
            loadTable(tableName).updateProperties().set(TableProperties.DELETE_MODE, "merge-on-read").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*create_changelog_view requires a copy-on-write table; property 'write.delete.mode' is set to 'merge-on-read'.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewFailsForUpdateModeMergeOnRead()
    {
        String tableName = "test_changelog_update_mor_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            // write.update.mode is not exposed as a Trino table property, so set it directly via the Iceberg API.
            loadTable(tableName).updateProperties().set(TableProperties.UPDATE_MODE, "merge-on-read").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertQueryFails(
                    createChangelogViewSql(tableName, startSnapshot, endSnapshot, "ARRAY['id']"),
                    ".*create_changelog_view requires a copy-on-write table; property 'write.update.mode' is set to 'merge-on-read'.*");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewSkipsWriteModeValidation()
    {
        String tableName = "test_changelog_mor_skip_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'merge-on-read')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate("ALTER TABLE " + tableName + " EXECUTE create_changelog_view(" +
                    "start_snapshot_id => " + startSnapshot + ", " +
                    "end_snapshot_id => " + endSnapshot + ", " +
                    "identifier_columns => ARRAY['id'], " +
                    "skip_write_mode_validation => true)");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewUsesSchemaIdentifierFields()
    {
        String tableName = "test_changelog_schema_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            loadTable(tableName).updateSchema().setIdentifierFields("id").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSqlWithoutIdentifiers(tableName, startSnapshot, endSnapshot));

            assertThat(query("SELECT id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(INT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewUsesCompositeSchemaIdentifierFields()
    {
        String tableName = "test_changelog_composite_schema_id_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (tenant_id INT NOT NULL, id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
        try {
            loadTable(tableName).updateSchema().setIdentifierFields("tenant_id", "id").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (10, 1, 'a'), (10, 2, 'b'), (20, 1, 'c')", 3);
            long startSnapshot = currentSnapshotId(tableName);
            assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE tenant_id = 10 AND id = 1", 1);
            long endSnapshot = currentSnapshotId(tableName);

            assertUpdate(createChangelogViewSqlWithoutIdentifiers(tableName, startSnapshot, endSnapshot));

            assertThat(query("SELECT tenant_id, id, value, _change_type FROM " + tableName + "_changes"))
                    .matches("VALUES " +
                            "(INT '10', INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                            "(INT '10', INT '1', VARCHAR 'a', VARCHAR 'update_before')");
        }
        finally {
            assertUpdate("DROP VIEW " + tableName + "_changes");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateChangelogViewViewAndSchemaNameCombinations()
    {
        for (boolean useCustomViewName : new boolean[] {false, true}) {
            for (boolean useCustomSchemaName : new boolean[] {false, true}) {
                String tableName = "test_changelog_combo_" + randomNameSuffix();
                String customViewName = "my_changelog_" + randomNameSuffix();
                String customSchemaName = "alt_schema_" + randomNameSuffix();
                String resolvedViewName = useCustomViewName ? customViewName : tableName + "_changes";
                String resolvedSchema = useCustomSchemaName ? customSchemaName : null;
                assertUpdate("CREATE TABLE " + tableName + " (id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')");
                try {
                    if (useCustomSchemaName) {
                        assertUpdate("CREATE SCHEMA " + customSchemaName);
                    }
                    assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
                    long startSnapshot = currentSnapshotId(tableName);
                    assertUpdate("UPDATE " + tableName + " SET value = 'a2' WHERE id = 1", 1);
                    long endSnapshot = currentSnapshotId(tableName);

                    StringBuilder sql = new StringBuilder("ALTER TABLE " + tableName + " EXECUTE create_changelog_view(")
                            .append("start_snapshot_id => ").append(startSnapshot).append(", ")
                            .append("end_snapshot_id => ").append(endSnapshot).append(", ")
                            .append("identifier_columns => ARRAY['id']");
                    if (useCustomViewName) {
                        sql.append(", view_name => '").append(customViewName).append("'");
                    }
                    if (useCustomSchemaName) {
                        sql.append(", schema_name => '").append(customSchemaName).append("'");
                    }
                    sql.append(")");
                    assertUpdate(sql.toString());

                    String qualifiedView = resolvedSchema != null ? resolvedSchema + "." + resolvedViewName : resolvedViewName;
                    assertThat(query("SELECT id, value, _change_type FROM " + qualifiedView))
                            .matches("VALUES " +
                                    "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                                    "(INT '1', VARCHAR 'a', VARCHAR 'update_before')");
                }
                finally {
                    String qualifiedView = resolvedSchema != null ? resolvedSchema + "." + resolvedViewName : resolvedViewName;
                    assertUpdate("DROP VIEW " + qualifiedView);
                    assertUpdate("DROP TABLE " + tableName);
                    if (useCustomSchemaName) {
                        assertUpdate("DROP SCHEMA IF EXISTS " + customSchemaName);
                    }
                }
            }
        }
    }

    private static @Language("SQL") String createChangelogViewSql(String tableName, long startSnapshot, long endSnapshot, String identifierColumns)
    {
        return "ALTER TABLE " + tableName + " EXECUTE create_changelog_view(start_snapshot_id => " + startSnapshot + ", end_snapshot_id => " + endSnapshot + ", identifier_columns => " + identifierColumns + ")";
    }

    private static @Language("SQL") String createChangelogViewSqlWithoutIdentifiers(String tableName, long startSnapshot, long endSnapshot)
    {
        return "ALTER TABLE " + tableName + " EXECUTE create_changelog_view(start_snapshot_id => " + startSnapshot + ", end_snapshot_id => " + endSnapshot + ")";
    }

    private long currentSnapshotId(String tableName)
    {
        return loadTable(tableName).currentSnapshot().snapshotId();
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
