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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.glue.BaseSchemaMappingRulesTest;
import io.trino.plugin.iceberg.IcebergPlugin;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the schema-mapping-prefix feature end to end through SQL: a single Iceberg/Glue catalog exposes
 * its real schema {@code sales} a second time under the prefixed name {@code redirect_sales}, with no separate
 * Trino catalog involved (see {@link SchemaMappingTrinoGlueCatalog}).
 */
final class TestIcebergSchemaMappingRules
        extends BaseSchemaMappingRulesTest
{
    public TestIcebergSchemaMappingRules()
    {
        super("iceberg", new IcebergPlugin(), ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "glue")
                .buildOrThrow());
    }

    @Test
    void testRenameTable()
    {
        String source = "rename_table_source_" + randomNameSuffix();
        String target = "rename_table_target_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(source) + " (id INT)");

        assertUpdate("ALTER TABLE " + prefixedTable(source) + " RENAME TO " + prefixedTable(target));

        assertThat(getQueryRunner().tableExists(getSession(), source)).isFalse();
        assertThat(getQueryRunner().tableExists(getSession(), target)).isTrue();
    }

    @Test
    void testUpdate()
    {
        String table = "update_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES (1, 10.5), (2, 20.0)", 2);

        assertUpdate("UPDATE " + prefixedTable(table) + " SET total = 99.9 WHERE id = 1", 1);

        assertThat(computeScalar("SELECT total FROM " + realTable(table) + " WHERE id = 1"))
                .isEqualTo(99.9);
    }

    @Test
    void testDelete()
    {
        String table = "delete_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2, 3", 3);

        assertUpdate("DELETE FROM " + prefixedTable(table) + " WHERE id = 2", 1);

        assertThat(computeActual("SELECT id FROM " + realTable(table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 3);
    }

    @Test
    void testMerge()
    {
        String table = "merge_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES (1, 10.5), (2, 20.0)", 2);

        assertUpdate(
                "MERGE INTO " + prefixedTable(table) + " t USING (SELECT 1 AS id, 99.9 AS total) s " +
                        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET total = s.total",
                1);

        assertThat(computeScalar("SELECT total FROM " + realTable(table) + " WHERE id = 1"))
                .isEqualTo(99.9);
    }

    @Test
    void testTruncate()
    {
        String table = "truncate_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2, 3", 3);

        assertUpdate("TRUNCATE TABLE " + prefixedTable(table));

        assertQueryReturnsEmptyResult("SELECT id FROM " + realTable(table));
    }

    @Test
    void testOptimize()
    {
        String table = "optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1", 1);
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 2", 1);

        // Table procedure invocation through the prefixed name must not fail; the physical
        // effect (file compaction) is already covered by Iceberg's own optimize tests, this only
        // proves the procedure call resolves against the real schema.
        assertUpdate("ALTER TABLE " + prefixedTable(table) + " EXECUTE optimize");

        assertThat(computeActual("SELECT id FROM " + realTable(table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void testThreeWayJoinAcrossPrefixedSchemas()
    {
        String schemaB = "join3_b_" + randomNameSuffix();
        String schemaC = "join3_c_" + randomNameSuffix();
        String tableA = "join3_a_" + randomNameSuffix();
        String tableB = "join3_b_" + randomNameSuffix();
        String tableC = "join3_c_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + catalog() + "." + schemaB);
        assertUpdate("CREATE SCHEMA " + catalog() + "." + schemaC);
        assertUpdate("CREATE TABLE " + realTable(tableA) + " (id INT, name VARCHAR)");
        assertUpdate("INSERT INTO " + realTable(tableA) + " VALUES (1, 'a')", 1);
        assertUpdate("CREATE TABLE " + table(schemaB, tableB) + " (id INT, total DOUBLE)");
        assertUpdate("INSERT INTO " + table(schemaB, tableB) + " VALUES (1, 10.5)", 1);
        assertUpdate("CREATE TABLE " + table(schemaC, tableC) + " (id INT, note VARCHAR)");
        assertUpdate("INSERT INTO " + table(schemaC, tableC) + " VALUES (1, 'x')", 1);

        assertThat(computeActual(
                "SELECT a.name, b.total, c.note FROM " + prefixedTable(tableA) + " a " +
                        "JOIN " + table(schemaB, tableB) + " b ON a.id = b.id " +
                        "JOIN " + table(schemaC, tableC) + " c ON a.id = c.id")
                .getRowCount())
                .isEqualTo(1);

        assertUpdate("DROP TABLE " + table(schemaC, tableC));
        assertUpdate("DROP TABLE " + table(schemaB, tableB));
        assertUpdate("DROP TABLE " + realTable(tableA));
        assertUpdate("DROP SCHEMA " + catalog() + "." + schemaC);
        assertUpdate("DROP SCHEMA " + catalog() + "." + schemaB);
    }

    @Test
    void testMaterializedView()
    {
        String table = "materialized_view_" + randomNameSuffix();
        String mv = "materialized_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);

        assertUpdate("CREATE MATERIALIZED VIEW " + prefixedTable(mv) + " AS SELECT * FROM " + prefixedTable(table));
        assertUpdate("REFRESH MATERIALIZED VIEW " + prefixedTable(mv), 2);

        assertThat(computeActual("SELECT id FROM " + prefixedTable(mv)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);

        // SHOW CREATE MATERIALIZED VIEW must reflect the body as SchemaMappingSqlRewriter rewrites it for
        // the prefix it is read under, i.e. qualified with the prefixed schema, matching SHOW CREATE VIEW.
        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + prefixedTable(mv)))
                .contains("SELECT *")
                .contains(prefixedTable(table));

        assertUpdate("DROP MATERIALIZED VIEW " + prefixedTable(mv));
    }
}
