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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.spi.Plugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseSchemaMappingRulesTest
        extends AbstractTestQueryFramework
{
    public static final String CATALOG = "test_catalog";
    private static final String PREFIX = "redirect_";
    public static final String REAL_SCHEMA = "sales";
    private static final String BUCKET = "test-router-queries-" + randomNameSuffix();

    private static final String CROSS_ACCOUNT_CATALOG = "cross_account_catalog";
    private static final String DEFAULT_CATALOG_ID = "111111111111";
    private static final String MAPPED_PREFIX = "mapped_";
    private static final String MAPPED_CATALOG_ID = "222222222222";
    private static final String MAPPED_ACCOUNT_TABLE_PREFIX = "mapped_account_";

    private final String connectorName;
    private final Plugin plugin;
    private final Map<String, String> catalogProperties;
    protected FlociS3AndGlue floci;
    private RecordingGlueProxy glueProxy;

    public BaseSchemaMappingRulesTest(String connectorName, Plugin plugin, Map<String, String> catalogProperties)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.plugin = requireNonNull(plugin, "plugin is null");
        this.catalogProperties = ImmutableMap.copyOf(catalogProperties);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        floci = closeAfterClass(new FlociS3AndGlue());
        floci.createBucket(BUCKET);

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog(CATALOG).setSchema(REAL_SCHEMA).build())
                .build();

        File dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve(getClass().getSimpleName()).toFile();
        verify(dataDirectory.mkdirs());

        queryRunner.installPlugin(plugin);
        queryRunner.createCatalog(
                CATALOG,
                connectorName,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.glue.schema-mapping-rules", PREFIX + ":")
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://" + BUCKET + "/")
                        .put("fs.s3.enabled", "true")
                        .putAll(floci.s3AndGlueProperties())
                        .putAll(catalogProperties)
                        .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + CATALOG + "." + REAL_SCHEMA + " WITH (location = 's3://" + BUCKET + "/" + REAL_SCHEMA + "')");

        // Floci ignores CatalogId, so record requests to verify routing, and deny the default account
        // access to mapped_account_* tables like Lake Formation does for tables of another account
        Map<String, String> glueProperties = floci.s3AndGlueProperties();
        glueProxy = closeAfterClass(new RecordingGlueProxy(
                URI.create(glueProperties.get("hive.metastore.glue.endpoint-url")),
                request -> request.operation().equals("GetTable")
                        && request.body().contains("\"CatalogId\":\"" + DEFAULT_CATALOG_ID + "\"")
                        && request.body().contains("\"Name\":\"" + MAPPED_ACCOUNT_TABLE_PREFIX)));
        ImmutableMap.Builder<String, String> crossAccountProperties = ImmutableMap.builder();
        crossAccountProperties.putAll(glueProperties);
        crossAccountProperties.putAll(catalogProperties);
        crossAccountProperties.put("hive.metastore.glue.endpoint-url", glueProxy.endpoint().toString());
        crossAccountProperties.put("hive.metastore.glue.catalogid", DEFAULT_CATALOG_ID);
        crossAccountProperties.put("hive.metastore.glue.schema-mapping-rules", MAPPED_PREFIX + ":" + MAPPED_CATALOG_ID);
        crossAccountProperties.put("fs.s3.enabled", "true");
        queryRunner.createCatalog(CROSS_ACCOUNT_CATALOG, connectorName, crossAccountProperties.buildKeepingLast());

        return queryRunner;
    }

    @Test
    void testExplain()
    {
        String table = "explain_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertQuerySucceeds("EXPLAIN SELECT * FROM " + prefixedTable(table));
        assertQuerySucceeds("EXPLAIN (TYPE VALIDATE) SELECT * FROM " + prefixedTable(table));

        assertThat((String) computeScalar("EXPLAIN SELECT * FROM " + prefixedTable(table)))
                .containsAnyOf(
                        "%s:%s:%s".formatted(CATALOG, prefixedSchemaName(REAL_SCHEMA), table), // HiveTableHandle.toString
                        "%s:%s.%s".formatted(CATALOG, prefixedSchemaName(REAL_SCHEMA), table)); // IcebergTableHandle.toString
        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testSelectSendsMappedCatalogId()
    {
        String table = MAPPED_ACCOUNT_TABLE_PREFIX + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + "(id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);

        assertThat(computeActual("SELECT * FROM %s.%s%s.%s".formatted(CROSS_ACCOUNT_CATALOG, MAPPED_PREFIX, REAL_SCHEMA, table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);
        assertThat(getTableRequests(table))
                .isNotEmpty()
                .allSatisfy(body -> assertThat(body)
                        .contains("\"CatalogId\":\"" + MAPPED_CATALOG_ID + "\"")
                        .contains("\"DatabaseName\":\"" + REAL_SCHEMA + "\""));

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testSelectSendsDefaultCatalogId()
    {
        String table = "default_account_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + "(id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);

        assertThat(computeActual("SELECT * FROM %s.%s.%s".formatted(CROSS_ACCOUNT_CATALOG, REAL_SCHEMA, table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);
        assertThat(getTableRequests(table))
                .isNotEmpty()
                .allSatisfy(body -> assertThat(body)
                        .contains("\"CatalogId\":\"" + DEFAULT_CATALOG_ID + "\"")
                        .contains("\"DatabaseName\":\"" + REAL_SCHEMA + "\""));

        assertUpdate("DROP TABLE " + realTable(table));
    }

    private List<String> getTableRequests(String table)
    {
        return glueProxy.requests("GetTable").stream()
                .map(RecordingGlueProxy.GlueRequest::body)
                .filter(body -> body.contains("\"Name\":\"" + table + "\""))
                .toList();
    }

    @Test
    void testCreateTable()
    {
        String table = "create_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + prefixedTable(table) + " (id INT)");

        assertThat(getQueryRunner().tableExists(getSession(), table)).isTrue();
    }

    @Test
    void testDropTable()
    {
        String table = "drop_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertUpdate("DROP TABLE " + prefixedTable(table));

        assertThat(getQueryRunner().tableExists(getSession(), table)).isFalse();
    }

    @Test
    void testInsert()
    {
        String table = "insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");

        assertUpdate("INSERT INTO " + prefixedTable(table) + " VALUES (1, 10.5), (2, 20.0)", 2);

        assertThat(query("TABLE " + prefixedTable(table)))
                .containsAll("TABLE " + realTable(table));
    }

    @Test
    void testShowCreateTable()
    {
        String table = "show_create_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        // SHOW CREATE TABLE must name the table qualified with the prefixed schema it was read under,
        // i.e. matching prefixedTable(table), not the real schema it is stored in.
        assertThat((String) computeScalar("SHOW CREATE TABLE " + prefixedTable(table)))
                .contains("CREATE TABLE " + prefixedTable(table))
                .doesNotContain(realTable(table));

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testAddColumn()
    {
        String table = "add_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertUpdate("ALTER TABLE " + prefixedTable(table) + " ADD COLUMN total DOUBLE");

        assertThat(computeActual("SELECT column_name FROM " + CATALOG + ".information_schema.columns WHERE table_schema = '" + REAL_SCHEMA + "' AND table_name = '" + table + "'")
                .getOnlyColumnAsSet())
                .contains("total");
    }

    @Test
    void testRenameColumn()
    {
        String table = "rename_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");

        assertUpdate("ALTER TABLE " + prefixedTable(table) + " RENAME COLUMN total TO amount");

        assertThat(computeActual("SELECT column_name FROM " + CATALOG + ".information_schema.columns WHERE table_schema = '" + REAL_SCHEMA + "' AND table_name = '" + table + "'")
                .getOnlyColumnAsSet())
                .contains("amount")
                .doesNotContain("total");
    }

    @Test
    void testDropColumn()
    {
        String table = "drop_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");

        assertUpdate("ALTER TABLE " + prefixedTable(table) + " DROP COLUMN total");

        assertThat(computeActual("SELECT column_name FROM " + CATALOG + ".information_schema.columns WHERE table_schema = '" + REAL_SCHEMA + "' AND table_name = '" + table + "'")
                .getOnlyColumnAsSet())
                .doesNotContain("total");
    }

    @Test
    void testMetadataTable()
    {
        String table = "metadata_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1", 1);
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 2", 1);

        assertThat(query("SELECT * FROM " + CATALOG + "." + PREFIX + REAL_SCHEMA + ".\"" + table + "$properties\""))
                .matches("SELECT * FROM " + CATALOG + "." + REAL_SCHEMA + ".\"" + table + "$properties\"");
    }

    @Test
    void testUnprefixedAndPrefixedSchemasAreDistinctNamespaces()
    {
        // "sales" (unprefixed, resolved by the default delegate) and "redirect_sales" (resolved through
        // the prefix rule to the same real schema "sales") must not be conflated in namespace listings.
        assertThatThrownBy(() -> computeActual("SELECT * FROM " + CATALOG + ".redirect_sales.does_not_exist"))
                .hasMessageContaining("does not exist");
    }

    @Test
    void testDropNonExistentTableIfExists()
    {
        assertUpdate("DROP TABLE IF EXISTS " + prefixedTable("does_not_exist_" + randomNameSuffix()));
    }

    @Test
    void testJoinAcrossPrefixedSchemas()
    {
        String otherSchema = "other_" + randomNameSuffix();
        String left = "join_left_" + randomNameSuffix();
        String right = "join_right_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + CATALOG + "." + otherSchema);
        assertUpdate("CREATE TABLE " + realTable(left) + " (id INT, name VARCHAR)");
        assertUpdate("INSERT INTO " + realTable(left) + " VALUES (1, 'a'), (2, 'b')", 2);
        assertUpdate("CREATE TABLE " + table(otherSchema, right) + " (id INT, total DOUBLE)");
        assertUpdate("INSERT INTO " + table(otherSchema, right) + " VALUES (1, 10.5), (2, 20.0)", 2);

        assertThat(computeActual(
                "SELECT l.name, r.total FROM " + prefixedTable(left) + " l JOIN " + table(otherSchema, right) + " r ON l.id = r.id")
                .getRowCount())
                .isEqualTo(2);

        assertUpdate("DROP TABLE " + table(otherSchema, right));
        assertUpdate("DROP SCHEMA " + CATALOG + "." + otherSchema);
    }

    @Test
    void testShowSchemas()
    {
        assertThat(computeActual("SHOW SCHEMAS FROM " + CATALOG).getOnlyColumnAsSet())
                .contains(PREFIX + REAL_SCHEMA);
    }

    @Test
    void testInformationSchemaTables()
    {
        String table = "info_schema_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertThat(computeActual(
                "SELECT table_name FROM " + CATALOG + ".information_schema.tables WHERE table_schema = '" + PREFIX + REAL_SCHEMA + "'")
                .getOnlyColumnAsSet())
                .contains(table);

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testDescribe()
    {
        String table = "describe_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, total DOUBLE)");

        assertThat(computeActual("DESCRIBE " + prefixedTable(table)).getMaterializedRows())
                .hasSize(2);

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testShowStats()
    {
        String table = "show_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);

        assertThat(computeActual("SHOW STATS FOR " + prefixedTable(table)).getRowCount())
                .isGreaterThan(0);

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testSelectView()
    {
        String table = "select_view_" + randomNameSuffix();
        String view = "select_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);
        assertUpdate("CREATE VIEW " + realTable(view) + " AS SELECT * FROM " + realTable(table));

        // The view body's unqualified schema reference must be resolved against the prefix it is read
        // under (see SchemaMappingSqlRewriter), not against the real schema the view is stored in.
        assertThat(computeActual("SELECT id FROM " + prefixedTable(view)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);

        assertUpdate("DROP VIEW " + realTable(view));
    }

    @Test
    void testCreateView()
    {
        String table = "create_view_" + randomNameSuffix();
        String view = "create_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + prefixedTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + prefixedTable(table) + " VALUES 1, 2", 2);

        assertUpdate("CREATE VIEW " + prefixedTable(view) + " AS SELECT * FROM " + prefixedTable(table));

        assertThat(computeActual("SELECT id FROM " + realTable(view)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);

        assertUpdate("DROP VIEW " + prefixedTable(view));
    }

    @Test
    void testShowCreateView()
    {
        String table = "show_create_view_" + randomNameSuffix();
        String view = "show_create_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("CREATE VIEW " + realTable(view) + " AS SELECT * FROM " + realTable(table));

        assertThat((String) computeScalar("SHOW CREATE VIEW " + prefixedTable(view)))
                .contains("\"" + PREFIX + REAL_SCHEMA + "\"." + table);

        assertUpdate("DROP VIEW " + realTable(view));
    }

    @Test
    void testDropView()
    {
        String view = "drop_view_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + prefixedTable(view) + " AS SELECT 1 x");

        assertUpdate("DROP VIEW " + prefixedTable(view));

        assertThat(getQueryRunner().tableExists(getSession(), view)).isFalse();
    }

    @Test
    void testViewBodyReferencingOtherSchemaByRealName()
    {
        // The view's own schema ("sales", read through its prefix) is not the only schema a stored body can
        // name: a body can also reference a second schema directly, by that schema's real (unprefixed) name,
        // the way a Hive/Trino view's stored SQL always does. Both the view's home schema and any other
        // schema its body names must resolve through the prefix.
        String otherSchema = "other_" + randomNameSuffix();
        String table = "cross_schema_" + randomNameSuffix();
        String view = "cross_schema_view_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + CATALOG + "." + otherSchema);
        assertUpdate("CREATE TABLE " + table(otherSchema, table) + " (id INT)");
        assertUpdate("INSERT INTO " + table(otherSchema, table) + " VALUES 1, 2", 2);
        assertUpdate("CREATE VIEW " + realTable(view) + " AS SELECT * FROM " + table(otherSchema, table));

        assertThat(computeActual("SELECT id FROM " + prefixedTable(view)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);

        assertUpdate("DROP VIEW " + realTable(view));
        assertUpdate("DROP TABLE " + table(otherSchema, table));
        assertUpdate("DROP SCHEMA " + CATALOG + "." + otherSchema);
    }

    public static String prefixedTable(String table)
    {
        return "%s.%s%s.%s".formatted(CATALOG, PREFIX, REAL_SCHEMA, table);
    }

    public static String realTable(String table)
    {
        return "%s.%s.%s".formatted(CATALOG, REAL_SCHEMA, table);
    }

    public static String catalog()
    {
        return CATALOG;
    }

    public static String realSchemaName()
    {
        return REAL_SCHEMA;
    }

    public static String table(String schema, String table)
    {
        return "%s.%s.%s".formatted(CATALOG, schema, table);
    }

    /**
     * The catalog-qualified prefixed name of a real schema other than {@link #REAL_SCHEMA}, e.g. to probe
     * how a nonexistent real schema is reported under the prefix.
     */
    public static String prefixedSchema(String realSchemaName)
    {
        return "%s.%s".formatted(CATALOG, prefixedSchemaName(realSchemaName));
    }

    /**
     * The unqualified prefixed name of a real schema, e.g. as a {@code schema_name} procedure argument.
     */
    public static String prefixedSchemaName(String realSchemaName)
    {
        return PREFIX + realSchemaName;
    }
}
