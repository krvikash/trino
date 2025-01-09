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
package io.trino.plugin.hudi;

import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                .setDataLoader(new TpchHudiTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .matches("\\QCREATE TABLE hudi." + schema + ".orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/orders'\n\\Q" +
                        ")");
    }

    @Test
    public void testHideHiveSysSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain("sys");
        assertQueryFails("SHOW TABLES IN hudi.sys", ".*Schema 'sys' does not exist");
    }

    @Test
    void testCreateDropDynamicCatalog()
    {
        String catalog = "new_catalog_" + randomNameSuffix();
        String createCatalogSql = "CREATE CATALOG %1$s USING hudi".formatted(catalog);
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "hudi", "mock_dynamic_listing", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "hudi", "mock_dynamic_listing", "tpch");
        // re-add the same catalog
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "hudi", "mock_dynamic_listing", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "hudi", "mock_dynamic_listing", "tpch");
    }

    @Test
    public void testCreateMultipleCatalogs()
    {
        String firstCatalog = "catalog_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING hudi
                WITH (
                   "hudi.parquet.use-column-names" = '%2$s'
                )""";
        try {
            assertUpdate(createCatalogSql.formatted(firstCatalog, "true"));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + firstCatalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(firstCatalog, "true"));
            assertQuerySucceeds("SHOW SCHEMAS FROM " + firstCatalog);

            assertUpdate(createCatalogSql.formatted(secondCatalog, "false"));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + secondCatalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(secondCatalog, "false"));
            assertQuerySucceeds("SHOW SCHEMAS FROM " + secondCatalog);
        }
        finally {
            assertUpdate("DROP CATALOG IF EXISTS " + firstCatalog);
            assertUpdate("DROP CATALOG IF EXISTS " + secondCatalog);
        }
    }

    @Test
    public void testRenameCatalog()
    {
        String oldCatalog = "catalog_rename_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING hudi
                WITH (
                   "hudi.parquet.use-column-names" = 'true'
                )""";
        assertUpdate(createCatalogSql.formatted(oldCatalog));

        String catalog = "catalog_rename_" + randomNameSuffix();
        assertUpdate(
                """
                ALTER CATALOG %s RENAME TO %s
                """
                        .formatted(oldCatalog, catalog));
        assertThatThrownBy(() -> computeActual("DROP CATALOG " + oldCatalog))
                .hasMessage("Catalog '%s' not found".formatted(oldCatalog));
        assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(catalog));
        assertQuerySucceeds("SHOW SCHEMAS FROM " + catalog);

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    public void testCatalogSetProperties()
    {
        String catalog = "catalog_set_props_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING hudi
                WITH (
                   "hudi.parquet.use-column-names" = '%2$s'
                )""";
        try {
            assertUpdate(createCatalogSql.formatted(catalog, "true"));

            assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(catalog, "true"));

            assertThatThrownBy(() -> assertUpdate(
                    """
                    ALTER CATALOG %s SET PROPERTIES
                       "hudi.parquet.use-column-names" = 'invalid'
                    """
                            .formatted(catalog))).hasMessageContaining("Invalid value 'invalid' for type boolean (property 'hudi.parquet.use-column-names')");
            assertUpdate(
                    """
                    ALTER CATALOG %1$s SET PROPERTIES
                       "hudi.parquet.use-column-names" = '%2$s'
                    """
                            .formatted(catalog, "false"));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(catalog, "false"));
            assertQuerySucceeds("SHOW SCHEMAS FROM " + catalog);
        }
        finally {
            assertUpdate("DROP CATALOG IF EXISTS " + catalog);
        }
    }
}
