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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Redundant over TestHiveConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like views may be supported by Hive only.
public class TestHiveConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_MULTI_STATEMENT_WRITES -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE hive.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'PARQUET'\n" +
                        ")");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        // Override because HivePrincipal's username is case-sensitive unlike TrinoPrincipal
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied: Cannot create schema")
                .hasStackTraceContaining("CREATE SCHEMA");
    }

    @Test
    void testCreateDropDynamicCatalog()
    {
        String catalog = "new_catalog_" + randomNameSuffix();
        String createCatalogSql = "CREATE CATALOG %s USING hive".formatted(catalog);
        assertUpdate(createCatalogSql);
        assertCatalogs(availableCatalogs(Optional.of(catalog)));

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs(availableCatalogs(Optional.empty()));
        // re-add the same catalog
        assertUpdate(createCatalogSql);
        assertCatalogs(availableCatalogs(Optional.of(catalog)));

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs(availableCatalogs(Optional.empty()));
    }

    @Test
    public void testCreateMultipleCatalogs()
    {
        String firstCatalog = "catalog_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING hive
                WITH (
                   "hive.allow-register-partition-procedure" = '%2$s'
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
                CREATE CATALOG %1$s USING hive
                WITH (
                   "hive.allow-register-partition-procedure" = 'true'
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
        try {
            String createCatalogSql = "CREATE CATALOG %s USING hive";
            assertUpdate(createCatalogSql.formatted(catalog));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(catalog));

            assertUpdate(
                    """
                    ALTER CATALOG %s SET PROPERTIES
                       "hive.security" = 'read-only'
                    """
                            .formatted(catalog));
            assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA %s.test_dynamic".formatted(catalog))).hasMessageContaining("Access Denied: Cannot create schema test_dynamic");
            assertUpdate(
                    """
                    ALTER CATALOG %s SET PROPERTIES
                      "hive.security" = 'allow-all'
                    """
                            .formatted(catalog));
            assertUpdate("CREATE SCHEMA %s.test_dynamic".formatted(catalog));
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS %s.test_dynamic".formatted(catalog));
            assertUpdate("DROP CATALOG IF EXISTS " + catalog);
        }
    }

    private static String[] availableCatalogs(Optional<String> catalog)
    {
        ImmutableList.Builder<String> catalogs = ImmutableList.builder();
        catalogs.add("system")
                .add("hive")
                .add("tpch");
        catalog.ifPresent(catalogs::add);
        return catalogs.build().toArray(new String[0]);
    }
}
