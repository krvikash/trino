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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.BaseS3AndGlueTest;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseDeltaLakeS3AndGlueTest
        extends BaseS3AndGlueTest
{
    protected BaseDeltaLakeS3AndGlueTest(String bucketName)
    {
        super("partitioned_by", "location", bucketName);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(schemaName)
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", schemaPath())
                        .put("fs.s3.enabled", "true")
                        .put("delta.enable-non-concurrent-writes", "true")
                        .putAll(s3AndGlueProperties())
                        .buildOrThrow())
                .setSchemaLocation(schemaPath())
                .build();
        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        return queryRunner;
    }

    protected Map<String, String> s3AndGlueProperties()
    {
        return Map.of();
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile -> {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + Pattern.quote(locationDirectory) + partitionPart + "[a-zA-Z0-9_-]+$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        String locationDirectory = location.endsWith("/") ? location : location + "/";
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile -> {
            assertThat(metadataFile).matches("^" + Pattern.quote(locationDirectory) + "_delta_log/[0-9]+.json$");
            verifyPathExist(metadataFile);
        });

        assertThat(getExtendedStatisticsFileFromTableDirectory(location)).matches("^" + Pattern.quote(locationDirectory) + "_delta_log/_trino_meta/extended_stats.json$");
    }

    @Override
    protected void validateFilesAfterDrop(String location)
    {
        // In Delta table created with location in treated as external, so files are not removed
        assertThat(getTableFiles(location)).isNotEmpty();
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> !path.contains("_delta_log"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<String> getAllMetadataDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/metadata"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private String getExtendedStatisticsFileFromTableDirectory(String tableLocation)
    {
        return getOnlyElement(getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/_trino_meta"))
                .collect(Collectors.toUnmodifiableSet()));
    }

    @Test
    void testCreateDropDynamicCatalog()
    {
        String catalog = "new_catalog_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING delta_lake
                WITH (
                    "hive.metastore" = 'glue',
                    "hive.metastore.glue.default-warehouse-dir" = '%2$s'
                )""".formatted(catalog, schemaPath());
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "delta", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "delta", "tpch");
        // re-add the same catalog
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "delta", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "delta", "tpch");
    }

    @Test
    public void testCreateMultipleCatalogs()
    {
        String firstCatalog = "catalog_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING delta_lake
                WITH (
                   "hive.metastore" = 'glue',
                   "hive.metastore.glue.default-warehouse-dir" = '%2$s'
                )""";
        try {
            assertUpdate(createCatalogSql.formatted(firstCatalog, schemaPath()));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + firstCatalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(firstCatalog, schemaPath()));
            assertQuerySucceeds("SHOW SCHEMAS FROM " + firstCatalog);

            String newSchemaPath = schemaPath() + "/catalog_new";
            assertUpdate(createCatalogSql.formatted(secondCatalog, newSchemaPath));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + secondCatalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(secondCatalog, newSchemaPath));
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
                CREATE CATALOG %1$s USING delta_lake
                WITH (
                   "hive.metastore" = 'glue',
                   "hive.metastore.glue.default-warehouse-dir" = '%2$s'
                )""";
        assertUpdate(createCatalogSql.formatted(oldCatalog, schemaPath()));

        String catalog = "catalog_rename_" + randomNameSuffix();
        assertUpdate("ALTER CATALOG %s RENAME TO %s".formatted(oldCatalog, catalog));
        assertThatThrownBy(() -> computeActual("DROP CATALOG " + oldCatalog))
                .hasMessage("Catalog '%s' not found".formatted(oldCatalog));
        assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(catalog, schemaPath()));
        assertQuerySucceeds("SHOW SCHEMAS FROM " + catalog);

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    public void testCatalogSetProperties()
    {
        String catalog = "catalog_set_props_" + randomNameSuffix();
        String dynamicSchema = "test_dynamic_" + randomNameSuffix();
        String createCatalogSql =
                """
                CREATE CATALOG %1$s USING delta_lake
                WITH (
                   "hive.metastore" = 'glue',
                   "hive.metastore.glue.default-warehouse-dir" = '%2$s'
                )""";
        try {
            assertUpdate(createCatalogSql.formatted(catalog, schemaPath()));
            assertThat((String) computeActual("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                    .isEqualTo(createCatalogSql.formatted(catalog, schemaPath()));

            assertThatThrownBy(() -> assertUpdate(
                    """
                    ALTER CATALOG %s SET PROPERTIES
                       "hive.metastore" = 'invalid'
                    """
                            .formatted(catalog))).hasMessageContaining("Configuration property 'hive.metastore.glue.default-warehouse-dir' was not used");
            assertUpdate(
                    """
                    ALTER CATALOG %s SET PROPERTIES
                      "hive.metastore" = 'glue',
                      "fs.hadoop.enabled" = 'false',
                      "fs.native-s3.enabled" = 'true'
                    """
                            .formatted(catalog));
            assertUpdate("CREATE SCHEMA %s.%s".formatted(catalog, dynamicSchema));
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS %s.%s".formatted(catalog, dynamicSchema));
            assertUpdate("DROP CATALOG IF EXISTS " + catalog);
        }
    }
}
