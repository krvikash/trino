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
package io.trino.plugin.objectstore;

import io.trino.plugin.hive.BaseTestHiveOnDataLake;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static java.util.regex.Pattern.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Test certain Hive features like flush_metadata_cache procedure via Object Store connector.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // some tests depend on metadata cache
public abstract class BaseTestObjectStoreHiveOnDataLake
        extends BaseTestHiveOnDataLake
{
    public BaseTestObjectStoreHiveOnDataLake(String bucketName)
    {
        super(bucketName, new Hive3FlociDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
    }

    @Override
    protected boolean isObjectStore()
    {
        return true;
    }

    abstract boolean isGalaxyMetastore();

    @Test
    @Override
    public void testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplateCreatedOnTrino()
    {
        // It's important to mix case here to detect if we properly handle rewriting
        // properties between Trino and Hive (e.g for Partition Projection)
        String schemaName = "Hive_Datalake_MixedCase";
        String tableName = getRandomTestTableName();

        // We create new schema to include mixed case location path and create such keys in Object Store
        computeActual("CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')".formatted(schemaName, bucketName));
        if (isGalaxyMetastore()) {
            computeActual("GRANT ALL PRIVILEGES ON hive.\"Hive_Datalake_MixedCase\".\"*\" TO ROLE accountadmin WITH GRANT OPTION");
        }

        String storageFormat = format(
                "s3a://%s/%s/%s/short_name1=${short_name1}/short_name2=${short_name2}/",
                this.bucketName,
                schemaName,
                tableName);
        computeActual(
                "CREATE TABLE " + getFullyQualifiedTestTableName(schemaName, tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL2', 'CZ2'] " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true, " +
                        "  partition_projection_location_template='" + storageFormat + "' " +
                        ")");
        assertThat(
                hiveFlociDataLake.getHiveHadoop()
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(schemaName, tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+storage\\.location\\.template[ |]+" + quote(storageFormat) + "[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.values[ |]+PL2,CZ2[ |]+");
        testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplate(schemaName, tableName);
    }

    @Test
    @Override
    public void testHiveViewColumnComment()
    {
        String tableName = getRandomTestTableName();
        String viewName = getRandomTestTableName();
        String partitionedViewName = getRandomTestTableName();

        hiveFlociDataLake.runOnHive(format("CREATE TABLE %s(id int, name string) PARTITIONED BY (ds date)", getHiveTestTableName(tableName)));
        hiveFlociDataLake.runOnHive(format("CREATE VIEW %s(id, name COMMENT 'comment', ds COMMENT 'test comment') AS SELECT * FROM %s", getHiveTestTableName(viewName), getHiveTestTableName(tableName)));
        hiveFlociDataLake.runOnHive(format("CREATE VIEW %s(name COMMENT 'comment', ds COMMENT 'test comment') PARTITIONED ON (ds) AS SELECT name, ds FROM %s", getHiveTestTableName(partitionedViewName), getHiveTestTableName(tableName)));

        if (isGalaxyMetastore()) {
            computeActual("GRANT ALL PRIVILEGES ON %s TO ROLE accountadmin WITH GRANT OPTION".formatted(getFullyQualifiedTestTableName(viewName)));
            computeActual("GRANT ALL PRIVILEGES ON %s TO ROLE accountadmin WITH GRANT OPTION".formatted(getFullyQualifiedTestTableName(partitionedViewName)));
        }

        assertThat(query("DESCRIBE " + getFullyQualifiedTestTableName(viewName))).result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("id", "integer", "", "")
                        .row("name", "varchar", "", "comment")
                        .row("ds", "date", "", "test comment")
                        .build());

        assertThat(query("DESCRIBE " + getFullyQualifiedTestTableName(partitionedViewName))).result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("name", "varchar", "", "comment")
                        .row("ds", "date", "", "test comment")
                        .build());

        assertUpdate("DROP VIEW " + getFullyQualifiedTestTableName(viewName));
        assertUpdate("DROP VIEW " + getFullyQualifiedTestTableName(partitionedViewName));
        assertUpdate("DROP TABLE " + getFullyQualifiedTestTableName(tableName));
    }

    @Test
    @Override
    public void testUnsupportedCommentOnHiveView()
    {
        String viewName = getRandomTestTableName();

        hiveFlociDataLake.runOnHive("CREATE VIEW " + getHiveTestTableName(viewName) + " AS SELECT 1 x");
        if (isGalaxyMetastore()) {
            computeActual("ALTER VIEW %s SET AUTHORIZATION ROLE accountadmin".formatted(getFullyQualifiedTestTableName(viewName)));
        }

        try {
            assertQueryFails("COMMENT ON COLUMN " + getHiveTestTableName(viewName) + ".x IS NULL", "Hive views are not supported.*");
        }
        finally {
            hiveFlociDataLake.runOnHive("DROP VIEW " + getHiveTestTableName(viewName));
        }
    }

    @Test
    @Override
    public void testUnsupportedDropSchemaCascadeWithNonHiveTable()
    {
        // objectstore connector allows drop schema cascade with non-hive tables
        assertThatThrownBy(super::testUnsupportedDropSchemaCascadeWithNonHiveTable)
                .hasMessageMatching("Expected query to fail: DROP SCHEMA test_unsupported_drop_schema_cascade_.{10} CASCADE .+");
    }

    @Test
    @Override
    public void testCreateFunction()
    {
        // CREATE FUNCTION not supported by Galaxy so far
        assertThatThrownBy(super::testCreateFunction)
                .hasMessageContaining("Catalog and schema must be specified when function schema is not configured");
    }
}
