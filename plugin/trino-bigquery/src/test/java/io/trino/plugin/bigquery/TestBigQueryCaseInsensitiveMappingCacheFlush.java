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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD) // Run sequentially to remove tests interference, as both tests flush the cache
final class TestBigQueryCaseInsensitiveMappingCacheFlush
        extends AbstractTestQueryFramework
{
    private final BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.case-insensitive-name-matching", "true")
                        .put("bigquery.case-insensitive-name-matching.cache-ttl", "5m")
                        .buildOrThrow())
                .build();
    }

    @Test
    void testFlushMetadataProcedureWithAmbiguousTableName()
            throws Exception
    {
        String schemaName = "test_flush_metadata_schema_" + randomNameSuffix();
        try (AutoCloseable _ = withSchema(schemaName);
                TestTable fullyQualifiedName = new TestTable(bigQuerySqlExecutor, schemaName + ".Test_Table", "(c string)")) {
            String tableName = fullyQualifiedName.getName().split("\\.")[1];
            String tableNameUpperCase = tableName.toUpperCase(ENGLISH);
            String trinoTableName = tableName.toLowerCase(ENGLISH);
            // Fill caches
            assertThat(computeActual("SHOW TABLES FROM " + schemaName).getOnlyValue()).isEqualTo(trinoTableName);

            // Create a new table with an ambiguous name
            bigQuerySqlExecutor.execute(format("CREATE TABLE %s.%s (c string)", schemaName, tableNameUpperCase));

            // Queries still work on ambiguous table, as the cache is not yet flushed
            assertThat(computeActual("SHOW TABLES FROM " + schemaName).getOnlyValue()).isEqualTo(trinoTableName);
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'".formatted(schemaName)).getOnlyValue()).isEqualTo(trinoTableName);
            assertThat(computeActual("SELECT * FROM %s.%s".formatted(schemaName, trinoTableName))).isEmpty();

            assertUpdate("CALL system.flush_metadata_cache()");

            // Queries fail on ambiguous table name after cache is flushed
            assertThat(computeActual("SHOW TABLES FROM " + schemaName)).isEmpty(); // No tables are listed as schema is not present due to filtering out ambiguous schemas
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'".formatted(schemaName))).isEmpty();
            assertQueryFails("SELECT * FROM %s.%s".formatted(schemaName, trinoTableName), "Found ambiguous names in BigQuery when looking up '%s'.*".formatted(trinoTableName));
        }
    }

    @Test
    void testFlushMetadataProcedureWithAmbiguousSchemaName()
            throws Exception
    {
        String schemaName = "test_flush_metadata_schema_" + randomNameSuffix();
        String bigQuerySchema = schemaName + "_Test_Schema";
        String bigQuerySchemaUpperCase = bigQuerySchema.toUpperCase(ENGLISH);
        String trinoSchema = bigQuerySchema.toLowerCase(ENGLISH);
        try (AutoCloseable _ = withSchema(bigQuerySchema);
                TestTable fullyQualifiedName = new TestTable(bigQuerySqlExecutor, bigQuerySchema + ".test_table", "(c string)")) {
            String tableName = fullyQualifiedName.getName().split("\\.")[1];
            // Fill caches
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains(trinoSchema);

            // Create a new schema with an ambiguous name
            bigQuerySqlExecutor.execute("CREATE SCHEMA " + bigQuerySchemaUpperCase);

            // Queries still work on ambiguous schema, as the cache is not yet flushed
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains(trinoSchema);
            assertThat(computeActual("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%" + schemaName + "%'").getOnlyValue()).isEqualTo(trinoSchema);
            assertThat(computeActual("SHOW TABLES FROM " + trinoSchema).getOnlyValue()).isEqualTo(tableName);
            assertThat(computeActual("SELECT * FROM " + fullyQualifiedName.getName())).isEmpty();

            assertUpdate("CALL system.flush_metadata_cache()");

            // Queries fail on ambiguous schema name after cache is flushed
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).doesNotContain(trinoSchema); // Schema is not present as Trino filters out ambiguous schemas
            assertThat(computeActual("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%" + trinoSchema + "'")).isEmpty(); // Schema is not present as Trino filters out ambiguous schemas
            assertQueryFails("SHOW TABLES FROM " + trinoSchema, "Found ambiguous names in BigQuery when looking up '%s'.*".formatted(trinoSchema));
            assertQueryFails("SELECT * FROM " + fullyQualifiedName.getName(), "Found ambiguous names in BigQuery when looking up '%s'.*".formatted(trinoSchema));
        }
        finally {
            bigQuerySqlExecutor.execute("DROP SCHEMA IF EXISTS " + bigQuerySchemaUpperCase + " CASCADE");
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        bigQuerySqlExecutor.dropDatasetIfExists(schemaName);
        bigQuerySqlExecutor.createDataset(schemaName);
        return () -> bigQuerySqlExecutor.dropDatasetIfExists(schemaName);
    }
}
