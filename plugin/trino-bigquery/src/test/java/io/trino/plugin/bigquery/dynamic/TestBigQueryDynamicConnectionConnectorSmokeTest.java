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
package io.trino.plugin.bigquery.dynamic;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.Session;
import io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner;
import io.trino.plugin.bigquery.BigQueryQueryRunner;
import io.trino.spi.security.Identity;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.CREDENTIALS_KEY_CREDENTIAL_NAME;
import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.PROJECT_ID_CREDENTIAL_NAME;
import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME;
import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigQueryDynamicConnectionConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String BIGQUERY_PROJECT_ID = requiredNonEmptySystemProperty("testing.bigquery-project-id");
    private static final String BIGQUERY_ALTERNATE_PROJECT_ID = requiredNonEmptySystemProperty("testing.bigquery-parent-project-id");
    private static final String BIGQUERY_CREDENTIALS_KEY = requiredNonEmptySystemProperty("testing.bigquery.credentials-key");
    private static final String BIGQUERY_ALTERNATE_CREDENTIALS_KEY = requiredNonEmptySystemProperty("testing.bigquery-case-insensitive.credentials-key");

    private final BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryDynamicConnectionQueryRunner.builder()
                .amendSession(sessionBuilder -> sessionBuilder
                        .setIdentity(Identity.forUser("test_user")
                                .withExtraCredentials(ImmutableMap.of(
                                        PROJECT_ID_CREDENTIAL_NAME, BIGQUERY_PROJECT_ID,
                                        CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_CREDENTIALS_KEY))
                                .build()))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        // https://starburstdata.atlassian.net/browse/ENG-8251
        // there is a brief delay before newly written data becomes visible in the BigQuery connector
        Failsafe.with(RetryPolicy.builder().withMaxAttempts(3).build())
                .run(super::testRowLevelDelete);
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.forUser("ADMIN")
                        .withExtraCredentials(ImmutableMap.of(
                                PROJECT_ID_CREDENTIAL_NAME, BIGQUERY_PROJECT_ID,
                                CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_CREDENTIALS_KEY))
                        .build())
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        assertThat(query(newSession, "SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s'", schemaName));
        assertUpdate(newSession, "DROP SCHEMA " + schemaName);
    }

    @Test
    void testMaterializationProjectAndDatasetCredentialName()
    {
        String dataset = "test_dataset_" + randomNameSuffix();
        String alternateProjectDataset = "test_alternate_dataset_" + randomNameSuffix();
        String mvName = "test_mv_" + randomNameSuffix();

        Session sessionWithMaterializationDataset = Session.builder(super.getSession())
                .setIdentity(Identity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.<String, String>builder()
                                .put(PROJECT_ID_CREDENTIAL_NAME, BIGQUERY_PROJECT_ID)
                                .put(CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_CREDENTIALS_KEY)
                                .put(VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME, dataset)
                                .buildOrThrow())
                        .build())
                .build();
        Session sessionWithMaterializationProject = Session.builder(super.getSession())
                .setIdentity(Identity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.<String, String>builder()
                                .put(PROJECT_ID_CREDENTIAL_NAME, BIGQUERY_PROJECT_ID)
                                .put(CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_CREDENTIALS_KEY)
                                .put(VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME, BIGQUERY_ALTERNATE_PROJECT_ID)
                                .put(VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME, alternateProjectDataset)
                                .buildOrThrow())
                        .build())
                .build();

        bigQuerySqlExecutor.createDataset(DatasetId.of(BIGQUERY_PROJECT_ID, dataset));
        bigQuerySqlExecutor.createDataset(DatasetId.of(BIGQUERY_ALTERNATE_PROJECT_ID, alternateProjectDataset));

        onBigQuery("CREATE MATERIALIZED VIEW test.%s AS SELECT count(1) AS cnt FROM tpch.region".formatted(mvName));
        try {
            assertThat(listTables(BIGQUERY_PROJECT_ID, dataset)).isEmpty();
            // With default session (no materialization dataset or project is set)
            assertQuery("SELECT cnt FROM test.%s".formatted(mvName), "VALUES (5)");
            List<String> tables = listTables(BIGQUERY_PROJECT_ID, dataset);
            assertThat(tables).isEmpty();

            // with view_materialization_dataset
            assertQuery(sessionWithMaterializationDataset, "SELECT cnt FROM test.%s".formatted(mvName), "VALUES (5)");
            tables = listTables(BIGQUERY_PROJECT_ID, dataset);
            assertThat(tables).hasSize(1);
            assertThat(tables.getFirst()).startsWith("_pbc_");

            // with view_materialization_project
            tables = listTables(BIGQUERY_ALTERNATE_PROJECT_ID, alternateProjectDataset);
            assertThat(tables).hasSize(0);
            assertQuery(sessionWithMaterializationProject, "SELECT cnt FROM test.%s".formatted(mvName), "VALUES (5)");
            tables = listTables(BIGQUERY_ALTERNATE_PROJECT_ID, alternateProjectDataset);
            assertThat(tables).hasSize(1);
            assertThat(tables.getFirst()).startsWith("_pbc_");
        }
        finally {
            onBigQuery("DROP MATERIALIZED VIEW test.%s".formatted(mvName));
            bigQuerySqlExecutor.dropDatasetIfExists(DatasetId.of(BIGQUERY_PROJECT_ID, dataset));
            bigQuerySqlExecutor.dropDatasetIfExists(DatasetId.of(BIGQUERY_ALTERNATE_PROJECT_ID, alternateProjectDataset));
        }
    }

    @Test
    void testMultipleSessionsWithDifferentCredentials()
            throws Exception
    {
        Session session1 = Session.builder(super.getSession())
                .setIdentity(Identity.forUser("Alice")
                        .withExtraCredentials(ImmutableMap.of(
                                PROJECT_ID_CREDENTIAL_NAME, BIGQUERY_PROJECT_ID,
                                CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_CREDENTIALS_KEY))
                        .build())
                .build();

        Session session2 = Session.builder(super.getSession())
                .setIdentity(Identity.forUser("Bob")
                        .withExtraCredentials(ImmutableMap.of(
                                CREDENTIALS_KEY_CREDENTIAL_NAME, BIGQUERY_ALTERNATE_CREDENTIALS_KEY)) // project-id will be picked from credential key
                        .build())
                .build();

        String schemaName1 = "test_schema_1_" + randomNameSuffix();
        String schemaName2 = "test_schema_2_" + randomNameSuffix();
        String tableName1 = "test_table_1_" + randomNameSuffix();
        String tableName2 = "test_table_2_" + randomNameSuffix();

        try (AutoCloseable _ = withSchema(session1, schemaName1);
                AutoCloseable _ = withSchema(session2, schemaName2);
                AutoCloseable _ = withTable(session1, "%s.%s".formatted(schemaName1, tableName1), "(id BIGINT, name VARCHAR)");
                AutoCloseable _ = withTable(session2, "%s.%s".formatted(schemaName2, tableName2), "(id BIGINT, name VARCHAR)")) {
            assertUpdate(session1, "INSERT INTO %s.%s VALUES (1, 'Alice')".formatted(schemaName1, tableName1), 1);
            assertQuery(session1, "SELECT * FROM %s.%s".formatted(schemaName1, tableName1), "VALUES (1, 'Alice')");

            assertUpdate(session2, "INSERT INTO %s.%s VALUES (2, 'Bob')".formatted(schemaName2, tableName2), 1);
            assertQuery(session2, "SELECT * FROM %s.%s".formatted(schemaName2, tableName2), "VALUES (2, 'Bob')");

            // Assert that session1 cannot see session2's schema and table, and vice versa
            assertQueryFails(session1, "SELECT * FROM %s.%s".formatted(schemaName2, tableName2), "(.*)Schema '%s' does not exist".formatted(schemaName2));
            assertQueryFails(session2, "SELECT * FROM %s.%s".formatted(schemaName1, tableName1), "(.*)Schema '%s' does not exist".formatted(schemaName1));
        }
    }

    @Test
    void testQueryWithoutCredentialsFails()
    {
        Session session = Session.builder(super.getSession())
                .setIdentity(Identity.forUser("Alice")
                        .build())
                .build();

        assertThatThrownBy(() -> computeActual(session, "SELECT count(*) FROM region"))
                .hasMessageContaining("Extra credential '%s' must be provided".formatted(CREDENTIALS_KEY_CREDENTIAL_NAME));
    }

    private void onBigQuery(@Language("SQL") String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }

    private List<String> listTables(String project, String dataset)
    {
        BigQuery bigQuery = bigQuerySqlExecutor.getBigQuery();

        return stream(bigQuery.listTables(DatasetId.of(project, dataset)).iterateAll().spliterator(), false)
                .map(Table::getTableId)
                .map(TableId::getTable)
                .collect(toImmutableList());
    }

    private AutoCloseable withSchema(Session session, String schemaName)
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute(session, "DROP SCHEMA IF EXISTS " + schemaName);
        queryRunner.execute(session, "CREATE SCHEMA " + schemaName);
        return () -> queryRunner.execute(session, "DROP SCHEMA IF EXISTS " + schemaName);
    }

    private AutoCloseable withTable(Session session, String tableName, String tableDefinition)
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute(session, "CREATE TABLE %s %s".formatted(tableName, tableDefinition));
        return () -> queryRunner.execute(session, "DROP TABLE %s".formatted(tableName));
    }
}
