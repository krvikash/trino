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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import io.trino.Session;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.metastore.unity.DatabricksSqlExecutor;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY;
import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class BaseDynamicS3AndUnityMetastoreDeltaConnectorSmokeTest
        extends BaseUnityMetastoreDeltaConnectorSmokeTest
{
    // See https://starburstdata.atlassian.net/wiki/spaces/STARBURST/pages/5223383046/Unity+on+AWS
    // on the details for dynamic configuration Unity's test details: credentials and variables
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("DATABRICKS_LOGIN");
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");

    private static final String DATABRICKS_CATALOG_ALPHA_NAME = "alpha";
    private static final String DATABRICKS_CATALOG_BETA_NAME = "beta";

    private static final String DATABRICKS_CATALOG_ALPHA_TOKEN = requireEnv("DATABRICKS_CATALOG_ALPHA_TOKEN");
    private static final String DATABRICKS_CATALOG_BETA_TOKEN = requireEnv("DATABRICKS_CATALOG_BETA_TOKEN");

    private static final String DATABRICKS_ALICE_EXTERNAL_LOCATION = "s3://sep-unity-alice/external";
    private static final String DATABRICKS_BOB_EXTERNAL_LOCATION = "s3://sep-unity-bob/external";

    protected static final String ALICE_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_ALICE_AWS_ACCESS_KEY_ID");
    protected static final String ALICE_AWS_SECRET_KEY = requireEnv("DATABRICKS_ALICE_AWS_SECRET_KEY");

    protected static final String BOB_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_BOB_AWS_ACCESS_KEY_ID");
    protected static final String BOB_AWS_SECRET_KEY = requireEnv("DATABRICKS_BOB_AWS_SECRET_KEY");

    protected static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");
    protected static final String AWS_ACCOUNT_ID = requireEnv("AWS_SEP_DEV_ACCOUNT_ID");

    private static final DatabricksSqlExecutor DATABRICKS_ALPHA_EXECUTOR = new DatabricksSqlExecutor(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_CATALOG_ALPHA_TOKEN);
    private static final DatabricksSqlExecutor DATABRICKS_BETA_EXECUTOR = new DatabricksSqlExecutor(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_CATALOG_BETA_TOKEN);

    @Override
    protected String getDatabricksUnityExternalLocation()
    {
        return DATABRICKS_ALICE_EXTERNAL_LOCATION;
    }

    @Override
    protected String getDatabricksUnityCatalogName()
    {
        return DATABRICKS_CATALOG_ALPHA_NAME;
    }

    @Override
    protected SqlExecutor onDatabricks()
    {
        return DATABRICKS_ALPHA_EXECUTOR;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(SCHEMA_NAME)
                .addDeltaProperty("hive.metastore", "unity")
                .addDeltaProperty("delta.security", "allow-all")
                .addDeltaProperty("hive.metastore.unity.catalog-managed-table-enabled", "true")
                .addDeltaProperty("delta.dynamic-configuration-passthrough.enabled", "true")
                .addDeltaProperty("fs.s3.enabled", "true")
                .addDeltaProperty("s3.region", DATABRICKS_AWS_REGION)
                .addDeltaProperties(getDeltaLakeProperties())
                .setCreateTpchSchemas(false)
                .build();

        Failsafe.with(DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY, DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .run(() -> createTpchTables(
                        queryRunner,
                        getSessionWithAliceCredentials(queryRunner.getDefaultSession()),
                        DATABRICKS_ALICE_EXTERNAL_LOCATION,
                        DATABRICKS_ALPHA_EXECUTOR,
                        DATABRICKS_CATALOG_ALPHA_NAME));

        Failsafe.with(DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY, DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .run(() -> {
                    Session session = queryRunner.getDefaultSession();
                    createTpchTables(
                            queryRunner,
                            Session.builder(session)
                                    .setIdentity(Identity.from(session.getIdentity())
                                            .withExtraCredentials(ImmutableMap.<String, String>builder()
                                                    .putAll(getCatalogBetaCredentials())
                                                    .putAll(getExtraCredentialsForBob())
                                                    .buildOrThrow())
                                            .build())
                                    .build(),
                            DATABRICKS_BOB_EXTERNAL_LOCATION,
                            DATABRICKS_BETA_EXECUTOR,
                            DATABRICKS_CATALOG_BETA_NAME);
                });

        return queryRunner;
    }

    @Override
    protected Session getSession()
    {
        // default session is Alice user
        // all the inherited methods are executed with this Alice session but
        // passed via dynamic mode
        return getSessionWithAliceCredentials(super.getSession());
    }

    protected Session getSessionWithAliceCredentials(Session session)
    {
        return Session.builder(session)
                .setIdentity(Identity.from(session.getIdentity())
                        .withExtraCredentials(ImmutableMap.<String, String>builder()
                                .putAll(getCatalogAlphaCredentials())
                                .putAll(getExtraCredentialsForAlice())
                                .buildOrThrow())
                        .build())
                .build();
    }

    protected Map<String, String> getCatalogAlphaCredentials()
    {
        return ImmutableMap.<String, String>builder()
                .put("dynamic_host", DATABRICKS_HOST)
                .put("dynamic_token", DATABRICKS_CATALOG_ALPHA_TOKEN)
                .put("dynamic_catalog", DATABRICKS_CATALOG_ALPHA_NAME)
                .buildOrThrow();
    }

    protected Map<String, String> getCatalogBetaCredentials()
    {
        return ImmutableMap.<String, String>builder()
                .put("dynamic_host", DATABRICKS_HOST)
                .put("dynamic_token", DATABRICKS_CATALOG_BETA_TOKEN)
                .put("dynamic_catalog", DATABRICKS_CATALOG_BETA_NAME)
                .buildOrThrow();
    }

    protected abstract Map<String, String> getExtraCredentialsForAlice();

    protected abstract Map<String, String> getExtraCredentialsForBob();

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        """
                        CREATE TABLE delta.%s.region (
                           regionkey bigint,
                           name varchar,
                           comment varchar
                        )
                        WITH (
                           deletion_vectors_enabled = true,
                           location = '%s/%s/region'
                        )""".formatted(SCHEMA_NAME, getDatabricksUnityExternalLocation(), SCHEMA_NAME));
    }

    @Override
    @Test
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        // Override it to pass extraCredentials to ADMIN session
        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.forUser("ADMIN")
                        .withExtraCredentials(getCatalogAlphaCredentials())
                        .build())
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        try {
            assertThat(query(newSession, "SHOW SCHEMAS"))
                    .skippingTypesCheck()
                    .containsAll(format("VALUES '%s'", schemaName));
        }
        finally {
            assertUpdate(newSession, "DROP SCHEMA " + schemaName);
        }
    }

    @Test
    void testCrossCatalogIsolation()
    {
        Session alice = Session.builder(getSession())
                .setSchema(SCHEMA_NAME)
                .setIdentity(Identity.from(getSession().getIdentity())
                        .withExtraCredentials(ImmutableMap.<String, String>builder()
                                .putAll(getCatalogAlphaCredentials())
                                .putAll(getExtraCredentialsForAlice())
                                .buildOrThrow())
                        .build())
                .build();

        Session bob = Session.builder(getSession())
                .setSchema(SCHEMA_NAME)
                .setIdentity(Identity.from(getSession().getIdentity())
                        .withExtraCredentials(ImmutableMap.<String, String>builder()
                                .putAll(getCatalogBetaCredentials())
                                .putAll(getExtraCredentialsForBob())
                                .buildOrThrow())
                        .build())
                .build();

        String aliceTableName = "test_table_dynamic_alice" + randomNameSuffix();
        String aliceTableLocation = format("%s/%s/%s", DATABRICKS_ALICE_EXTERNAL_LOCATION, SCHEMA_NAME, aliceTableName);
        String bobTableName = "test_table_dynamic_bob" + randomNameSuffix();
        String bobTableLocation = format("%s/%s/%s", DATABRICKS_BOB_EXTERNAL_LOCATION, SCHEMA_NAME, bobTableName);

        try {
            // alice
            assertUpdate(alice, "CREATE TABLE %s (id INT, city VARCHAR) WITH (location='%s')".formatted(aliceTableName, aliceTableLocation));
            assertUpdate(alice, "INSERT INTO %s VALUES (1, 'New York'), (2, 'Los Angeles')".formatted(aliceTableName), 2);
            assertQuery(alice, "SELECT * FROM " + aliceTableName, "VALUES (1, 'New York'), (2, 'Los Angeles')");

            // bob
            assertUpdate(bob, "CREATE TABLE %s (id INT, city VARCHAR) WITH (location='%s')".formatted(bobTableName, bobTableLocation));
            assertUpdate(bob, "INSERT INTO %s VALUES (1, 'London'), (2, 'Paris')".formatted(bobTableName), 2);
            assertQuery(bob, "SELECT * FROM " + bobTableName, "VALUES (1, 'London'), (2, 'Paris')");

            // cannot read each other
            assertThatThrownBy(() -> computeActual(bob, "SELECT * FROM " + aliceTableName))
                    .hasStackTraceContaining("does not exist");

            assertThatThrownBy(() -> computeActual(alice, "SELECT * FROM " + bobTableName))
                    .hasStackTraceContaining("does not exist");
        }
        finally {
            assertUpdate(alice, "DROP TABLE IF EXISTS " + aliceTableName);
            assertUpdate(bob, "DROP TABLE IF EXISTS " + bobTableName);
        }
    }
}
