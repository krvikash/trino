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
import io.trino.plugin.hive.metastore.unity.DatabricksSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureUnityMetastoreDeltaConnectorSmokeTest
        extends BaseUnityMetastoreDeltaConnectorSmokeTest
{
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("AZURE_DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("AZURE_DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("AZURE_DATABRICKS_LOGIN");
    private static final String DATABRICKS_TOKEN = requireEnv("AZURE_DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("AZURE_DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_UNITY_EXTERNAL_LOCATION = requireEnv("AZURE_DATABRICKS_UNITY_EXTERNAL_LOCATION");
    private static final String DATABRICKS_AZURE_STORAGE_ACCESS_KEY = requireEnv("DATABRICKS_AZURE_STORAGE_ACCESS_KEY");

    private static final DatabricksSqlExecutor DATABRICKS = new DatabricksSqlExecutor(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_TOKEN);

    @Override
    public Map<String, String> getDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore.unity.host", DATABRICKS_HOST)
                .put("hive.metastore.unity.token", DATABRICKS_TOKEN)
                .put("hive.metastore.unity.catalog-managed-table-enabled", "true")
                .put("fs.native-azure.enabled", "true")
                .put("azure.auth-type", "ACCESS_KEY")
                .put("azure.access-key", DATABRICKS_AZURE_STORAGE_ACCESS_KEY)
                .buildOrThrow();
    }

    @Override
    protected String getDatabricksUnityExternalLocation()
    {
        return DATABRICKS_UNITY_EXTERNAL_LOCATION;
    }

    @Override
    protected String getDatabricksUnityCatalogName()
    {
        return DATABRICKS_UNITY_CATALOG_NAME;
    }

    @Override
    protected SqlExecutor onDatabricks()
    {
        return DATABRICKS;
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
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
}
