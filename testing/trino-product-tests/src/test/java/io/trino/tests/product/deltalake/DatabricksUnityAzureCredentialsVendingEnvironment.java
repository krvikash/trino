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
package io.trino.tests.product.deltalake;

import org.testcontainers.trino.TrinoContainer;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

/**
 * Databricks Unity environment on Azure where Trino reads and writes ADLS with the credentials vended by Unity,
 * so Trino container is not given any storage credentials.
 */
public class DatabricksUnityAzureCredentialsVendingEnvironment
        extends DatabricksUnityEnvironment
{
    @Override
    protected Map<String, String> deltaCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>(super.deltaCatalogProperties());
        properties.put("hive.metastore.unity.vended-credentials-enabled", "true");
        return properties;
    }

    @Override
    protected Map<String, String> fileSystemProperties()
    {
        return Map.of(
                "fs.azure.enabled", "true",
                "azure.auth-type", "DEFAULT");
    }

    @Override
    protected void configureStorageCredentials(TrinoContainer trino) {}

    @Override
    public String unityCatalogName()
    {
        return requireEnv("AZURE_DATABRICKS_UNITY_CATALOG_NAME");
    }

    @Override
    public String unityExternalLocation()
    {
        return requireEnv("AZURE_DATABRICKS_UNITY_EXTERNAL_LOCATION");
    }

    @Override
    protected String databricksHost()
    {
        return requireEnv("AZURE_DATABRICKS_HOST");
    }

    @Override
    protected String databricksJdbcUrl()
    {
        return requireEnv("AZURE_DATABRICKS_UNITY_JDBC_URL");
    }

    @Override
    protected String databricksLoginEnvironmentVariable()
    {
        return "AZURE_DATABRICKS_LOGIN";
    }

    @Override
    protected String databricksTokenEnvironmentVariable()
    {
        return "AZURE_DATABRICKS_TOKEN";
    }
}
