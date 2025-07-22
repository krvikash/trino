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

import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

abstract class BaseS3AndUnityMetastoreDeltaConnectorSmokeTest
        extends BaseUnityMetastoreDeltaConnectorSmokeTest
{
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_LOGIN = requireEnv("DATABRICKS_LOGIN");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_UNITY_EXTERNAL_LOCATION = requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");

    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");

    private static final DatabricksSqlExecutor DATABRICKS = new DatabricksSqlExecutor(DATABRICKS_UNITY_JDBC_URL, DATABRICKS_LOGIN, DATABRICKS_TOKEN);

    protected abstract Map<String, String> getAdditionalDeltaLakeProperties();

    @Override
    public Map<String, String> getDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore.unity.host", DATABRICKS_HOST)
                .put("hive.metastore.unity.token", DATABRICKS_TOKEN)
                .put("hive.metastore.unity.catalog-owned-table-enabled", "true")
                .put("fs.native-s3.enabled", "true")
                .put("s3.region", DATABRICKS_AWS_REGION)
                .putAll(getAdditionalDeltaLakeProperties())
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
}
