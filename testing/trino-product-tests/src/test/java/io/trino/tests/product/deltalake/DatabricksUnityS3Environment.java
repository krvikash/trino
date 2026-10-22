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

import java.util.Map;
import java.util.Optional;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

/**
 * Databricks Unity environment on AWS where Trino reads and writes S3 with static credentials.
 */
public class DatabricksUnityS3Environment
        extends DatabricksUnityEnvironment
{
    @Override
    public String unityCatalogName()
    {
        return requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    }

    @Override
    public String unityExternalLocation()
    {
        return requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION");
    }

    @Override
    protected String databricksHost()
    {
        return requireEnv("DATABRICKS_HOST");
    }

    @Override
    protected String databricksJdbcUrl()
    {
        return requireEnv("DATABRICKS_UNITY_JDBC_URL");
    }

    @Override
    protected String databricksLoginEnvironmentVariable()
    {
        return "DATABRICKS_LOGIN";
    }

    @Override
    protected String databricksTokenEnvironmentVariable()
    {
        return "DATABRICKS_TOKEN";
    }

    @Override
    protected Map<String, String> fileSystemProperties()
    {
        return Map.of(
                "fs.s3.enabled", "true",
                // We need to give access to bucket owner (the AWS account integrated with Databricks), otherwise files won't be readable from Databricks
                "s3.canned-acl", "BUCKET_OWNER_FULL_CONTROL");
    }

    @Override
    protected void configureStorageCredentials(TrinoContainer trino)
    {
        trino.withEnv("AWS_REGION", requireEnv("AWS_REGION"));
        trino.withEnv("AWS_ACCESS_KEY_ID", requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID"));
        trino.withEnv("AWS_SECRET_ACCESS_KEY", requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY"));
        Optional.ofNullable(System.getenv("DATABRICKS_AWS_SESSION_TOKEN"))
                .ifPresent(token -> trino.withEnv("AWS_SESSION_TOKEN", token));
    }
}
