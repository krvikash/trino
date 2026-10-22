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
 * Databricks Unity environment where Trino reads and writes S3 with the credentials vended by Unity,
 * so Trino container is not given any AWS credentials.
 */
public class DatabricksUnityS3CredentialsVendingEnvironment
        extends DatabricksUnityS3Environment
{
    @Override
    protected Map<String, String> deltaCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>(super.deltaCatalogProperties());
        properties.put("hive.metastore.unity.vended-credentials-enabled", "true");
        properties.put("s3.region", "us-east-1");
        return properties;
    }

    @Override
    protected void configureStorageCredentials(TrinoContainer trino)
    {
        trino.withEnv("AWS_REGION", requireEnv("AWS_REGION"));
    }
}
