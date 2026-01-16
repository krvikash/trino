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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Base64;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestGcsAndUnityMetastoreDeltaConnectorSmokeTest
        extends BaseGcsAndUnityMetastoreDeltaConnectorSmokeTest
{
    private static final String GCP_CREDENTIALS_KEY = new String(Base64.getDecoder().decode(requireEnv("DATABRICKS_UNITY_GCP_GCS_JSON_KEY")), UTF_8);

    @Override
    protected Map<String, String> getAdditionalDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("gcs.json-key", GCP_CREDENTIALS_KEY)
                .buildOrThrow();
    }

    @Test
    @Disabled
    @Override
    void testCatalogManagedTable()
    {
        // TODO (https://starburstdata.atlassian.net/browse/ENG-6676) enable the test when delta.feature.catalogManaged table feature is supported
    }
}
