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

import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestS3AndUnityMetastoreDeltaConnectorSmokeTest
        extends BaseS3AndUnityMetastoreDeltaConnectorSmokeTest
{
    private static final String DATABRICKS_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID");
    private static final String DATABRICKS_AWS_SECRET_ACCESS_KEY = requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY");

    @Override
    protected Map<String, String> getAdditionalDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("s3.aws-access-key", DATABRICKS_AWS_ACCESS_KEY_ID)
                .put("s3.aws-secret-key", DATABRICKS_AWS_SECRET_ACCESS_KEY)
                .buildOrThrow();
    }

    @Test
    @Disabled
    @Override
    void testCatalogManagedTable()
    {
        // TODO (https://starburstdata.atlassian.net/browse/ENG-6677) enable the test when deletes on catalog managed tables are fixed
    }
}
