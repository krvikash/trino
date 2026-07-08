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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
@Disabled("https://starburstdata.atlassian.net/browse/ENG-19790")
class TestGcsAndUnityMetastoreWithCredentialVendingDeltaConnectorSmokeTest
        extends BaseGcsAndUnityMetastoreDeltaConnectorSmokeTest
{
    @Override
    protected Map<String, String> getAdditionalDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore.unity.vended-credentials-enabled", "true")
                .put("gcs.auth-type", "APPLICATION_DEFAULT")
                .buildOrThrow();
    }
}
