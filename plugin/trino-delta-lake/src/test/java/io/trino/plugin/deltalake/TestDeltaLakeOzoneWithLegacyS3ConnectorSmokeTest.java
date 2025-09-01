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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.ozone.HiveOzoneS3Gateway;

import java.util.Map;

import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_ACCESS_KEY;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_SECRET_KEY;

public class TestDeltaLakeOzoneWithLegacyS3ConnectorSmokeTest
        extends BaseDeltaLakeOzoneConnectorSmokeTest
{
    @Override
    protected Map<String, String> s3Config(HiveOzoneS3Gateway hiveOzoneS3Gateway)
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.s3.max-connections", "2")
                .put("hive.s3.aws-access-key", DUMMY_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", DUMMY_SECRET_KEY)
                .put("hive.s3.endpoint", hiveOzoneS3Gateway.getApacheOzoneContainer().getS3EndpointAddress())
                .put("hive.s3.path-style-access", "true")
                .put("s3.exclusive-create", "false")
                // tests against legacy filesystem since TestDeltaLakeOzoneWithNativeS3ConnectorSmokeTest is using native-fs
                .put("fs.hadoop.enabled", "true")
                .buildOrThrow();
    }
}
