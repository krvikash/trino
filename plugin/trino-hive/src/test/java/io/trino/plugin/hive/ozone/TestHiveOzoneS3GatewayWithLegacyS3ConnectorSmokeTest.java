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
package io.trino.plugin.hive.ozone;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_ACCESS_KEY;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_SECRET_KEY;

public class TestHiveOzoneS3GatewayWithLegacyS3ConnectorSmokeTest
        extends BaseHiveOzoneS3GatewayConnectorSmokeTest
{
    @Override
    protected Map<String, String> s3Config()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", DUMMY_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", DUMMY_SECRET_KEY)
                .put("hive.s3.endpoint", hiveOzoneS3Gateway.getApacheOzoneContainer().getS3EndpointAddress())
                .put("hive.s3.path-style-access", "true")
                .buildOrThrow();
    }
}
