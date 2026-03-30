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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.IOException;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestUnloadAbfs
        extends BaseUnloadFileSystemTest
{
    private final String container;
    private final String account;
    private final String accessKey;
    private final String bucketName;

    public TestUnloadAbfs()
    {
        container = requireEnv("ABFS_CONTAINER");
        account = requireEnv("ABFS_ACCOUNT");
        accessKey = requireEnv("ABFS_ACCESSKEY");
        bucketName = "test-unload-test-" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-azure.enabled", "true")
                        .put("azure.auth-type", "ACCESS_KEY")
                        .put("azure.access-key", accessKey)
                        .buildOrThrow())
                .build();
    }

    @Override
    protected TrinoFileSystemFactory getFileSystemFactory()
            throws IOException
    {
        AzureFileSystemConfig config = new AzureFileSystemConfig().setAuthType(AzureFileSystemConfig.AuthType.ACCESS_KEY);
        return new AzureFileSystemFactory(OpenTelemetry.noop(), new AzureAuthAccessKey(accessKey), config);
    }

    @Override
    protected String getLocation(String path)
    {
        return "abfs://%s@%s.dfs.core.windows.net/%s/%s".formatted(container, account, bucketName, path);
    }
}
