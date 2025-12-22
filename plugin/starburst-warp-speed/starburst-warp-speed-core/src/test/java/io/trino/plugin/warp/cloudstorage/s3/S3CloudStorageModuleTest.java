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
package io.trino.plugin.warp.cloudstorage.s3;

import com.google.inject.Guice;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.ConfigFactoryWithPrefix;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.spi.connector.ConnectorContext;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_PATH;
import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_TYPE;

class S3CloudStorageModuleTest
{
    @Test
    void testDefaultCredentials()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.S3.name(),
                        STORE_PATH, "s3://path",
                        "s3.region", "us-east-1"),
                null,
                Logger.get(S3CloudStorageModuleTest.class)::warn);

        S3CloudStorageModule module = new S3CloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        // Just verify the module can be created - full instantiation requires AWS credentials
        Guice.createInjector(module);
    }

    @Test
    void testStaticCredentials()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.S3.name(),
                        STORE_PATH, "s3://path",
                        "s3.region", "us-east-1",
                        "s3.aws-access-key", "test-access-key",
                        "s3.aws-secret-key", "test-secret-key"),
                null,
                Logger.get(S3CloudStorageModuleTest.class)::warn);

        S3CloudStorageModule module = new S3CloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        // Just verify the module can be created - full instantiation requires AWS credentials
        Guice.createInjector(module);
    }

    @Test
    void testIamRoleCredentials()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.S3.name(),
                        STORE_PATH, "s3://path",
                        "s3.region", "us-east-1",
                        "s3.iam-role", "arn:aws:iam::123456789012:role/test-role"),
                null,
                Logger.get(S3CloudStorageModuleTest.class)::warn);

        S3CloudStorageModule module = new S3CloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        // Just verify the module can be created - full instantiation requires AWS credentials
        Guice.createInjector(module);
    }
}
