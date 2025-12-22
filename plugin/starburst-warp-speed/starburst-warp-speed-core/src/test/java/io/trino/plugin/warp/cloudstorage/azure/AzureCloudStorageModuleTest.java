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
package io.trino.plugin.warp.cloudstorage.azure;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureAuthDefault;
import io.trino.filesystem.azure.AzureAuthOauth;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.ConfigFactoryWithPrefix;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.spi.connector.ConnectorContext;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_PATH;
import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class AzureCloudStorageModuleTest
{
    @Test
    void testAccessKeyAuth()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.AZURE.name(),
                        STORE_PATH, "abfss://path",
                        "azure.auth-type", "ACCESS_KEY",
                        "azure.access-key", "test-key"),
                null,
                Logger.get(AzureCloudStorageModuleTest.class)::warn);

        AzureCloudStorageModule module = new AzureCloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        Injector injector = Guice.createInjector(module);
        assertThat(injector.getInstance(AzureAuth.class)).isInstanceOf(AzureAuthAccessKey.class);
    }

    @Test
    void testOAuthAuth()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.AZURE.name(),
                        STORE_PATH, "abfss://path",
                        "azure.auth-type", "OAUTH",
                        "azure.oauth.endpoint", "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token",
                        "azure.oauth.client-id", "test-client-id",
                        "azure.oauth.secret", "test-secret",
                        "azure.oauth.tenant-id", "test-tenant-id"),
                null,
                Logger.get(AzureCloudStorageModuleTest.class)::warn);

        AzureCloudStorageModule module = new AzureCloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        Injector injector = Guice.createInjector(module);
        assertThat(injector.getInstance(AzureAuth.class)).isInstanceOf(AzureAuthOauth.class);
    }

    @Test
    void testDefaultAuth()
    {
        ConnectorContext connectorContext = new TestingConnectorContext();
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.AZURE.name(),
                        STORE_PATH, "abfss://path",
                        "azure.auth-type", "DEFAULT"),
                null,
                Logger.get(AzureCloudStorageModuleTest.class)::warn);

        AzureCloudStorageModule module = new AzureCloudStorageModule("test", connectorContext, configFactory, ForWarp.class);

        Injector injector = Guice.createInjector(module);
        assertThat(injector.getInstance(AzureAuth.class)).isInstanceOf(AzureAuthDefault.class);
    }
}
