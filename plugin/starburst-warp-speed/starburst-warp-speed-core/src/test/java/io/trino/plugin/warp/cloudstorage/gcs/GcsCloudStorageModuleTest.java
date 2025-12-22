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
package io.trino.plugin.warp.cloudstorage.gcs;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.filesystem.gcs.GcsAccessTokenAuth;
import io.trino.filesystem.gcs.GcsAuth;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.ConfigFactoryWithPrefix;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_PATH;
import static io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig.STORE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class GcsCloudStorageModuleTest
{
    @Test
    void testAccessTokenAuth()
    {
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.GS.name(),
                        STORE_PATH, "gs://path",
                        "gcs.auth-type", "ACCESS_TOKEN"),
                null,
                Logger.get(GcsCloudStorageModuleTest.class)::warn);

        GcsCloudStorageModule module = new GcsCloudStorageModule(configFactory, ForWarp.class);

        Injector injector = Guice.createInjector(module);
        assertThat(injector.getInstance(GcsAuth.class)).isInstanceOf(GcsAccessTokenAuth.class);
    }

    @Test
    void testServiceAccountAuth()
    {
        ConfigurationFactory configFactory = new ConfigFactoryWithPrefix(
                Map.of(
                        STORE_TYPE, StoreType.GS.name(),
                        STORE_PATH, "gs://path",
                        "gcs.auth-type", "SERVICE_ACCOUNT",
                        "gcs.json-key", "{}"),
                null,
                Logger.get(GcsCloudStorageModuleTest.class)::warn);

        GcsCloudStorageModule module = new GcsCloudStorageModule(configFactory, ForWarp.class);

        // Just verify the injector can be created - instantiating GcsAuth requires a valid private key
        Guice.createInjector(module);
    }
}
