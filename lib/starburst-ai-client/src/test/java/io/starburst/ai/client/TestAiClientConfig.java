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
package io.starburst.ai.client;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiClientConfig.StorageType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAiClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AiClientConfig.class)
                .setStorageType(StorageType.NONE)
                .setClientCacheRefreshInterval(new Duration(1, TimeUnit.SECONDS))
                .setClientCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setClientCacheRefreshEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ai.client.models.storage", "FILE")
                .put("ai.client.cache.refresh.interval", "5s")
                .put("ai.client.cache.ttl", "30m")
                .put("ai.client.cache.refresh.enabled", "true")
                .buildOrThrow();
        AiClientConfig expected = new AiClientConfig()
                .setStorageType(StorageType.FILE)
                .setClientCacheRefreshInterval(new Duration(5, TimeUnit.SECONDS))
                .setClientCacheTtl(new Duration(30, TimeUnit.MINUTES))
                .setClientCacheRefreshEnabled(true);

        assertFullMapping(properties, expected);
    }
}
