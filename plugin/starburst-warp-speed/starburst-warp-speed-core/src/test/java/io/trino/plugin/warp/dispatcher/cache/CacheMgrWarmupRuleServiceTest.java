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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheMgrWarmupRuleServiceTest
{
    @Test
    public void testEnabled()
    {
        CacheManagerConfig cacheManagerConfig = new CacheManagerConfig();
        cacheManagerConfig.setRulesEnabled(true);
        CacheMgrWarmupRuleService service = new CacheMgrWarmupRuleService(cacheManagerConfig);
        assertThat(service.getAll()).isEmpty();

        CacheManagerRule cacheManagerRule1 = new CacheManagerRule("key1", 1L, Duration.ofMillis(1L));
        service.replaceAll(List.of(cacheManagerRule1));
        assertThat(service.getAll())
                .isEqualTo(
                        Map.of(
                                service.hash(cacheManagerRule1.signatureKey()), new CacheManagerRule(service.hash(
                                        cacheManagerRule1.signatureKey()),
                                        cacheManagerRule1.priority(),
                                        cacheManagerRule1.ttl())));

        CacheManagerRule cacheManagerRule2 = new CacheManagerRule("key2", 1L, Duration.ofMillis(1L));
        service.replaceAll(List.of(cacheManagerRule2));
        assertThat(service.getAll())
                .isEqualTo(
                        Map.of(
                                service.hash(cacheManagerRule2.signatureKey()), new CacheManagerRule(
                                        service.hash(cacheManagerRule2.signatureKey()),
                                        cacheManagerRule1.priority(),
                                        cacheManagerRule1.ttl())));
    }

    @Test
    public void testDisabled()
    {
        CacheManagerConfig cacheManagerConfig = new CacheManagerConfig();
        cacheManagerConfig.setRulesEnabled(false);

        CacheMgrWarmupRuleService service = new CacheMgrWarmupRuleService(cacheManagerConfig);
        assertThat(service.getAll()).isEmpty();

        CacheManagerRule cacheManagerRule1 = new CacheManagerRule("key1", 1L, Duration.ofMillis(1L));
        service.replaceAll(List.of(cacheManagerRule1));
        assertThat(service.getAll()).isEmpty();

        CacheManagerRule cacheManagerRule2 = new CacheManagerRule("key2", 1L, Duration.ofMillis(1L));
        service.replaceAll(List.of(cacheManagerRule2));
        assertThat(service.getAll()).isEmpty();
    }
}
