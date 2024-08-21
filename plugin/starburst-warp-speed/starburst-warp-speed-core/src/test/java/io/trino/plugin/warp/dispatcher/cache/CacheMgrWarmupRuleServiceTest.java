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

import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheMgrWarmupRuleServiceTest
{
    @Test
    public void test()
    {
        CacheMgrWarmupRuleService service = new CacheMgrWarmupRuleService(new WarpInitializedServiceRegistry());
        assertThat(service.getAll()).isEmpty();

        List<CacheManagerRule> cacheManagerRules1 = List.of(new CacheManagerRule("key1", 1L, Duration.ofMillis(1L)));
        service.replaceAll(cacheManagerRules1);
        assertThat(service.getAll()).isEqualTo(cacheManagerRules1);

        List<CacheManagerRule> cacheManagerRules2 = List.of(new CacheManagerRule("key2", 1L, Duration.ofMillis(1L)));
        service.replaceAll(cacheManagerRules2);
        assertThat(service.getAll()).isEqualTo(cacheManagerRules2);
    }
}
