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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Singleton
public class CacheMgrWarmupRuleService
        implements WarpInitializedServiceMarker
{
    private final Map<String, CacheManagerRule> cache;

    @Inject
    public CacheMgrWarmupRuleService(WarpInitializedServiceRegistry warpInitializedServiceRegistry)
    {
        cache = new ConcurrentHashMap<>();

        warpInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init() {}

    public synchronized void replaceAll(List<CacheManagerRule> newWarmupRules)
            throws TrinoException
    {
        try {
            cache.clear();
            cache.putAll(newWarmupRules.stream().collect(Collectors.toMap(CacheManagerRule::signatureKey, Function.identity())));
        }
        catch (Exception e) {
            throw new TrinoException(
                    WarpErrorCode.WARP_RULE_CONFIGURATION_ERROR,
                    "failed to replace existing rules with new rules=%s".formatted(newWarmupRules),
                    e);
        }
    }

    public List<CacheManagerRule> getAll()
    {
        return ImmutableList.copyOf(cache.values());
    }
}
