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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.spi.TrinoException;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class CacheMgrWarmupRuleService
{
    private Map<String, CacheManagerRule> cache = Map.of();

    private final CacheManagerConfig cacheManagerConfig;
    private final HashFunction hashFunction;

    @Inject
    public CacheMgrWarmupRuleService(CacheManagerConfig cacheManagerConfig)
    {
        this.cacheManagerConfig = requireNonNull(cacheManagerConfig);
        hashFunction = Hashing.farmHashFingerprint64();
    }

    public synchronized void replaceAll(List<CacheManagerRule> newWarmupRules)
            throws TrinoException
    {
        if (newWarmupRules.isEmpty()) {
            cache = Map.of();
        }
        else {
            try {
                cache = newWarmupRules.stream()
                        .map(cacheManagerRule -> new CacheManagerRule(
                                hash(cacheManagerRule.signatureKey()),
                                cacheManagerRule.priority(),
                                cacheManagerRule.ttl()))
                        .collect(Collectors.toMap(CacheManagerRule::signatureKey, Function.identity()));
            }
            catch (Exception e) {
                throw new TrinoException(
                        WarpErrorCode.WARP_RULE_CONFIGURATION_ERROR,
                        "failed to replace existing rules with new rules=%s".formatted(newWarmupRules),
                        e);
            }
        }
    }

    public Map<String, CacheManagerRule> getAll()
    {
        if (cacheManagerConfig.isRulesEnabled()) {
            return cache;
        }
        return Map.of();
    }

    public final String hash(String signatureKey)
    {
        return hashFunction.hashString(signatureKey, StandardCharsets.UTF_8).toString();
    }
}
