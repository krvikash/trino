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
package io.trino.plugin.warp.config;

import io.airlift.configuration.Config;

public class CacheManagerConfig
{
    public static final String CONFIG_IS_CACHE = "warp-speed.config.is-cache";
    public static final String CACHE_MANAGER_RULES_ENABLED = "warp-speed.cache-manager.rules.enabled";

    private boolean isCache;
    private boolean isRulesEnabled;

    public boolean getIsCache()
    {
        return isCache;
    }

    @Config(CONFIG_IS_CACHE)
    public void setIsCache(boolean isCache)
    {
        this.isCache = isCache;
    }

    public boolean isRulesEnabled()
    {
        return isRulesEnabled;
    }

    @Config(CACHE_MANAGER_RULES_ENABLED)
    public void setRulesEnabled(boolean rulesEnabled)
    {
        isRulesEnabled = rulesEnabled;
    }
}
