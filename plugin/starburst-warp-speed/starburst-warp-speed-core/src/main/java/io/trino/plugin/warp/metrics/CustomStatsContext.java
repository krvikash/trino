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
package io.trino.plugin.warp.metrics;

import java.util.HashMap;
import java.util.Map;

public class CustomStatsContext
{
    private final Map<String, WarpStatsBase> registeredStats = new HashMap<>();
    private final Map<String, Long> fixedStats = new HashMap<>();
    private final MetricsManager metricsManager;

    public CustomStatsContext(MetricsManager metricsManager)
    {
        this.metricsManager = metricsManager;
    }

    public WarpStatsBase getStat(String statKey)
    {
        return registeredStats.get(statKey);
    }

    public WarpStatsBase getOrRegister(WarpStatsBase warpStatsBase)
    {
        if (registeredStats.containsKey(warpStatsBase.getJmxKey())) {
            return registeredStats.get(warpStatsBase.getJmxKey());
        }
        registeredStats.put(warpStatsBase.getJmxKey(), warpStatsBase);
        return warpStatsBase;
    }

    public Map<String, WarpStatsBase> getRegisteredStats()
    {
        return new HashMap<>(registeredStats);
    }

    public void addFixedStat(String key, long value)
    {
        fixedStats.put(key, value);
    }

    public Map<String, Long> getFixedStats()
    {
        return fixedStats;
    }

    public void copyStatsToGlobalMetricsManager()
    {
        registeredStats.forEach((key, value) -> {
            WarpStatsBase warpStatsBase = metricsManager.get(key);
            if (warpStatsBase != null) {
                warpStatsBase.mergeStats(value);
            }
        });
    }
}
