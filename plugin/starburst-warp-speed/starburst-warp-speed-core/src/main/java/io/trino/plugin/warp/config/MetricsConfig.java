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
import io.airlift.configuration.DefunctConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@DefunctConfig({
        "warp-speed.metrics.max-limits",
        "warp-speed.metrics.cluster.interval",
        "warp-speed.metrics.cleaner.keep",
})
public class MetricsConfig
{
    private final Map<String, Long> limits;
    private boolean enabled = true;

    private Duration delayDuration = Duration.ofSeconds(10);
    private Duration intervalCleanerDuration = Duration.ofSeconds(45);
    private Duration printMetricsDuration = Duration.ofMinutes(15);

    public MetricsConfig()
    {
        limits = new HashMap<>();
        limits.put("column", 16384L);
        limits.put("device", 1024L);
    }

    public long getLimit(String type)
    {
        return limits.getOrDefault(type, 1024L);
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("warp-speed.metrics.enabled")
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public Duration getDelayDuration()
    {
        return delayDuration;
    }

    @Config("warp-speed.metrics.delay")
    public void setDelayDuration(Duration delayDuration)
    {
        this.delayDuration = delayDuration;
    }

    public Duration getIntervalCleanerDuration()
    {
        return intervalCleanerDuration;
    }

    @Config("warp-speed.metrics.cleaner.interval")
    public void setIntervalCleanerDuration(Duration intervalCleanerDuration)
    {
        this.intervalCleanerDuration = intervalCleanerDuration;
    }

    public Duration getPrintMetricsDuration()
    {
        return printMetricsDuration;
    }

    @Config("warp-speed.metrics.dump.interval")
    public void setPrintMetricsDuration(io.airlift.units.Duration printMetricsDuration)
    {
        this.printMetricsDuration = printMetricsDuration.toJavaTime();
    }

    @Override
    public String toString()
    {
        return "MetricsConfig{" +
                "limits=" + limits +
                ", enabled=" + enabled +
                ", delayDuration=" + delayDuration +
                ", intervalCleanerDuration=" + intervalCleanerDuration +
                '}';
    }
}
