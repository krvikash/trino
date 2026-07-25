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
import java.util.Objects;

@DefunctConfig("warp-speed.enable.fs-cache-mode")
public class SharedConfig
{
    public static final String CONFIG_IS_SINGLE = "warp-speed.config.is-single";
    public static final String FAILURE_GENERATOR_ENABLED = "warp-speed.config.failure-generator-enabled";

    private boolean isSingle;
    private boolean enableMatchCollect = true;
    private boolean enableMappedMatchCollect = true;
    private boolean enableVarcharMappedMatchCollect = true;

    private int shapingLoggerThreshold = 1000;
    private Duration shapingLoggerDuration = Duration.ofSeconds(60);
    private int shapingLoggerNumberOfSamples = 3;

    private boolean debugWarming;
    private boolean debugFailureGenerator;

    public boolean getIsSingle()
    {
        return isSingle;
    }

    @Config(CONFIG_IS_SINGLE)
    public void setIsSingle(boolean isSingle)
    {
        this.isSingle = isSingle;
    }

    public boolean getEnableMatchCollect()
    {
        return enableMatchCollect;
    }

    @Config("warp-speed.enable.match-collect")
    public void setEnableMatchCollect(boolean enableMatchCollect)
    {
        this.enableMatchCollect = enableMatchCollect;
    }

    public boolean getEnableMappedMatchCollect()
    {
        return enableMappedMatchCollect;
    }

    @Config("warp-speed.enable.mapped-match-collect")
    public void setEnableMappedMatchCollect(boolean enableMappedMatchCollect)
    {
        this.enableMappedMatchCollect = enableMappedMatchCollect;
    }

    public boolean getEnableVarcharMappedMatchCollect()
    {
        return enableVarcharMappedMatchCollect;
    }

    @Config("warp-speed.enable.varchar-mapped-match-collect")
    public void setEnableVarcharMappedMatchCollect(boolean enableVarcharMappedMatchCollect)
    {
        this.enableVarcharMappedMatchCollect = enableVarcharMappedMatchCollect;
    }

    public int getShapingLoggerThreshold()
    {
        return shapingLoggerThreshold;
    }

    @Config("warp-speed.shaping-logger.threshold")
    public void setShapingLoggerThreshold(int threshold)
    {
        this.shapingLoggerThreshold = threshold;
    }

    public Duration getShapingLoggerDuration()
    {
        return shapingLoggerDuration;
    }

    @Config("warp-speed.shaping-logger.duration")
    public void setShapingLoggerDuration(io.airlift.units.Duration duration)
    {
        this.shapingLoggerDuration = duration.toJavaTime();
    }

    public int getShapingLoggerNumberOfSamples()
    {
        return shapingLoggerNumberOfSamples;
    }

    @Config("warp-speed.shaping-logger-num-samples")
    public void setShapingLoggerNumberOfSamples(int shapingLoggerNumberOfSamples)
    {
        this.shapingLoggerNumberOfSamples = shapingLoggerNumberOfSamples;
    }

    public boolean getDebugWarming()
    {
        return debugWarming;
    }

    @Config("warp-speed.debug.warming")
    public void setDebugWarming(boolean debugWarming)
    {
        this.debugWarming = debugWarming;
    }

    public boolean isFailureGeneratorEnabled()
    {
        return debugFailureGenerator;
    }

    @Config(FAILURE_GENERATOR_ENABLED)
    public void setFailureGeneratorEnabled(boolean debugFailureGenerator)
    {
        this.debugFailureGenerator = debugFailureGenerator;
    }

    @Override
    public boolean equals(Object object)
    {
        if ((object == null) || (getClass() != object.getClass())) {
            return false;
        }
        SharedConfig that = (SharedConfig) object;
        return (isSingle == that.isSingle) &&
                (enableMatchCollect == that.enableMatchCollect) &&
                (enableMappedMatchCollect == that.enableMappedMatchCollect) &&
                (enableVarcharMappedMatchCollect == that.enableVarcharMappedMatchCollect) &&
                (shapingLoggerThreshold == that.shapingLoggerThreshold) &&
                (shapingLoggerNumberOfSamples == that.shapingLoggerNumberOfSamples) &&
                Objects.equals(shapingLoggerDuration, that.shapingLoggerDuration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                isSingle,
                enableMatchCollect,
                enableMappedMatchCollect,
                enableVarcharMappedMatchCollect,
                shapingLoggerThreshold,
                shapingLoggerDuration,
                shapingLoggerNumberOfSamples);
    }

    @Override
    public String toString()
    {
        return "SharedConfig{" +
                "isSingle=" + isSingle +
                ", enableMatchCollect=" + enableMatchCollect +
                ", shapingLoggerThreshold=" + shapingLoggerThreshold +
                ", shapingLoggerDuration=" + shapingLoggerDuration +
                ", shapingLoggerNumberOfSamples=" + shapingLoggerNumberOfSamples +
                ", debugWarming=" + debugWarming +
                ", debugFailureGenerator=" + debugFailureGenerator +
                '}';
    }
}
