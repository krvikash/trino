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
package io.trino.plugin.warp.extension.config;

import io.airlift.configuration.Config;

public class CallHomeConfig
{
    private boolean enable = true;

    private int intervalInSeconds = 3600;
    private int maxWaitTimeInSeconds = 120;

    public int getIntervalInSeconds()
    {
        return intervalInSeconds;
    }

    @Config("warp-speed.call-home.interval.seconds")
    public void setIntervalInSeconds(int intervalInSeconds)
    {
        this.intervalInSeconds = intervalInSeconds;
    }

    public boolean isEnable()
    {
        return enable;
    }

    @Config("warp-speed.call-home.enable")
    public void setEnable(boolean enable)
    {
        this.enable = enable;
    }

    public int getMaxWaitTimeInSeconds()
    {
        return maxWaitTimeInSeconds;
    }

    @Config("warp-speed.call-home.maxWaitTimeInSeconds")
    public void setMaxWaitTimeInSeconds(int maxWaitTimeInSeconds)
    {
        this.maxWaitTimeInSeconds = maxWaitTimeInSeconds;
    }

    @Override
    public String toString()
    {
        return "CallHomeConfig{" +
                "enable=" + enable +
                ", intervalInSeconds=" + intervalInSeconds +
                ", maxWaitTimeInSeconds=" + maxWaitTimeInSeconds +
                '}';
    }
}
