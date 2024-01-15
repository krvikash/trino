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
package io.trino.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MaxSplitsPerTableConfig
{
    private String queryMaxSplitsPerTableConfigFilePath;
    private Duration refreshPeriod = new Duration(60, SECONDS);

    @Config("query.max-splits-per-table.config-file")
    @ConfigDescription("JSON configuration file containing limits for split count per table")
    public MaxSplitsPerTableConfig setQueryMaxSplitsPerTableConfigFilePath(String queryMaxSplitsPerTableConfigFilePath)
    {
        this.queryMaxSplitsPerTableConfigFilePath = queryMaxSplitsPerTableConfigFilePath;
        return this;
    }

    @FileExists
    public String getQueryMaxSplitsPerTableConfigFilePath()
    {
        return this.queryMaxSplitsPerTableConfigFilePath;
    }

    @Config("query.max-splits-per-table.refresh-period")
    @ConfigDescription("How often to refresh the max splits per table configuration")
    public MaxSplitsPerTableConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    @MinDuration("60s")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }
}
