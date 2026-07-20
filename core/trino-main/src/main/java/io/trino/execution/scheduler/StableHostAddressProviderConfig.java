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
package io.trino.execution.scheduler;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

public class StableHostAddressProviderConfig
{
    // Upstream defaults to 2; warp-speed warms each split's data on its affinity host, so a
    // single preferred host keeps warm data on exactly one worker instead of duplicating it.
    private int preferredHostsCount = 1;

    @Min(1)
    public int getPreferredHostsCount()
    {
        return preferredHostsCount;
    }

    @Config("node-scheduler.cache-preferred-hosts-count")
    @ConfigDescription("Number of preferred worker hosts the scheduler derives from a split's cache key")
    public StableHostAddressProviderConfig setPreferredHostsCount(int preferredHostsCount)
    {
        this.preferredHostsCount = preferredHostsCount;
        return this;
    }
}
