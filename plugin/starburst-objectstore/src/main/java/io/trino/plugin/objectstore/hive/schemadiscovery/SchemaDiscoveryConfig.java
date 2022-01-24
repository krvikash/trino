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
package io.trino.plugin.objectstore.hive.schemadiscovery;

import com.amazonaws.ClientConfiguration;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class SchemaDiscoveryConfig
{
    private int executorThreadCount = Math.min(100, ClientConfiguration.DEFAULT_MAX_CONNECTIONS);

    @Min(1)
    @Max(ClientConfiguration.DEFAULT_MAX_CONNECTIONS)
    public int getExecutorThreadCount()
    {
        return executorThreadCount;
    }

    @Config("hive.schema-discovery.threads")
    @ConfigDescription("Number of threads for schema discovery executor, shouldn't be more than AWS http client's default max connection, as it might lead to thread-pool bottleneck")
    public SchemaDiscoveryConfig setExecutorThreadCount(int executorThreadCount)
    {
        this.executorThreadCount = executorThreadCount;
        return this;
    }
}
