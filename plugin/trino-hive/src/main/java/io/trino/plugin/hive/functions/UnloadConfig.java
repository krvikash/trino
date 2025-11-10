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
package io.trino.plugin.hive.functions;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

public class UnloadConfig
{
    private boolean useRowSemantics;

    @Config("hive.unload.experimental-use-row-semantics")
    @LegacyConfig("hive.unload.use-row-semantics")
    @ConfigDescription("Forces unload function to use row semantics. PARTITION BY and ORDER BY clauses are not supported")
    public UnloadConfig setUseRowSemantics(boolean useRowSemantics)
    {
        this.useRowSemantics = useRowSemantics;
        return this;
    }

    public boolean isUseRowSemantics()
    {
        return useRowSemantics;
    }
}
