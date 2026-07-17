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
package io.trino.server.starburst;

import io.airlift.configuration.Config;

public class GalaxyEnabledConfig
{
    private boolean galaxyEnabled;
    private boolean galaxyRbacEnabled = true;
    private boolean galaxyCorsEnabled = true;
    private boolean galaxyHeartbeatEnabled = true;
    private boolean galaxyOperatorAuthenticationEnabled = true;
    private boolean galaxyTransactionsDisabled;
    private boolean useSharedPermissionsCache;

    public boolean isGalaxyEnabled()
    {
        return galaxyEnabled;
    }

    @Config("galaxy.enabled")
    public GalaxyEnabledConfig setGalaxyEnabled(boolean galaxyEnabled)
    {
        this.galaxyEnabled = galaxyEnabled;
        return this;
    }

    public boolean isGalaxyRbacEnabled()
    {
        return galaxyEnabled && galaxyRbacEnabled;
    }

    @Config("galaxy.rbac.enabled")
    public GalaxyEnabledConfig setGalaxyRbacEnabled(boolean galaxyRbacEnabled)
    {
        this.galaxyRbacEnabled = galaxyRbacEnabled;
        return this;
    }

    public boolean isGalaxyCorsEnabled()
    {
        return galaxyEnabled && galaxyCorsEnabled;
    }

    @Config("galaxy.cors.enabled")
    public GalaxyEnabledConfig setGalaxyCorsEnabled(boolean galaxyCorsEnabled)
    {
        this.galaxyCorsEnabled = galaxyCorsEnabled;
        return this;
    }

    public boolean isGalaxyHeartbeatEnabled()
    {
        return galaxyEnabled && galaxyHeartbeatEnabled;
    }

    @Config("galaxy.heartbeat.enabled")
    public GalaxyEnabledConfig setGalaxyHeartbeatEnabled(boolean galaxyHeartbeatEnabled)
    {
        this.galaxyHeartbeatEnabled = galaxyHeartbeatEnabled;
        return this;
    }

    public boolean isGalaxyOperatorAuthenticationEnabled()
    {
        return galaxyEnabled && galaxyOperatorAuthenticationEnabled;
    }

    @Config("galaxy.operator.enabled")
    public GalaxyEnabledConfig setGalaxyOperatorAuthenticationEnabled(boolean galaxyOperatorAuthenticationEnabled)
    {
        this.galaxyOperatorAuthenticationEnabled = galaxyOperatorAuthenticationEnabled;
        return this;
    }

    public boolean isGalaxyTransactionsDisabled()
    {
        return galaxyEnabled && galaxyTransactionsDisabled;
    }

    @Config("galaxy.transaction.disabled")
    public GalaxyEnabledConfig setGalaxyTransactionsDisabled(boolean galaxyTransactionsDisabled)
    {
        this.galaxyTransactionsDisabled = galaxyTransactionsDisabled;
        return this;
    }

    public boolean isUseSharedPermissionsCache()
    {
        return useSharedPermissionsCache;
    }

    @Config("galaxy.use-shared-permissions-cache")
    public GalaxyEnabledConfig setUseSharedPermissionsCache(boolean useSharedPermissionsCache)
    {
        this.useSharedPermissionsCache = useSharedPermissionsCache;
        return this;
    }
}
