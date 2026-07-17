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
package io.trino.server.starburst.accesscontrol;

import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;

import java.util.Optional;

/**
 * @see StacIsLiveTableCache
 * @see DisabledLiveTableCache
 */
public interface IsIcehouseLiveTableCache
{
    /**
     * If the table is not a live table, return Optional.empty()
     * If it is stopped, return Optional.of(true)
     * If it is not stopped, return Optional.of(false)
     */
    Optional<Boolean> isLiveTableStopped(TrinoSecurityApi trinoSecurityApi, TableId tableId, DispatchSession session);

    static IsIcehouseLiveTableCache create(GalaxySystemAccessControlConfig.AccessControlMode accessControlMode)
    {
        return switch (accessControlMode) {
            case GALAXY -> new StacIsLiveTableCache();
            case SEP -> new DisabledLiveTableCache();
        };
    }
}
