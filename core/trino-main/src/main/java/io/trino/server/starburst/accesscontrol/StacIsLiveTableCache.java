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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;

import java.util.Optional;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;

public class StacIsLiveTableCache
        implements IsIcehouseLiveTableCache
{
    /**
     * Optional.empty() means that the cache does not have this table yet, which prompts GalaxyPermissionsCache.isLiveTableStopped to call trinoSecurityApi.isLiveTableStopped and fill in the cache value.
     * Optional.of(true) means that the table is a live table, which means that GalaxyPermissionsCache.isLiveTableStopped will have to call trinoSecurityApi.isLiveTableStopped to get the latest state.
     * Optional.of(false) means that the table is not a live table. GalaxyPermissionsCache.isLiveTableStopped will return Optional.empty().
     */
    private final LoadingCache<TableId, Optional<Boolean>> isLiveTableCache;

    public StacIsLiveTableCache()
    {
        isLiveTableCache = buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(_ -> Optional.empty()));
    }

    @Override
    public Optional<Boolean> isLiveTableStopped(TrinoSecurityApi trinoSecurityApi, TableId tableId, DispatchSession session)
    {
        Optional<Boolean> isLiveTable = isLiveTableCache.getUnchecked(tableId);
        if (isLiveTable.isPresent() && !isLiveTable.get()) {
            return Optional.empty();
        }
        Optional<Boolean> isLiveTableStopped = trinoSecurityApi.isLiveTableStopped(session, tableId);
        isLiveTableCache.put(tableId, Optional.of(isLiveTableStopped.isPresent()));
        return isLiveTableStopped;
    }
}
