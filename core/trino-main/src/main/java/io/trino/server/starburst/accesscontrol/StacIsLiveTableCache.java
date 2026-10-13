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
import io.starburst.stargate.accesscontrol.client.LiveTableInfo;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.cache.NonEvictableCache;

import java.time.Duration;
import java.util.Optional;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;

public class StacIsLiveTableCache
        implements IsIcehouseLiveTableCache
{
    private final NonEvictableCache<TableId, Optional<LiveTableInfo>> liveTableInfoCache = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(Duration.ofHours(4)));

    @Override
    public Optional<LiveTableInfo> getLiveTableInfo(TrinoSecurityApi trinoSecurityApi, TableId tableId, DispatchSession session)
    {
        Optional<LiveTableInfo> cached = liveTableInfoCache.getIfPresent(tableId);
        if (cached != null) {
            return cached;
        }
        Optional<LiveTableInfo> liveTableInfo = trinoSecurityApi.getLiveTableInfo(session, tableId);
        liveTableInfoCache.put(tableId, liveTableInfo);
        return liveTableInfo;
    }
}
