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
package io.trino.cache;

import com.google.inject.Inject;
import io.trino.spi.subquery.cache.CacheSplitId;
import io.trino.spi.subquery.cache.PlanSignature;

import java.util.LinkedHashMap;

public class CachePerformanceTracker
{
    private static final int MAX_SIZE = 150_000;

    private final LinkedHashMap<Long, Long> cpuNanosMap;

    @Inject
    public CachePerformanceTracker()
    {
        // Use LinkedHashMap with accessOrder=true to maintain access order
        this.cpuNanosMap = new LinkedHashMap<>(64, 0.75f, true);
    }

    public synchronized Long get(CacheSplitId splitId, PlanSignature planSignature)
    {
        return cpuNanosMap.get(getKey(splitId, planSignature));
    }

    public synchronized void put(CacheSplitId splitId, PlanSignature planSignature, long cpuNanos)
    {
        if (cpuNanosMap.size() >= MAX_SIZE) {
            // Remove the oldest entry (the first entry in the map)
            cpuNanosMap.pollFirstEntry();
        }
        cpuNanosMap.put(getKey(splitId, planSignature), cpuNanos);
    }

    public synchronized void remove(CacheSplitId splitId, PlanSignature planSignature)
    {
        cpuNanosMap.remove(getKey(splitId, planSignature));
    }

    private long getKey(CacheSplitId splitId, PlanSignature planSignature)
    {
        return (((long) splitId.hashCode()) << Integer.SIZE) + planSignature.hashCode();
    }
}
