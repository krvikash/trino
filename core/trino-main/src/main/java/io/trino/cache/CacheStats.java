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

import com.google.errorprone.annotations.MustBeClosed;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import io.airlift.stats.TimeStat.BlockTimer;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CacheStats
{
    private final CounterStat cacheHits = new CounterStat();
    private final CounterStat cacheMiss = new CounterStat();
    private final CounterStat splitRejected = new CounterStat();
    private final CounterStat splitFailoverHappened = new CounterStat();
    private final CounterStat missingSplitId = new CounterStat();
    private final CounterStat predicateTooBig = new CounterStat();
    private final CounterStat tooBigSplit = new CounterStat();
    private final CounterStat dynamicFilterWithBloomFilter = new CounterStat();
    private final DistributionStat readFromCacheData = new DistributionStat();
    private final DistributionStat cachedData = new DistributionStat();
    private final TimeStat revokeMemoryTime = new TimeStat();
    private final TimeStat cacheLookupTime = new TimeStat();
    private final CounterStat sparedCpuTime = new CounterStat();
    private final CounterStat missingStatForSparedCpuTime = new CounterStat();

    @Managed
    @Nested
    public CounterStat getCacheHits()
    {
        return cacheHits;
    }

    @Managed
    @Nested
    public CounterStat getCacheMiss()
    {
        return cacheMiss;
    }

    @Managed
    @Nested
    public CounterStat getSplitRejected()
    {
        return splitRejected;
    }

    @Managed
    @Nested
    public CounterStat getSplitFailoverHappened()
    {
        return splitFailoverHappened;
    }

    @Managed
    @Nested
    public CounterStat getMissingSplitId()
    {
        return missingSplitId;
    }

    @Managed
    @Nested
    public CounterStat getPredicateTooBig()
    {
        return predicateTooBig;
    }

    @Managed
    @Nested
    public CounterStat getTooBigSplit()
    {
        return tooBigSplit;
    }

    @Managed
    @Nested
    public DistributionStat getReadFromCacheData()
    {
        return readFromCacheData;
    }

    @Managed
    @Nested
    public DistributionStat getCachedData()
    {
        return cachedData;
    }

    @Managed
    @Nested
    public TimeStat getRevokeMemoryTime()
    {
        return revokeMemoryTime;
    }

    @Managed
    @Nested
    public TimeStat getCacheLookupTime()
    {
        return cacheLookupTime;
    }

    @Managed
    @Nested
    public CounterStat getSparedCpuTime()
    {
        return sparedCpuTime;
    }

    @Managed
    @Nested
    public CounterStat getMissingStatForSparedCpuTime()
    {
        return missingStatForSparedCpuTime;
    }

    public void recordCacheMiss()
    {
        cacheMiss.update(1);
    }

    public void recordCacheHit()
    {
        cacheHits.update(1);
    }

    public void recordSplitRejected()
    {
        splitRejected.update(1);
    }

    public void recordSplitFailoverHappened()
    {
        splitFailoverHappened.update(1);
    }

    public void recordMissingSplitId()
    {
        missingSplitId.update(1);
    }

    public void recordPredicateTooBig()
    {
        predicateTooBig.update(1);
    }

    public void recordDynamicFilterWithBloomFilter()
    {
        dynamicFilterWithBloomFilter.update(1);
    }

    public void recordTooBigSplit()
    {
        tooBigSplit.update(1);
    }

    public void recordReadFromCacheData(long bytes)
    {
        readFromCacheData.add(bytes);
    }

    public void recordCacheData(long bytes)
    {
        cachedData.add(bytes);
    }

    @MustBeClosed
    public BlockTimer recordRevokeMemoryTime()
    {
        return revokeMemoryTime.time();
    }

    public void safeUpdateSparedCpuTime(long cpuNanos)
    {
        if (cpuNanos < 0) {
            synchronized (sparedCpuTime) { // protect 2 parallel reduces
                long update = Math.max(-1 * sparedCpuTime.getTotalCount(), cpuNanos);
                sparedCpuTime.update(update);
            }
        }
        else {
            sparedCpuTime.update(cpuNanos);
        }
    }
}
