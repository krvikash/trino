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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CacheMetrics
{
    /**
     * Counts number of splits not cached due to excessive split data size.
     */
    private final AtomicInteger tooBigSplitCount = new AtomicInteger();
    private final AtomicInteger splitNotCachedCount = new AtomicInteger();
    private final AtomicInteger splitCachedCount = new AtomicInteger();
    private final AtomicLong sourceBytes = new AtomicLong();
    private final AtomicLong inputCacheBytes = new AtomicLong();

    public int getTooBigSplitCount()
    {
        return tooBigSplitCount.get();
    }

    public int getSplitNotCachedCount()
    {
        return splitNotCachedCount.get();
    }

    public int getSplitCachedCount()
    {
        return splitCachedCount.get();
    }

    public long getSourceBytes()
    {
        return sourceBytes.get();
    }

    public long getInputCacheBytes()
    {
        return inputCacheBytes.get();
    }

    public void incrementTooBigSplitCount()
    {
        tooBigSplitCount.incrementAndGet();
    }

    public void incrementSplitsNotCached()
    {
        splitNotCachedCount.incrementAndGet();
    }

    public void incrementSplitsCached()
    {
        splitCachedCount.incrementAndGet();
    }

    public void addSourceBytes(long bytes)
    {
        sourceBytes.addAndGet(bytes);
    }

    public void addInputCacheBytes(long bytes)
    {
        inputCacheBytes.addAndGet(bytes);
    }
}
