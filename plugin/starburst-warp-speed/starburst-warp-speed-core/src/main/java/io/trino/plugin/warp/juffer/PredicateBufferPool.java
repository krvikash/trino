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
package io.trino.plugin.warp.juffer;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.WarpErrorCode.WARP_PREDICATE_BUFFER_ALLOCATION;

class PredicateBufferPool
{
    private static final Logger logger = Logger.get(PredicateBufferPool.class);
    private final PredicateBufferPoolType poolType;
    private final int poolSize;
    private final int bufSize;
    private final ArrayBlockingQueue<MemorySegment> queue;

    PredicateBufferPool(PredicateBufferPoolType poolType, long bufSize, long poolSize, SegmentAllocator poolSlicer)
    {
        checkArgument(poolType != PredicateBufferPoolType.INVALID, "invalid predicate pool type");
        this.poolType = poolType;
        checkArgument(bufSize > 0, "predicate pool buffer size is zero");
        this.bufSize = (int) bufSize;
        checkArgument(poolSize > 0, "predicate pool size is zero");
        this.poolSize = (int) poolSize;

        ArrayList<MemorySegment> buffList;
        MemorySegment allBuffs = poolSlicer.allocate(poolSize * bufSize);
        SegmentAllocator buffSlicer = SegmentAllocator.slicingAllocator(allBuffs);
        buffList = new ArrayList<>(this.poolSize);
        for (int i = 0; i < this.poolSize; i++) {
            buffList.add(buffSlicer.allocate(bufSize));
        }

        this.queue = new ArrayBlockingQueue<>(buffList.size(), true, buffList);
        logger.info("predicate buffer pool %s", this);
    }

    public boolean canHandle(int size)
    {
        return size <= bufSize;
    }

    // returns buffer if allocation succeeds or null if failed
    MemorySegment alloc(int size)
    {
        if (!canHandle(size)) {
            return null;
        }
        return queue.poll();
    }

    void free(MemorySegment buff)
    {
        if (!queue.offer(buff)) {
            throw new TrinoException(WARP_PREDICATE_BUFFER_ALLOCATION, String.format("failed to release predicate buffer to queue %s", this));
        }
    }

    int getBufSize()
    {
        return bufSize;
    }

    int getPoolSize()
    {
        return poolSize;
    }

    PredicateBufferPoolType getPredicateBufferPoolType()
    {
        return poolType;
    }

    void clear()
    {
        queue.clear();
    }

    @Override
    public String toString()
    {
        return "PredicateBufferPool{" +
                "queue.size=" + queue.size() +
                ", poolType=" + poolType +
                ", poolSize=" + poolSize +
                ", bufSize=" + bufSize +
                '}';
    }
}
