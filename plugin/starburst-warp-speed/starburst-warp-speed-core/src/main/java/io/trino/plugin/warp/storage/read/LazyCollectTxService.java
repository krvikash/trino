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
package io.trino.plugin.warp.storage.read;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.constants.CollectStats;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.QueryMemory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

public class LazyCollectTxService
        extends BaseCollectTxService
{
    private static final Logger logger = Logger.get(LazyCollectorLoader.class);
    private final NativeConfig nativeConfig;

    @Inject
    public LazyCollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ConnectorSync connectorSync,
            BufferAllocator bufferAllocator,
            GlobalConfig globalConfig,
            NativeConfig nativeConfig)
    {
        super(storageEngine, storageEngineConstants, connectorSync, bufferAllocator, globalConfig);
        this.nativeConfig = nativeConfig;
    }

    LazyCollectOpenResult collectOpen(int rowsLimit, LazyCollectorLoaderArgs lazyCollectorLoaderArgs, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        QueryMemory queryMemory = allocQueryMemory();
        SegmentAllocator queryMemoryAllocator = getQueryMemoryAllocator(queryMemory);
        int queryMemoryId = queryMemory.id();
        WarmupElementCollectParams collectParams = lazyCollectorLoaderArgs.collectParams();

        long[] collectBuffers = lazyCollectorLoaderArgs.txArgs().collectBuffers()[0];
        MemorySegment[] collectSegments = new MemorySegment[collectBuffers.length];
        allocCollectBuffer(queryMemoryAllocator, JbufType.JBUF_TYPE_REC, nativeConfig.getMaxRecJufferSize(), collectSegments, collectBuffers);
        allocCollectBuffer(queryMemoryAllocator, JbufType.JBUF_TYPE_NULL, bufferAllocator.getQueryNullBufferSize(RecTypeCode.REC_TYPE_VARCHAR), collectSegments, collectBuffers);
        lazyCollectorLoaderArgs.collectJufferWE().createBuffers(collectParams.getRecTypeCode(), collectParams.getRecTypeLength(), collectParams.hasDictionary(), collectSegments);

        RecordIndexes recordIndexes = lazyCollectorLoaderArgs.recordIndexes();
        recordIndexes.setMemory(lazyCollectorLoaderArgs.queryParams().getArena());
        collectOpen(lazyCollectorLoaderArgs.queryParams(),
                lazyCollectorLoaderArgs.txArgs(),
                queryMemoryId,
                1,
                lazyCollectorLoaderArgs.numChunksInRange(),
                -1, // invalid reopen chunk index
                lazyCollectorLoaderArgs.warmUpElementAtt().address(),
                lazyCollectorLoaderArgs.recordBufferStates().address(),
                recordIndexes.getAddress(),
                0,
                dispatcherPageSourceStats);

        logger.debug("collectOpen queryMemoryId %d rowsLimit %d", queryMemoryId, rowsLimit);
        return new LazyCollectOpenResult(queryMemoryId);
    }

    // Lazy collect doesn't use store/restore mechanism, so store/restore params are not initialized
    void collectClose(int queryMemoryId, NativeStats nativeStats, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        long[] collectStats = new long[CollectStats.COLLECT_STATS_NUM_OF.ordinal()];
        long startTime = System.nanoTime();
        storageEngine.collectClose(queryMemoryId, collectStats);
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);

        nativeStats.addread_cache_md_chunk_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_HITS.ordinal()]);
        nativeStats.addread_cache_md_basic_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_HITS.ordinal()]);
        nativeStats.addread_cache_md_data_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_HITS.ordinal()]);
        nativeStats.addread_cache_md_nulls_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_HITS.ordinal()]);
        nativeStats.addread_cache_md_chunk_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_MISSES.ordinal()]);
        nativeStats.addread_cache_md_basic_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_MISSES.ordinal()]);
        nativeStats.addread_cache_md_data_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_MISSES.ordinal()]);
        nativeStats.addread_cache_md_nulls_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_MISSES.ordinal()]);
        nativeStats.addread_uncache_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_MISSES.ordinal()]);
        nativeStats.addread_uncache_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_DATA_MISSES.ordinal()]);
        nativeStats.addread_uncache_ext_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_EXT_DATA_MISSES.ordinal()]);
        nativeStats.addread_time_wait_nanos(collectStats[CollectStats.COLLECT_STATS_READ_TIME_WAIT_NANOS.ordinal()]);

        freeQueryMemory(queryMemoryId);
    }
}
