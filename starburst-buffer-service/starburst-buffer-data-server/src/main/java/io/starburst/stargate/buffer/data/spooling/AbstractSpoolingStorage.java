/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.translateFailures;
import static java.util.Objects.requireNonNull;

public abstract class AbstractSpoolingStorage
            implements SpoolingStorage
{
    private final MergedFileNameGenerator mergedFileNameGenerator;
    private final CounterStat spooledDataSize;
    private final CounterStat spoolingFailures;
    private final DistributionStat spooledChunkSizeDistribution;
    private final DistributionStat spooledSharingExchangeCount;
    private final DistributionStat spooledFileSizeDistribution;

    // exchangeId -> chunkId -> fileSize
    private final Map<String, Map<Long, Integer>> fileSizes = new ConcurrentHashMap<>();

    protected AbstractSpoolingStorage(
            BufferNodeId bufferNodeId,
            MergedFileNameGenerator mergedFileNameGenerator,
            DataServerStats dataServerStats)
    {
        this.mergedFileNameGenerator = requireNonNull(mergedFileNameGenerator, "mergedFileNameGenerator is null");
        this.spooledDataSize = dataServerStats.getSpooledDataSize();
        this.spoolingFailures = dataServerStats.getSpoolingFailures();
        this.spooledChunkSizeDistribution = dataServerStats.getSpooledChunkSizeDistribution();
        this.spooledSharingExchangeCount = dataServerStats.getSpooledSharingExchangeCount();
        this.spooledFileSizeDistribution = dataServerStats.getSpooledFileSizeDistribution();
    }

    protected abstract String getLocation(String fileName);

    protected abstract ListenableFuture<Void> deleteDirectories(List<String> directoryNames);

    protected abstract ListenableFuture<Map<Long, SpooledChunk>> putStorageObject(
            String fileName,
            Map<Chunk, ChunkDataLease> chunkDataLeaseMap,
            long contentLength);

    @Override
    public ListenableFuture<Map<Long, SpooledChunk>> writeMergedChunks(long bufferNodeId, String exchangeId, Map<Chunk, ChunkDataLease> chunkDataLeaseMap, long contentLength)
    {
        checkArgument(!chunkDataLeaseMap.isEmpty(), "unexpected empty collection when spooling merged chunks");
        String mergedFileName = mergedFileNameGenerator.getNextMergedFileName(bufferNodeId, exchangeId);
        ListenableFuture<Map<Long, SpooledChunk>> putObjectFuture = putStorageObject(mergedFileName, chunkDataLeaseMap, contentLength);
        // not chaining result with whenComplete as it breaks cancellation
        Futures.addCallback(putObjectFuture,
                new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result)
                    {
                        spooledDataSize.update(contentLength);
                        chunkDataLeaseMap.values().forEach(chunkDataLease -> spooledChunkSizeDistribution.add(chunkDataLease.serializedSizeInBytes()));
                        spooledSharingExchangeCount.add(chunkDataLeaseMap.size());
                        spooledFileSizeDistribution.add(contentLength);
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        spoolingFailures.update(1);
                    }
                }, directExecutor());
        return translateFailures(putObjectFuture);
    }

    @Override
    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        fileSizes.remove(exchangeId);
        return translateFailures(deleteDirectories(getPrefixedDirectories(bufferNodeId, exchangeId)));
    }
}
