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
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.translateFailures;

public abstract class AbstractSpoolingStorage
            implements SpoolingStorage
{
    private final long bufferNodeId;

    private final CounterStat spooledDataSize;
    private final CounterStat spoolingFailures;
    private final DistributionStat spooledChunkSizeDistribution;

    // exchangeId -> chunkId -> fileSize
    private final Map<String, Map<Long, Integer>> fileSizes = new ConcurrentHashMap<>();

    protected AbstractSpoolingStorage(
            BufferNodeId bufferNodeId,
            DataServerStats dataServerStats)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.spooledDataSize = dataServerStats.getSpooledDataSize();
        this.spoolingFailures = dataServerStats.getSpoolingFailures();
        this.spooledChunkSizeDistribution = dataServerStats.getSpooledChunkSizeDistribution();
    }

    protected abstract int getFileSize(String fileName) throws SpooledChunkNotFoundException;

    protected abstract String getLocation(String fileName);

    protected abstract ListenableFuture<Void> deleteDirectories(List<String> directoryNames);

    protected abstract ListenableFuture<?> putStorageObject(String fileName, ChunkDataLease chunkDataLease);

    @Override
    public SpoolingFile getSpoolingFile(long chunkBufferNodeId, String exchangeId, long chunkId)
    {
        String fileName = getFileName(chunkBufferNodeId, exchangeId, chunkId);
        Map<Long, Integer> chunkIdToFileSizes = fileSizes.get(exchangeId);
        int length;
        try {
            if (chunkIdToFileSizes != null && chunkBufferNodeId == this.bufferNodeId) {
                Integer fileSize = chunkIdToFileSizes.get(chunkId);
                length = fileSize != null ? fileSize : getFileSize(fileName);
            }
            else {
                // Synchronous communication to external storage for file size is rare and will only happen when a node dies
                // TODO: measure metrics to requesting file size from external storage
                length = getFileSize(fileName);
            }
        }
        catch (SpooledChunkNotFoundException e) {
            throw new DataServerException(CHUNK_NOT_FOUND,
                    "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(chunkBufferNodeId, exchangeId, chunkId));
        }
        return new SpoolingFile(getLocation(fileName), length);
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease)
    {
        checkArgument(!chunkDataLease.chunkSlices().isEmpty(), "unexpected empty chunk when spooling");

        fileSizes.computeIfAbsent(exchangeId, ignored -> new ConcurrentHashMap<>()).put(chunkId, chunkDataLease.serializedSizeInBytes());

        ListenableFuture<?> putObjectFuture = putStorageObject(getFileName(bufferNodeId, exchangeId, chunkId), chunkDataLease);
        // not chaining result with whenComplete as it breaks cancellation
        Futures.addCallback(putObjectFuture,
                new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result)
                    {
                        spooledDataSize.update(chunkDataLease.serializedSizeInBytes());
                        spooledChunkSizeDistribution.add(chunkDataLease.serializedSizeInBytes());
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        spoolingFailures.update(1);
                    }
                }, MoreExecutors.directExecutor());

        return translateFailures(asVoid(putObjectFuture));
    }

    @Override
    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        fileSizes.remove(exchangeId);
        return translateFailures(deleteDirectories(getPrefixedDirectories(bufferNodeId, exchangeId)));
    }

    @Override
    public int getSpooledChunks()
    {
        return fileSizes.values().stream().mapToInt(Map::size).sum();
    }
}
