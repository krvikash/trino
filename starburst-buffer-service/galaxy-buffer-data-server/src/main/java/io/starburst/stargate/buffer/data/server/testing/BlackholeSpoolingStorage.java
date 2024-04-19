/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
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

package io.starburst.stargate.buffer.data.server.testing;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class BlackholeSpoolingStorage
        implements SpoolingStorage
{
    @Inject
    public BlackholeSpoolingStorage(
            ChunkManagerConfig config)
    {
        checkArgument(config.isChunkSpoolMergeEnabled(), "chunk merging on spool must be enabled");
    }

    @Override
    public SpooledChunk getSpooledChunk(long chunkBufferNodeId, String exchangeId, long chunkId)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public ListenableFuture<Map<Long, SpooledChunk>> writeMergedChunks(long bufferNodeId, String exchangeId, Map<Chunk, ChunkDataLease> chunkDataLeaseMap, long contentLength)
    {
        Map<Long, SpooledChunk> resultMap = new HashMap<>();
        chunkDataLeaseMap.keySet().forEach(chunk -> resultMap.put(chunk.getChunkId(), new SpooledChunk("dummy", 0, 10)));
        CompletableFuture<Map<Long, SpooledChunk>> future = new CompletableFuture<>();
        future.completeOnTimeout(resultMap, 1, TimeUnit.MILLISECONDS);
        return MoreFutures.toListenableFuture(future);
    }

    @Override
    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> writeMetadataFile(long bufferNodeId, Slice metadataSlice)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public ListenableFuture<Slice> readMetadataFile(long bufferNodeId)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public int getSpooledChunksCount()
    {
        // we do not keep track of count; this is only used for statistics so returning dummy value in test code does not matter
        return 0;
    }

    @Override
    public void close() {}
}
