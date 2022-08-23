/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DataApiFacade
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final ApiFactory apiFactory;
    private final Map<Long, DataApi> dataApiClients = new ConcurrentHashMap<>();

    @Inject
    public DataApiFacade(
            BufferNodeDiscoveryManager discoveryManager,
            ApiFactory apiFactory)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
    }

    public ListenableFuture<ChunkList> listClosedChunks(long bufferNodeId, String exchangeId, OptionalLong pagingId)
    {
        try {
            return getDataApi(bufferNodeId).listClosedChunks(exchangeId, pagingId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> registerExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).registerExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> pingExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).pingExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).removeExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> addDataPages(long bufferNodeId, String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, List<Slice> dataPages)
    {
        try {
            return getDataApi(bufferNodeId).addDataPages(exchangeId, partitionId, taskId, attemptId, dataPageId, dataPages);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> finishExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).finishExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
    {
        try {
            return getDataApi(bufferNodeId).getChunkData(exchangeId, partitionId, chunkId, chunkBufferNodeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    private DataApi getDataApi(long bufferNodeId)
    {
        return dataApiClients.computeIfAbsent(bufferNodeId, this::createDataApi);
    }

    // todo periodically destroy DataApi objects for buffer nodes which disappeared

    private DataApi createDataApi(long bufferNodeId)
    {
        BufferNodeInfo bufferNodeInfo = discoveryManager.getBufferNodes().get(bufferNodeId);
        if (bufferNodeInfo == null) {
            // todo: for created clients periodically check if node is still around
            throw new DataApiException(ErrorCode.BUFFER_NODE_NOT_FOUND, "Buffer node " + bufferNodeId + " not found");
        }
        return createDataApi(bufferNodeInfo);
    }

    private DataApi createDataApi(BufferNodeInfo bufferNodeInfo)
    {
        return apiFactory.createDataApi(bufferNodeInfo);
    }

    private void closeDataApi(DataApi dataApi)
    {
        // todo
    }
}
