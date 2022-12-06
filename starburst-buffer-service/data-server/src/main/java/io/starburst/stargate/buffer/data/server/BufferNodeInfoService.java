/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;

import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class BufferNodeInfoService
{
    private final long bufferNodeId;
    private final URI baseUri;
    private final MemoryAllocator memoryAllocator;
    private final ChunkManager chunkManager;
    private final BufferNodeStateManager stateManager;

    @Inject
    public BufferNodeInfoService(
            BufferNodeId bufferNodeId,
            MemoryAllocator memoryAllocator,
            ChunkManager chunkManager,
            HttpServerInfo httpServerInfo,
            NodeInfo airliftNodeInfo,
            BufferNodeStateManager stateManager)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.stateManager = requireNonNull(stateManager, "stateManager is null");

        baseUri = uriBuilderFrom(getBaseUri(httpServerInfo))
                .host(airliftNodeInfo.getExternalAddress())
                .build();
    }

    private URI getBaseUri(HttpServerInfo httpServerInfo)
    {
        if (!isNull(httpServerInfo.getHttpsUri())) {
            return httpServerInfo.getHttpsUri();
        }
        return httpServerInfo.getHttpUri();
    }

    public BufferNodeInfo getNodeInfo()
    {
        return new BufferNodeInfo(bufferNodeId, baseUri, getBufferNodeStats(), stateManager.getState());
    }

    private Optional<BufferNodeStats> getBufferNodeStats()
    {
        BufferNodeStats bufferNodeStats = new BufferNodeStats(
                memoryAllocator.getTotalMemory(),
                memoryAllocator.getFreeMemory(),
                chunkManager.getTrackedExchanges(),
                chunkManager.getOpenChunks(),
                chunkManager.getClosedChunks(),
                chunkManager.getSpooledChunks());
        return Optional.of(bufferNodeStats);
    }
}
