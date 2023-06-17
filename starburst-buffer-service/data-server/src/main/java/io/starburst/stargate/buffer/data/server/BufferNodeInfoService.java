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

import com.google.inject.Inject;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class BufferNodeInfoService
{
    private final long bufferNodeId;
    private final URI baseUri;
    private final MemoryAllocator memoryAllocator;
    private final BufferNodeStateManager stateManager;
    private final DataServerStats dataServerStats;

    @Inject
    public BufferNodeInfoService(
            BufferNodeId bufferNodeId,
            MemoryAllocator memoryAllocator,
            HttpServerInfo httpServerInfo,
            NodeInfo airliftNodeInfo,
            BufferNodeStateManager stateManager,
            DataServerStats dataServerStats)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.stateManager = requireNonNull(stateManager, "stateManager is null");
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");

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
        return new BufferNodeInfo(bufferNodeId, baseUri, getBufferNodeStats(), stateManager.getState(), Instant.now());
    }

    private Optional<BufferNodeStats> getBufferNodeStats()
    {
        BufferNodeStats bufferNodeStats = new BufferNodeStats(
                memoryAllocator.getTotalMemory(),
                memoryAllocator.getFreeMemory(),
                (int) dataServerStats.getTrackedExchanges(),
                (int) dataServerStats.getOpenChunks(),
                (int) dataServerStats.getClosedChunks(),
                (int) dataServerStats.getSpooledChunks(),
                dataServerStats.getSpooledDataSize().getTotalCount(),
                dataServerStats.getReadDataSize().getTotalCount(),
                dataServerStats.getWrittenDataSize().getTotalCount());
        return Optional.of(bufferNodeStats);
    }
}
