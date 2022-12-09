/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server.testing;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestingDiscoveryApi
        implements DiscoveryApi
{
    private final Map<Long, BufferNodeInfo> infos = new ConcurrentHashMap<>();

    @Override
    public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
    {
        infos.put(bufferNodeInfo.nodeId(), bufferNodeInfo);
    }

    @Override
    public BufferNodeInfoResponse getBufferNodes()
    {
        return new BufferNodeInfoResponse(
                true,
                ImmutableSet.copyOf(infos.values()));
    }
}
