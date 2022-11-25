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

import io.starburst.stargate.buffer.status.StatusProvider;

import javax.inject.Inject;

import java.util.Optional;

import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.STARTED;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.STARTING;
import static java.util.Objects.requireNonNull;

public class DataServerStatusProvider
        implements StatusProvider
{
    private final BufferNodeStateManager stateManager;
    private final Optional<DiscoveryBroadcast> discoveryBroadcast;

    @Inject
    public DataServerStatusProvider(BufferNodeStateManager stateManager, Optional<DiscoveryBroadcast> discoveryBroadcast)
    {
        this.stateManager = requireNonNull(stateManager, "stateManager is null");
        this.discoveryBroadcast = requireNonNull(discoveryBroadcast, "discoveryBroadcast is null");
    }

    @Override
    public boolean isStarted()
    {
        return this.stateManager.getState() != STARTING;
    }

    @Override
    public boolean isReady()
    {
        return isStarted() && this.stateManager.getState() != STARTED;
    }

    @Override
    public boolean isAlive()
    {
        return isReady() && discoveryBroadcast.map(DiscoveryBroadcast::isRegistered).orElse(true);
    }
}
