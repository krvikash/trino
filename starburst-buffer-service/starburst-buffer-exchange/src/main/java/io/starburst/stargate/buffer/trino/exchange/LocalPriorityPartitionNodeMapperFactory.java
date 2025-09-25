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

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.exchange.ExchangeId;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class LocalPriorityPartitionNodeMapperFactory
        implements PartitionNodeMapperFactory
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final ScheduledExecutorService executor;
    private final Duration maxWaitActiveBufferNodes;

    @Inject
    public LocalPriorityPartitionNodeMapperFactory(BufferNodeDiscoveryManager discoveryManager, ScheduledExecutorService executor, BufferExchangeConfig config)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.maxWaitActiveBufferNodes = config.getMaxWaitActiveBufferNodes();
    }

    @Override
    public PartitionNodeMapper getPartitionNodeMapper(ExchangeId exchangeId, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return new LocalPriorityPartitionNodeMapper(discoveryManager, executor, outputPartitionCount, 3, maxWaitActiveBufferNodes);
    }
}
