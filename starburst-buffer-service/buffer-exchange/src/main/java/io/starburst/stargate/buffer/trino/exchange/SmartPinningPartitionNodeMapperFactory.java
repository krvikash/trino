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

import io.trino.spi.exchange.ExchangeId;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class SmartPinningPartitionNodeMapperFactory
        implements PartitionNodeMapperFactory
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final int minBufferNodesPerPartition;
    private final int maxBufferNodesPerPartition;

    @Inject
    public SmartPinningPartitionNodeMapperFactory(BufferNodeDiscoveryManager discoveryManager, BufferExchangeConfig config)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.minBufferNodesPerPartition = config.getMinBufferNodesPerPartition();
        this.maxBufferNodesPerPartition = config.getMaxBufferNodesPerPartition();
    }

    @Override
    public PartitionNodeMapper getPartitionNodeMapper(ExchangeId exchangeId, int outputPartitionCount)
    {
        return new SmartPinningPartitionNodeMapper(
                exchangeId,
                discoveryManager,
                outputPartitionCount,
                minBufferNodesPerPartition,
                maxBufferNodesPerPartition);
    }
}
