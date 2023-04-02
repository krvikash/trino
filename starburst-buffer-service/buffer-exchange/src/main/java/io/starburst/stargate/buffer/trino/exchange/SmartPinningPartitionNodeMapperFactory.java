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

import io.airlift.units.Duration;
import io.trino.spi.exchange.ExchangeId;

import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class SmartPinningPartitionNodeMapperFactory
        implements PartitionNodeMapperFactory
{
    private final BufferNodeDiscoveryManager discoveryManager;
    private final ScheduledExecutorService executor;
    private final int minBaseBufferNodesPerPartition;
    private final int maxBaseBufferNodesPerPartition;
    private final Duration maxWaitActiveBufferNodes;
    private final double bonusBufferNodesMultiplier;
    private final int minTotalBufferNodesPerPartition;
    private final int maxTotalBufferNodesPerPartition;

    @Inject
    public SmartPinningPartitionNodeMapperFactory(BufferNodeDiscoveryManager discoveryManager, ScheduledExecutorService executor, BufferExchangeConfig config)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.minBaseBufferNodesPerPartition = config.getMinBaseBufferNodesPerPartition();
        this.bonusBufferNodesMultiplier = config.getBonusBufferNodesPerPartitionMultiplier();
        this.maxBaseBufferNodesPerPartition = config.getMaxBaseBufferNodesPerPartition();
        this.minTotalBufferNodesPerPartition = config.getMinTotalBufferNodesPerPartition();
        this.maxTotalBufferNodesPerPartition = config.getMaxTotalBufferNodesPerPartition();
        this.maxWaitActiveBufferNodes = config.getMaxWaitActiveBufferNodes();
    }

    @Override
    public PartitionNodeMapper getPartitionNodeMapper(ExchangeId exchangeId, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return new SmartPinningPartitionNodeMapper(
                exchangeId,
                discoveryManager,
                executor,
                outputPartitionCount,
                preserveOrderWithinPartition,
                minBaseBufferNodesPerPartition,
                maxBaseBufferNodesPerPartition,
                bonusBufferNodesMultiplier,
                minTotalBufferNodesPerPartition,
                maxTotalBufferNodesPerPartition,
                maxWaitActiveBufferNodes);
    }
}
