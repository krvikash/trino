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
import io.airlift.units.DataSize;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class BufferCoordinatorExchangeManager
{
    private final DataApiFacade dataApi;
    private final PartitionNodeMapperFactory partitionNodeMapperFactory;
    private final ScheduledExecutorService scheduledExecutor;
    private final int sourceHandleTargetChunksCount;
    private final DataSize sourceHandleTargetDataSize;

    @Inject
    public BufferCoordinatorExchangeManager(
            DataApiFacade dataApi,
            PartitionNodeMapperFactory partitionNodeMapperFactory,
            ScheduledExecutorService scheduledExecutor,
            BufferExchangeConfig config)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.partitionNodeMapperFactory = requireNonNull(partitionNodeMapperFactory, "partitionNodeMapperFactory is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        requireNonNull(config, "config is null");
        this.sourceHandleTargetChunksCount = config.getSourceHandleTargetChunksCount();
        this.sourceHandleTargetDataSize = requireNonNull(config.getSourceHandleTargetDataSize(), "sourceHandleTargetDataSize is null");
    }

    public Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return new BufferExchange(
                context.getQueryId(),
                context.getExchangeId(),
                outputPartitionCount,
                preserveOrderWithinPartition,
                dataApi,
                partitionNodeMapperFactory,
                scheduledExecutor,
                sourceHandleTargetChunksCount,
                sourceHandleTargetDataSize);
    }
}
