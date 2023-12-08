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
import io.opentelemetry.api.trace.Tracer;
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
    private final Tracer tracer;

    @Inject
    public BufferCoordinatorExchangeManager(
            DataApiFacade dataApi,
            PartitionNodeMapperFactory partitionNodeMapperFactory,
            ScheduledExecutorService scheduledExecutor,
            BufferExchangeConfig config,
            Tracer tracer)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.partitionNodeMapperFactory = requireNonNull(partitionNodeMapperFactory, "partitionNodeMapperFactory is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        requireNonNull(config, "config is null");
        this.sourceHandleTargetChunksCount = config.getSourceHandleTargetChunksCount();
        this.sourceHandleTargetDataSize = requireNonNull(config.getSourceHandleTargetDataSize(), "sourceHandleTargetDataSize is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public Exchange createExchange(ExchangeContext exchangeContext, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return new BufferExchange(
                exchangeContext.getQueryId(),
                exchangeContext.getExchangeId(),
                outputPartitionCount,
                preserveOrderWithinPartition,
                dataApi,
                tracer,
                exchangeContext.getParentSpan(),
                partitionNodeMapperFactory,
                scheduledExecutor,
                sourceHandleTargetChunksCount,
                sourceHandleTargetDataSize);
    }
}
