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

import io.airlift.log.Logger;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class BufferExchangeManager
        implements ExchangeManager
{
    private static final Logger log = Logger.get(BufferExchangeManager.class);

    private final BufferCoordinatorExchangeManager coordinatorExchangeManager;
    private final BufferWorkerExchangeManager workerExchangeManager;

    @Inject
    public BufferExchangeManager(
            BufferCoordinatorExchangeManager coordinatorExchangeManager,
            BufferWorkerExchangeManager workerExchangeManager)
    {
        this.coordinatorExchangeManager = requireNonNull(coordinatorExchangeManager, "coordinatorExchangeManager is null");
        this.workerExchangeManager = requireNonNull(workerExchangeManager, "workerExchangeManager is null");
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return coordinatorExchangeManager.createExchange(context, outputPartitionCount, preserveOrderWithinPartition);
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        return workerExchangeManager.createSink(handle);
    }

    @Override
    public ExchangeSource createSource()
    {
        return workerExchangeManager.createSource();
    }
}
