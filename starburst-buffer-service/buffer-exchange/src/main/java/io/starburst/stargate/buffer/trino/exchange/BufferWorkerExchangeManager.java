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

import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class BufferWorkerExchangeManager
{
    private final DataApiFacade dataApi;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final BufferExchangeConfig config;
    private final ExecutorService executor;

    @Inject
    public BufferWorkerExchangeManager(
            DataApiFacade dataApi,
            BufferNodeDiscoveryManager discoveryManager,
            BufferExchangeConfig config,
            ExecutorService executor)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.config = requireNonNull(config, "config is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        BufferExchangeSinkInstanceHandle bufferExchangeSinkInstanceHandle = (BufferExchangeSinkInstanceHandle) handle;
        return new BufferExchangeSink(
                dataApi,
                bufferExchangeSinkInstanceHandle,
                config.getSinkBlockedMemoryLowWaterMark(),
                config.getSinkBlockedMemoryHighWaterMark(),
                config.getSinkTargetWrittenPagesCount(),
                config.getSinkTargetWrittenPagesSize(),
                executor);
    }

    public ExchangeSource createSource()
    {
        return new BufferExchangeSource(
                dataApi,
                discoveryManager,
                config.getSourceBlockedMemoryLowWaterMark(),
                config.getSourceBlockedMemoryHighWaterMark(),
                config.getSourceParallelism());
    }
}
