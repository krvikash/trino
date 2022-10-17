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

import io.airlift.units.DataSize;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;

import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.starburst.stargate.buffer.trino.exchange.EncryptionKeys.generateNewEncryptionKey;
import static java.util.Objects.requireNonNull;

public class BufferCoordinatorExchangeManager
{
    private final DataApiFacade dataApi;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final PartitionNodeMapperFactory partitionNodeMapperFactory;
    private final ScheduledExecutorService scheduledExecutor;
    private final int sourceHandleTargetChunksCount;
    private final DataSize sourceHandleTargetDataSize;
    private final boolean encryptionEnabled;

    @Inject
    public BufferCoordinatorExchangeManager(
            DataApiFacade dataApi,
            BufferNodeDiscoveryManager discoveryManager,
            PartitionNodeMapperFactory partitionNodeMapperFactory,
            ScheduledExecutorService scheduledExecutor,
            BufferExchangeConfig config)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.partitionNodeMapperFactory = requireNonNull(partitionNodeMapperFactory, "partitionNodeMapperFactory is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        requireNonNull(config, "config is null");
        this.sourceHandleTargetChunksCount = config.getSourceHandleTargetChunksCount();
        this.sourceHandleTargetDataSize = requireNonNull(config.getSourceHandleTargetDataSize(), "sourceHandleTargetDataSize is null");
        this.encryptionEnabled = config.isEncryptionEnabled();
    }

    public Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        Optional<SecretKey> encryptionKey = Optional.empty();
        if (encryptionEnabled) {
            encryptionKey = Optional.of(generateNewEncryptionKey());
        }
        return new BufferExchange(
                context.getQueryId(),
                context.getExchangeId(),
                outputPartitionCount,
                preserveOrderWithinPartition,
                dataApi,
                discoveryManager,
                partitionNodeMapperFactory,
                scheduledExecutor,
                sourceHandleTargetChunksCount,
                sourceHandleTargetDataSize,
                encryptionKey);
    }
}
