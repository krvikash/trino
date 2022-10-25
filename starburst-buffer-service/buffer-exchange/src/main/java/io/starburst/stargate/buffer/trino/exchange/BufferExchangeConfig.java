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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class BufferExchangeConfig
{
    private URI discoveryServiceUri;
    private DataSize sinkBlockedMemoryLowWaterMark = DataSize.of(128, DataSize.Unit.MEGABYTE);
    private DataSize sinkBlockedMemoryHighWaterMark = DataSize.of(512, DataSize.Unit.MEGABYTE);
    private DataSize sourceBlockedMemoryLowWaterMark = DataSize.of(128, DataSize.Unit.MEGABYTE);
    private DataSize sourceBlockedMemoryHighWaterMark = DataSize.of(512, DataSize.Unit.MEGABYTE);
    private int sourceParallelism = 4;
    private boolean dataIntegrityVerificationEnabled = true;
    private int sourceHandleTargetChunksCount = 64;
    private DataSize sourceHandleTargetDataSize = DataSize.of(256, DataSize.Unit.MEGABYTE);
    private boolean encryptionEnabled = true;
    private int sinkTargetWrittenPagesCount = 3;
    private DataSize sinkTargetWrittenPagesSize = DataSize.of(8, DataSize.Unit.MEGABYTE);

    @NotNull
    public URI getDiscoveryServiceUri()
    {
        return discoveryServiceUri;
    }

    @Config("exchange.buffer-discovery.uri")
    @ConfigDescription("Buffer discovery service URI")
    public BufferExchangeConfig setDiscoveryServiceUri(URI discoveryServiceUri)
    {
        this.discoveryServiceUri = discoveryServiceUri;
        return this;
    }

    public DataSize getSinkBlockedMemoryLowWaterMark()
    {
        return sinkBlockedMemoryLowWaterMark;
    }

    @Config("exchange.sink-blocked-memory-low")
    @ConfigDescription("Sink blocked memory low water mark")
    public BufferExchangeConfig setSinkBlockedMemoryLowWaterMark(DataSize sinkBlockedMemoryLowWaterMark)
    {
        this.sinkBlockedMemoryLowWaterMark = sinkBlockedMemoryLowWaterMark;
        return this;
    }

    public DataSize getSinkBlockedMemoryHighWaterMark()
    {
        return sinkBlockedMemoryHighWaterMark;
    }

    @Config("exchange.sink-blocked-memory-high")
    @ConfigDescription("Sink blocked memory high water mark")
    public BufferExchangeConfig setSinkBlockedMemoryHighWaterMark(DataSize sinkBlockedMemoryHighWaterMark)
    {
        this.sinkBlockedMemoryHighWaterMark = sinkBlockedMemoryHighWaterMark;
        return this;
    }

    public DataSize getSourceBlockedMemoryLowWaterMark()
    {
        return sourceBlockedMemoryLowWaterMark;
    }

    @Config("exchange.source-blocked-memory-low")
    @ConfigDescription("Source blocked memory low water mark")
    public BufferExchangeConfig setSourceBlockedMemoryLowWaterMark(DataSize sourceBlockedMemoryLowWaterMark)
    {
        this.sourceBlockedMemoryLowWaterMark = sourceBlockedMemoryLowWaterMark;
        return this;
    }

    public DataSize getSourceBlockedMemoryHighWaterMark()
    {
        return sourceBlockedMemoryHighWaterMark;
    }

    @Config("exchange.source-blocked-memory-high")
    @ConfigDescription("Source blocked memory high water mark")
    public BufferExchangeConfig setSourceBlockedMemoryHighWaterMark(DataSize sourceBlockedMemoryHighWaterMark)
    {
        this.sourceBlockedMemoryHighWaterMark = sourceBlockedMemoryHighWaterMark;
        return this;
    }

    public int getSourceParallelism()
    {
        return sourceParallelism;
    }

    @Config("exchange.source-parallelism")
    @ConfigDescription("Source data reading request parallelism")
    public BufferExchangeConfig setSourceParallelism(int sourceParallelism)
    {
        this.sourceParallelism = sourceParallelism;
        return this;
    }

    @Config("exchange.data-integrity-verification-enabled")
    public BufferExchangeConfig setDataIntegrityVerificationEnabled(boolean dataIntegrityVerificationEnabled)
    {
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        return this;
    }

    public boolean isDataIntegrityVerificationEnabled()
    {
        return dataIntegrityVerificationEnabled;
    }

    @NotNull
    public int getSourceHandleTargetChunksCount()
    {
        return sourceHandleTargetChunksCount;
    }

    @Config("exchange.source-handle-target-chunks-count")
    @ConfigDescription("Target number of chunks referenced by a single source handle")
    public BufferExchangeConfig setSourceHandleTargetChunksCount(int sourceHandleTargetChunksCount)
    {
        this.sourceHandleTargetChunksCount = sourceHandleTargetChunksCount;
        return this;
    }

    @NotNull
    public DataSize getSourceHandleTargetDataSize()
    {
        return sourceHandleTargetDataSize;
    }

    @Config("exchange.source-handle-target-data-size")
    @ConfigDescription("Target size of the data referenced by a single source handle")
    public BufferExchangeConfig setSourceHandleTargetDataSize(DataSize sourceHandleTargetDataSize)
    {
        this.sourceHandleTargetDataSize = sourceHandleTargetDataSize;
        return this;
    }

    public boolean isEncryptionEnabled()
    {
        return encryptionEnabled;
    }

    @Config("exchange.encryption-enabled")
    @ConfigDescription("Should data sent to buffer service be encrypted with per-exchange key")
    public BufferExchangeConfig setEncryptionEnabled(boolean encryptionEnabled)
    {
        this.encryptionEnabled = encryptionEnabled;
        return this;
    }

    public int getSinkTargetWrittenPagesCount()
    {
        return sinkTargetWrittenPagesCount;
    }

    @Config("exchange.sink-target-written-pages-count")
    @ConfigDescription("Target number of pages to be sent in single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPagesCount(int sinkTargetWrittenPagesCount)
    {
        this.sinkTargetWrittenPagesCount = sinkTargetWrittenPagesCount;
        return this;
    }

    public DataSize getSinkTargetWrittenPagesSize()
    {
        return sinkTargetWrittenPagesSize;
    }

    @Config("exchange.sink-target-written-pages-size")
    @ConfigDescription("Target size of data to be sent in single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPagesSize(DataSize sinkTargetWrittenPagesSize)
    {
        this.sinkTargetWrittenPagesSize = sinkTargetWrittenPagesSize;
        return this;
    }
}
