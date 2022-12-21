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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;

import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.PINNING_MULTI;

public class BufferExchangeConfig
{
    private URI discoveryServiceUri;
    private DataSize sinkBlockedMemoryLowWaterMark = DataSize.of(128, DataSize.Unit.MEGABYTE);
    private DataSize sinkBlockedMemoryHighWaterMark = DataSize.of(256, DataSize.Unit.MEGABYTE);
    private DataSize sourceBlockedMemoryLowWaterMark = DataSize.of(32, DataSize.Unit.MEGABYTE);
    private DataSize sourceBlockedMemoryHighWaterMark = DataSize.of(64, DataSize.Unit.MEGABYTE);
    private int sourceParallelism = 4;
    private int sourceHandleTargetChunksCount = 64;
    private DataSize sourceHandleTargetDataSize = DataSize.of(256, DataSize.Unit.MEGABYTE);
    private int sinkTargetWrittenPagesCount = 512;
    private DataSize sinkTargetWrittenPagesSize = DataSize.of(16, DataSize.Unit.MEGABYTE);
    private int sinkTargetWrittenPartitionsCount = 16;
    private PartitionNodeMappingMode partitionNodeMappingMode = PINNING_MULTI;
    private int minBufferNodesPerPartition = 2;
    private int maxBufferNodesPerPartition = 32;

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

    public int getSinkTargetWrittenPartitionsCount()
    {
        return sinkTargetWrittenPartitionsCount;
    }

    @Config("exchange.sink-target-written-partitions-count")
    @ConfigDescription("Target number of partitions written to in a single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPartitionsCount(int sinkTargetWrittenPartitionsCount)
    {
        this.sinkTargetWrittenPartitionsCount = sinkTargetWrittenPartitionsCount;
        return this;
    }

    @Config("exchange.partition-node-mapping-mode")
    @ConfigDescription("How are the output partitions of Trino tasks mapped to buffer service nodes")
    public BufferExchangeConfig setPartitionNodeMappingMode(PartitionNodeMappingMode partitionNodeMappingMode)
    {
        this.partitionNodeMappingMode = partitionNodeMappingMode;
        return this;
    }

    public PartitionNodeMappingMode getPartitionNodeMappingMode()
    {
        return partitionNodeMappingMode;
    }

    @Min(1)
    public int getMinBufferNodesPerPartition()
    {
        return minBufferNodesPerPartition;
    }

    @Config("exchange.min-buffer-nodes-per-partition")
    public BufferExchangeConfig setMinBufferNodesPerPartition(int minBufferNodesPerPartition)
    {
        this.minBufferNodesPerPartition = minBufferNodesPerPartition;
        return this;
    }

    @Min(1)
    public int getMaxBufferNodesPerPartition()
    {
        return maxBufferNodesPerPartition;
    }

    @Config("exchange.max-buffer-nodes-per-partition")
    public BufferExchangeConfig setMaxBufferNodesPerPartition(int maxBufferNodesPerPartition)
    {
        this.maxBufferNodesPerPartition = maxBufferNodesPerPartition;
        return this;
    }
}
