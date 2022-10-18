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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(BufferExchangeSourceHandle.class).instanceSize());

    private final String externalExchangeId;
    private final int partitionId;
    private final long[] bufferNodeIds;
    private final long[] chunkIds;
    private final long dataSizeInBytes;
    private final boolean preserveOrderWithinPartition;
    private final Optional<byte[]> encryptionKey;

    @JsonCreator
    @Deprecated // for Jackson and internal use
    public BufferExchangeSourceHandle(
            @JsonProperty("externalExchangeId") String externalExchangeId,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("bufferNodeIds") long[] bufferNodeIds,
            @JsonProperty("chunkIds") long[] chunkIds,
            @JsonProperty("dataSizeInBytes") long dataSizeInBytes,
            @JsonProperty("preserveOrderWithinPartition") boolean preserveOrderWithinPartition,
            @JsonProperty("encryptionKey") Optional<byte[]> encryptionKey)
    {
        this.externalExchangeId = requireNonNull(externalExchangeId, "externalExchangeId is null");
        this.partitionId = partitionId;
        this.bufferNodeIds = requireNonNull(bufferNodeIds, "bufferNodeIds is null");
        this.chunkIds = requireNonNull(chunkIds, "chunkIds is null");
        checkArgument(bufferNodeIds.length == chunkIds.length, "length of bufferNodeIds(%d) and chunkIds(%d) should be equal", bufferNodeIds.length, chunkIds.length);
        this.dataSizeInBytes = dataSizeInBytes;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
    }

    public static BufferExchangeSourceHandle fromChunkHandles(
            String externalExchangeId,
            int partitionId,
            List<ChunkHandle> chunkHandles,
            boolean preserveOrderWithinPartition,
            Optional<byte[]> encryptionKey)
    {
        long[] bufferNodeIds = new long[chunkHandles.size()];
        long[] chunkIds = new long[chunkHandles.size()];
        long dataSizeInBytes = 0;
        int pos = 0;
        for (ChunkHandle chunkHandle : chunkHandles) {
            checkArgument(chunkHandle.partitionId() == partitionId, "all chunkHandles should belong to same partition %d; got %d", partitionId, chunkHandle.partitionId());
            bufferNodeIds[pos] = chunkHandle.bufferNodeId();
            chunkIds[pos] = chunkHandle.chunkId();
            dataSizeInBytes += chunkHandle.dataSizeInBytes();
            pos++;
        }

        return new BufferExchangeSourceHandle(
                externalExchangeId,
                partitionId,
                bufferNodeIds,
                chunkIds,
                dataSizeInBytes,
                preserveOrderWithinPartition,
                encryptionKey);
    }

    @JsonProperty
    public String getExternalExchangeId()
    {
        return externalExchangeId;
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @Deprecated // needed for JSON serialization
    public long[] getBufferNodeIds()
    {
        return bufferNodeIds;
    }

    @JsonProperty
    @Deprecated // needed for JSON serialization
    public long[] getChunkIds()
    {
        return chunkIds;
    }

    public int getChunksCount()
    {
        return bufferNodeIds.length;
    }

    public long getBufferNodeId(int pos)
    {
        checkPositionIndex(pos, bufferNodeIds.length);
        return bufferNodeIds[pos];
    }

    public long getChunkId(int pos)
    {
        checkPositionIndex(pos, chunkIds.length);
        return chunkIds[pos];
    }

    @Override
    @JsonProperty
    public long getDataSizeInBytes()
    {
        return dataSizeInBytes;
    }

    @JsonProperty
    public boolean isPreserveOrderWithinPartition()
    {
        return preserveOrderWithinPartition;
    }

    @JsonProperty
    public Optional<byte[]> getEncryptionKey()
    {
        return encryptionKey;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(externalExchangeId)
                + sizeOf(bufferNodeIds)
                + sizeOf(chunkIds)
                + sizeOf(encryptionKey, SizeOf::sizeOf);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("externalExchangeId", externalExchangeId)
                .add("partitionId", partitionId)
                .add("bufferNodeIds", bufferNodeIds)
                .add("chunkIds", chunkIds)
                .add("dataSizeInBytes", dataSizeInBytes)
                .add("encryptionKey", encryptionKey.map(value -> "[REDACTED]"))
                .toString();
    }
}
