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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSinkInstanceHandle
        implements ExchangeSinkInstanceHandle
{
    private final BufferExchangeSinkHandle sinkHandle;
    private final int taskAttemptId;
    private final Map<Integer, Long> partitionToBufferNode;

    @JsonCreator
    public BufferExchangeSinkInstanceHandle(
            @JsonProperty("sinkHandle") BufferExchangeSinkHandle sinkHandle,
            @JsonProperty("taskAttemptId") int taskAttemptId,
            @JsonProperty("partitionToBufferNode") Map<Integer, Long> partitionToBufferNode)
    {
        this.sinkHandle = sinkHandle;
        this.taskAttemptId = taskAttemptId;
        requireNonNull(partitionToBufferNode, "partitionToBufferNode is null");
        this.partitionToBufferNode = ImmutableMap.copyOf(partitionToBufferNode);
    }

    @JsonProperty
    public BufferExchangeSinkHandle getSinkHandle()
    {
        return sinkHandle;
    }

    public String getExternalExchangeId()
    {
        return sinkHandle.getExternalExchangeId();
    }

    public int getTaskPartitionId()
    {
        return sinkHandle.getTaskPartitionId();
    }

    @JsonProperty
    public int getTaskAttemptId()
    {
        return taskAttemptId;
    }

    @JsonProperty
    public Map<Integer, Long> getPartitionToBufferNode()
    {
        return partitionToBufferNode;
    }

    public boolean isPreserveOrderWithinPartition()
    {
        return sinkHandle.isPreserveOrderWithinPartition();
    }

    public int getOutputPartitionCount()
    {
        return sinkHandle.getOutputPartitionCount();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sinkHandle", sinkHandle)
                .add("taskAttemptId", taskAttemptId)
                .toString();
    }
}
