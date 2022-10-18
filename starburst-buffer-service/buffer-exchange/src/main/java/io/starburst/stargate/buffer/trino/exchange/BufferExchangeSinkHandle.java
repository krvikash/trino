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
import io.trino.spi.exchange.ExchangeSinkHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSinkHandle
        implements ExchangeSinkHandle
{
    private final String externalExchangeId;
    private final int taskPartitionId;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    // todo encryption related stuff

    @JsonCreator
    public BufferExchangeSinkHandle(
            @JsonProperty("externalExchangeId") String externalExchangeId,
            @JsonProperty("taskPartitionId") int taskPartitionId,
            @JsonProperty("outputPartitionCount") int outputPartitionCount,
            @JsonProperty("preserveOrderWithinPartition") boolean preserveOrderWithinPartition)
    {
        this.externalExchangeId = requireNonNull(externalExchangeId, "exchangeId is null");
        this.taskPartitionId = taskPartitionId;
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
    }

    @JsonProperty
    public String getExternalExchangeId()
    {
        return externalExchangeId;
    }

    @JsonProperty
    public int getTaskPartitionId()
    {
        return taskPartitionId;
    }

    @JsonProperty
    public int getOutputPartitionCount()
    {
        return outputPartitionCount;
    }

    @JsonProperty
    public boolean isPreserveOrderWithinPartition()
    {
        return preserveOrderWithinPartition;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferExchangeSinkHandle that = (BufferExchangeSinkHandle) o;
        return taskPartitionId == that.taskPartitionId
                && outputPartitionCount == that.outputPartitionCount
                && preserveOrderWithinPartition == that.preserveOrderWithinPartition
                && Objects.equals(externalExchangeId, that.externalExchangeId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(externalExchangeId, taskPartitionId, outputPartitionCount, preserveOrderWithinPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("externalExchangeId", externalExchangeId)
                .add("taskPartitionId", taskPartitionId)
                .add("outputPartitionCount", outputPartitionCount)
                .add("preserveOrderWithinPartition", preserveOrderWithinPartition)
                .toString();
    }
}
