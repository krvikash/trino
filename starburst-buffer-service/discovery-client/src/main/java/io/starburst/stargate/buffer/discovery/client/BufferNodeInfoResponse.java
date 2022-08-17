/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BufferNodeInfoResponse
{
    // should response be considered complete and actionable upon
    // Discovery service will return false some time after startup
    private final boolean responseComplete;
    private final Set<BufferNodeInfo> bufferNodeInfos;

    @JsonCreator
    public BufferNodeInfoResponse(
            @JsonProperty("responseComplete") boolean responseComplete,
            @JsonProperty("bufferNodeInfos") Set<BufferNodeInfo> bufferNodeInfos)
    {
        this.responseComplete = responseComplete;
        this.bufferNodeInfos = requireNonNull(bufferNodeInfos, "bufferNodeInfos is null");
    }

    @JsonProperty
    public boolean isResponseComplete()
    {
        return responseComplete;
    }

    @JsonProperty
    public Set<BufferNodeInfo> getBufferNodeInfos()
    {
        return bufferNodeInfos;
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
        BufferNodeInfoResponse that = (BufferNodeInfoResponse) o;
        return responseComplete == that.responseComplete
                && bufferNodeInfos.equals(that.bufferNodeInfos);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(responseComplete, bufferNodeInfos);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("responseComplete", responseComplete)
                .add("bufferNodeInfos", bufferNodeInfos)
                .toString();
    }
}
