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

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @param responseComplete should response be considered complete and actionable upon Discovery service will return false some time after startup
 */
public record BufferNodeInfoResponse(boolean responseComplete, Set<BufferNodeInfo> bufferNodeInfos)
{
    public BufferNodeInfoResponse
    {
        requireNonNull(bufferNodeInfos, "bufferNodeInfos is null");
    }
}
