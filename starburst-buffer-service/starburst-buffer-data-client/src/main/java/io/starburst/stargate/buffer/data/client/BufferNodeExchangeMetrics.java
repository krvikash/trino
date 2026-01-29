/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

/**
 * Metrics for data storage and distribution within a single buffer node for an exchange.
 * Provides visibility into memory usage vs object storage spooling.
 */
public record BufferNodeExchangeMetrics(
        int partitionCount,
        int chunksInMemory,
        long bytesInMemory,
        int chunksSpooled,
        long bytesSpooled,
        int totalChunks,
        long totalBytes)
{}
