/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.benchmarks;

import io.airlift.units.DataSize;

public record SourceBenchmarkSetup(
        int readersCount,
        DataSize totalDataSize,
        DataSize chunkSize,
        DataSize pageSize,
        int chunksPerSourceHandle,
        int parallelism,
        DataSize sourceBlockedLow,
        DataSize sourceBlockedHigh,
        boolean encryptionEnabled) {}
