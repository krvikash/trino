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

public record SinkBenchmarkSetup(
        int concurrency,
        int outputPartitionsCount,
        DataSize pageSize,
        DataSize dataSizePerWriter,
        DataSize sinkBlockedLow,
        DataSize sinkBlockedHigh) {}
