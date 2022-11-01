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

import java.time.Duration;

public record SourceBenchmarkResult(
        DataSize totalDataRead,
        Duration runtime,
        DataSize throughput,
        Long dummyHash) {}
