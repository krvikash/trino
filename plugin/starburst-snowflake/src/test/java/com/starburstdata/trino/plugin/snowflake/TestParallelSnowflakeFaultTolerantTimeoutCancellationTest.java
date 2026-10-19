/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import java.util.Map;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;

/**
 * The fault-tolerant scheduler drives split generation and the cleanup of a canceled query from a single thread, so a
 * split source blocking that thread until the Snowflake query completes keeps both the Snowflake and the Trino query running.
 */
public class TestParallelSnowflakeFaultTolerantTimeoutCancellationTest
        extends BaseSnowflakeTimeoutCancellationTest
{
    @Override
    protected SnowflakeQueryRunner.Builder getSnowflakeQueryRunnerBuilder()
    {
        return parallelBuilder()
                .addExtraProperties(Map.of("retry-policy", "TASK"))
                .withExchange("filesystem");
    }
}
