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

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;

final class TestJdbcSnowflakeQueryCommentFormat
        extends BaseSnowflakeQueryCommentFormat
{
    @Override
    protected SnowflakeQueryRunner.Builder getSnowflakeQueryRunnerBuilder()
    {
        return jdbcBuilder();
    }
}
