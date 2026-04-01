/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.FixedSplitSource;

/**
 * Split source that executes a Snowflake query and returns splits based on the chunks of results returned.
 *
 * <p>TODO: implement ConnectorSplitSource directly instead of just being a FixedSplitSource
 */
public class SnowflakeParallelSplitSource
        extends FixedSplitSource
{
    SnowflakeParallelSplitSource(Iterable<ConnectorSplit> splits)
    {
        super(splits);
    }
}
