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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

class RemovedSnowflakeJdbcConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "snowflake_jdbc";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        throw new RuntimeException(
                """
                The snowflake_jdbc connector is DEPRECATED.
                It will be removed in a future release.
                Please migrate to the snowflake_parallel connector.
                If you need to continue using the JDBC connector temporarily, set connector.name=deprecated_snowflake_jdbc.
                """);
    }
}
