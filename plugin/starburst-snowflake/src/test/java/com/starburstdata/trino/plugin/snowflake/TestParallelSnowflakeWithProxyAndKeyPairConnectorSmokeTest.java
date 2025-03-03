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

import io.trino.testing.QueryRunner;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.PRIVATE_KEY;

public class TestParallelSnowflakeWithProxyAndKeyPairConnectorSmokeTest
        extends TestParallelSnowflakeWithProxyConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(createProxyServer());
        return parallelBuilder()
                .withDatabase(Optional.of(getTestDatabase().getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withPrivateKey(Optional.of(PRIVATE_KEY))
                .withPassword(Optional.empty())
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of(
                        "snowflake.proxy.enabled", "true",
                        "snowflake.proxy.host", "localhost",
                        "snowflake.proxy.port", String.valueOf(getPort()),
                        "snowflake.proxy.protocol", "http",
                        "snowflake.proxy.username", PROXY_USER,
                        "snowflake.proxy.password", PROXY_PASSWORD))
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected int getPort()
    {
        return 8889;
    }
}
