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

import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.PRIVATE_KEY_2;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.PRIVATE_KEY_2_PASSPHRASE;

public class TestParallelSnowflakeKeyPairWithPassphraseAuthenticationConnectorSmokeTest
        extends BaseSnowflakeConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return parallelBuilder()
                .withDatabase(Optional.of(getTestDatabase().getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withPrivateKey(Optional.of(PRIVATE_KEY_2))
                .withPrivateKeyPassphrase(Optional.of(PRIVATE_KEY_2_PASSPHRASE))
                .withPassword(Optional.empty())
                .withConnectorProperties(impersonationDisabled())
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
