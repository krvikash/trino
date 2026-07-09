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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.ALICE_USER;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.createSessionForUser;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestParallelSnowflakeWithFixedRole
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestDatabase testDB = closeAfterClass(SnowflakeServer.createTestDatabase());
        return parallelBuilder()
                .withConnectorProperties(impersonationDisabled())
                .withDatabase(Optional.of(testDB.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withCreateUserContextView()
                .build();
    }

    @Test
    public void testUsersAreNotImpersonated()
    {
        assertQuery(
                createSessionForUser(ALICE_USER),
                "SELECT * FROM public.user_context",
                format("VALUES ('%s', '%s')", USER.toUpperCase(ENGLISH), ROLE.toUpperCase(ENGLISH)));
    }
}
