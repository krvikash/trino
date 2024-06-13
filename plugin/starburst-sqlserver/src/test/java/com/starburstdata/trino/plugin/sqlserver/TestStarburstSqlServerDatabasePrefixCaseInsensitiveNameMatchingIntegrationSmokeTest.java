/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static java.util.Locale.ENGLISH;

public class TestStarburstSqlServerDatabasePrefixCaseInsensitiveNameMatchingIntegrationSmokeTest
        extends TestStarburstSqlServerDatabasePrefixIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServerDatabaseName = sqlServer.getDatabaseName().toLowerCase(ENGLISH);
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withConnectorProperties(ImmutableMap.of(
                        "sqlserver.database-prefix-for-schema.enabled", "true",
                        "case-insensitive-name-matching", "true"))
                .build();
    }
}
