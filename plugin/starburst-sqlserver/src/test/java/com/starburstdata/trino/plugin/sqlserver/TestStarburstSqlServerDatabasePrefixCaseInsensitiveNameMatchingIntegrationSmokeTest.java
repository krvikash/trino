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
        this.sqlServer.execute("CREATE LOGIN %s WITH PASSWORD = '%s'".formatted(ANOTHER_USER, ANOTHER_PASSWORD));
        this.sqlServer.execute("CREATE USER %1$s FROM LOGIN %1$s".formatted(ANOTHER_USER));
        this.sqlServer.execute("GRANT CONTROL ON DATABASE::%s TO %s".formatted(sqlServer.getDatabaseName(), ANOTHER_USER));
        sqlServerDatabaseName = sqlServer.getDatabaseName().toLowerCase(ENGLISH);
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withConnectorProperties(ImmutableMap.of(
                        "connection-user", ANOTHER_USER,
                        "connection-password", ANOTHER_PASSWORD,
                        "sqlserver.database-prefix-for-schema.enabled", "true",
                        "case-insensitive-name-matching", "true"))
                .build();
    }
}
