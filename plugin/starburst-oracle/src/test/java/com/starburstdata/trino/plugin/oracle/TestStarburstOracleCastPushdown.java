/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.oracle.TestOracleCastPushdown;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStarburstOracleCastPushdown
        extends TestOracleCastPushdown
{
    private SharedResource.Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        return com.starburstdata.trino.plugin.oracle.OracleQueryRunner.builder(oracleServer)
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.remarks-reporting.enabled", "true")
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        .put("join-pushdown.strategy", "EAGER")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return oracleServer.get().getSqlExecutor();
    }
}
