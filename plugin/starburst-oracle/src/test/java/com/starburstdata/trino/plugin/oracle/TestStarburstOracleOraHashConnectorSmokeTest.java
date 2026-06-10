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
import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource;
import io.trino.testing.TestingConnectorBehavior;

import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.createStandardUsers;

public class TestStarburstOracleOraHashConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    private SharedResource.Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        return OracleQueryRunner.builder(oracleServer)
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.parallelism-type", "ORA_HASH")
                        .put("oracle.parallel.max-splits-per-scan", "4")
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .withCreateUsers(() -> createStandardUsers(oracleServer.get()))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN, SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
