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

import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.PASSWORD;
import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.USER;
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
        QueryRunner queryRunner = OracleQueryRunner.builder(oracleServer)
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.remarks-reporting.enabled", "true")
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        .put("join-pushdown.strategy", "EAGER")
                        // Set oracle.number.default-scale=s to map Oracle NUMBER (without precision/scale) to DECIMAL(38, s)
                        .put("oracle.number.default-scale", "2")
                        .buildOrThrow())
                .build();

        queryRunner.createCatalog(
                "oracle_number_mapped_to_varchar",
                "oracle",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.get().getJdbcUrl())
                        .put("connection-user", USER)
                        .put("connection-password", PASSWORD)
                        .put("unsupported-type-handling", "CONVERT_TO_VARCHAR")
                        .put("join-pushdown.enabled", "true")
                        .buildOrThrow());
        return queryRunner;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return oracleServer.get().getSqlExecutor();
    }
}
