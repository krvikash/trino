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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.PASSWORD;
import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.USER;
import static org.assertj.core.api.Assertions.assertThat;
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

    @Test
    @Override
    public void testJoinPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            String sql = "SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn());
            // Casting char to varchar keeps the char's trailing pad-spaces in Trino (NO PAD), but Oracle compares
            // varchar with PAD SPACE and ignores them. With EAGER join pushdown the join is still fully pushed down,
            // so keep the plan assertion and skip the results correctness check for that case.
            if (testCase.equals(new CastTestCase("c_char_50", "varchar(50)", "c_varchar_50"))
                    || testCase.equals(new CastTestCase("c_char_10", "varchar(50)", "c_varchar_50"))
                    || testCase.equals(new CastTestCase("c_nchar_10", "varchar(50)", "c_varchar_50"))) {
                assertThat(query(sql))
                        .skipResultsCorrectnessCheckForPushdown()
                        .isFullyPushedDown();
            }
            else {
                assertThat(query(sql))
                        .isFullyPushedDown();
            }
        }

        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .joinIsNotFullyPushedDown();
        }
    }
}
