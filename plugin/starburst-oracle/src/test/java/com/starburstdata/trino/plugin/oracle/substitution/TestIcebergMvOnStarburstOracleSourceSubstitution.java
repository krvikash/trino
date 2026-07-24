/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle.substitution;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.oracle.OracleQueryRunner;
import com.starburstdata.trino.plugin.oracle.TestingStarburstOracleServer;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource.Lease;
import org.junit.jupiter.api.parallel.Execution;

import static com.starburstdata.trino.plugin.oracle.OracleTestUsers.USER;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnStarburstOracleSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        Lease<TestingStarburstOracleServer> oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        // Starburst OracleQueryRunner builds a licensed oracle catalog (+ tpch, jmx) with the
        // standard test users; the session runs as ALICE against the presto_test_user schema.
        return OracleQueryRunner.builder(oracleServer)
                .withCoordinatorProperties(ImmutableMap.of("materialized-view-substitution.support.enabled", "true"))
                .build();
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("oracle", USER);
    }

    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }
}
