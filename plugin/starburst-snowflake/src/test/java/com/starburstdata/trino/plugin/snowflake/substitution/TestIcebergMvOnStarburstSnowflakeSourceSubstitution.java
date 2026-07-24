/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.substitution;

import com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Requires real Snowflake credentials ({@code snowflake.test.server.*}); run in CI via the
 * connector's cloud test job, not in the default build.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnStarburstSnowflakeSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        return SnowflakeQueryRunner.parallelBuilder()
                .addCoordinatorProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // SnowflakeQueryRunner already provisions the source catalog, its schema, and a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("snowflake", "test_schema_2");
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
