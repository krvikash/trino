/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana.substitution;

import com.starburstdata.trino.plugin.saphana.SapHanaQueryRunner;
import com.starburstdata.trino.plugin.saphana.TestingSapHanaServer;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Requires the SAP HANA test container (a private image pulled from ECR); runs in CI's SAP HANA job.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnSapHanaSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingSapHanaServer sapHanaServer = closeAfterClass(TestingSapHanaServer.create());
        return SapHanaQueryRunner.builder(sapHanaServer)
                .addCoordinatorProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // SapHanaQueryRunner already provisions the source catalog, its tpch schema, and a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("saphana", "tpch");
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
