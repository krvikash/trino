/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.synapse.SynapseServer;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static com.starburstdata.trino.plugin.synapse.SynapseQueryRunner.DEFAULT_CATALOG_NAME;
import static com.starburstdata.trino.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Requires a real Azure Synapse instance ({@code test.synapse.jdbc.*}); run in CI, not in the
 * default build.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnSynapseSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                ImmutableMap.of("materialized-view-substitution.support.enabled", "true"),
                synapseServer,
                DEFAULT_CATALOG_NAME,
                ImmutableMap.of(),
                ImmutableList.of());
    }

    // Synapse uses a random per-run schema and installs its own tpch catalog; read the actual source
    // schema from the runner session rather than assuming a fixed name.
    @Override
    protected String sourceSchemaName()
    {
        return getSession().getSchema().orElseThrow();
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
