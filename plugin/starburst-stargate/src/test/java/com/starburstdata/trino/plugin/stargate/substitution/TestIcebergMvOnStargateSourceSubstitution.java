/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.stargate.StargateQueryRunner;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnStargateSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        // Stargate reads from a remote Starburst cluster; use its writable in-memory catalog as the
        // base-table store. The local runner exposes it through the p2p_remote stargate catalog.
        DistributedQueryRunner remoteStarburst = closeAfterClass(
                createRemoteStarburstQueryRunnerWithMemory(ImmutableList.of(), Optional.empty()));
        return StargateQueryRunner.builder(remoteStarburst, "memory")
                .enableWrites()
                .withCoordinatorProperties(ImmutableMap.of("materialized-view-substitution.support.enabled", "true"))
                .build();
    }

    // StargateQueryRunner exposes the remote memory catalog as p2p_remote (schema tiny) and installs a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("p2p_remote", "tiny");
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
