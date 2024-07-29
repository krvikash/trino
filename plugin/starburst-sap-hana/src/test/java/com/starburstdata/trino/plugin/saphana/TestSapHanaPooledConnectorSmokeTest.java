/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

public class TestSapHanaPooledConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSapHanaServer server = closeAfterClass(TestingSapHanaServer.create());
        return SapHanaQueryRunner.builder(server)
                .addConnectorProperty("connection-pool.enabled", "true")
                .addExtraProperty("scale-writers", "false")
                .addExtraProperty("task.scale-writers.enabled", "false")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_ARRAY:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_MERGE:
            case SUPPORTS_ROW_LEVEL_UPDATE:
                return false;

            case SUPPORTS_UPDATE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
