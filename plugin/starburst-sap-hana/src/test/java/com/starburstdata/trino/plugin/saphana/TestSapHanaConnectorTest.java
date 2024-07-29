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

import io.trino.testing.QueryRunner;

public class TestSapHanaConnectorTest
        extends BaseSapHanaConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(TestingSapHanaServer.create());
        return SapHanaQueryRunner.builder(server)
                .addConnectorProperty("metadata.cache-ttl", "0m")
                .addConnectorProperty("metadata.cache-missing", "false")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
