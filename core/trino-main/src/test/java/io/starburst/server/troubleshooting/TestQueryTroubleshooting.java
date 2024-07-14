/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.starburstdata.presto.server.StarburstQueryRunner;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

public class TestQueryTroubleshooting
        extends AbstractQueryTroubleshootingTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of("insights.authorized-users", AUTHORIZED_USER, "troubleshooting.max-access-duration", "20s"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.createCatalog("postgres", "postgresql", POSTGRES_CATALOG_PROPERTIES);

        return queryRunner;
    }
}
