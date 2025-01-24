/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugin.stargate;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.POSTGRESQL_TPCH_SCHEMA;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateWithPostgreSqlConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private static final String REMOTE_CATALOG_NAME = "postgresql";
    private static final String CREATE_CATALOG_SQL_TEMPLATE =
            """
            CREATE CATALOG %s USING stargate
            WITH (
               "connection-url" = '%s',
               "connection-user" = '%s'
            )""";
    private TestingPostgreSqlServer postgreSqlServer;
    private DistributedQueryRunner remoteStarburst;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());

        remoteStarburst = closeAfterClass(StargateQueryRunner.createRemoteStarburstQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword()),
                REQUIRED_TPCH_TABLES,
                Optional.empty()));

        return StargateQueryRunner.builder(remoteStarburst, "postgresql")
                .build();
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN -> true;
            // Writes are not enabled
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_RENAME_TABLE -> false;
            // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/SEP-4795)
            case SUPPORTS_CREATE_VIEW -> false;
            // Writes are not enabled
            case SUPPORTS_INSERT,
                 SUPPORTS_DELETE,
                 SUPPORTS_MERGE,
                 SUPPORTS_UPDATE -> false;
            // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/SEP-4798)
            case SUPPORTS_ARRAY -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testCreateDropDynamicCatalog()
    {
        String catalog = "new_catalog_" + randomNameSuffix();
        @Language("SQL")
        String createCatalogSql = CREATE_CATALOG_SQL_TEMPLATE
                .formatted(catalog, stargateConnectionUrl(remoteStarburst, REMOTE_CATALOG_NAME), postgreSqlServer.getUser());
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "tpch");
        // re-add the same catalog
        assertUpdate(createCatalogSql);
        assertCatalogs("system", "tpch", catalog);

        assertUpdate("DROP CATALOG " + catalog);
        assertCatalogs("system", "tpch");
    }

    @Test
    void testCreateDropMultipleCatalogs()
    {
        String firstCatalog = "catalog1_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        try {
            @Language("SQL")
            String createFirstCatalogSql = CREATE_CATALOG_SQL_TEMPLATE
                    .formatted(firstCatalog, stargateConnectionUrl(remoteStarburst, REMOTE_CATALOG_NAME), postgreSqlServer.getUser());
            assertUpdate(createFirstCatalogSql);
            assertThat(computeScalar("SHOW CREATE CATALOG " + firstCatalog)).isEqualTo(createFirstCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(firstCatalog, POSTGRESQL_TPCH_SCHEMA));

            @Language("SQL")
            String createSecondCatalogSql =
                    """
                    CREATE CATALOG %s USING stargate
                    WITH (
                       "connection-url" = '%s',
                       "connection-user" = '%s',
                       "jdbc-types-mapped-to-varchar" = 'ARRAY'
                    )""".formatted(secondCatalog, stargateConnectionUrl(remoteStarburst, REMOTE_CATALOG_NAME), postgreSqlServer.getUser());
            assertUpdate(createSecondCatalogSql);
            assertThat(computeScalar("SHOW CREATE CATALOG " + secondCatalog)).isEqualTo(createSecondCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(secondCatalog, POSTGRESQL_TPCH_SCHEMA));
        }
        finally {
            assertUpdate("DROP CATALOG IF EXISTS " + firstCatalog);
            assertUpdate("DROP CATALOG IF EXISTS " + secondCatalog);
        }
    }
}
