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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.starburstdata.presto.biac.BiacSession;
import com.starburstdata.presto.biac.RbacBiacService;
import com.starburstdata.presto.biac.model.Entity;
import com.starburstdata.presto.biac.model.Grant;
import com.starburstdata.presto.biac.storage.BiacStorage;
import com.starburstdata.presto.server.StarburstQueryRunner;
import com.starburstdata.presto.server.diagnostics.BiacDiagnostics;
import com.starburstdata.presto.server.diagnostics.BiacStoragePayload;
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.biac.model.Action.SET;
import static com.starburstdata.presto.biac.model.Effect.ALLOW;
import static com.starburstdata.presto.biac.storage.BuiltinBiacStorage.PUBLIC_ROLE_ID;
import static com.starburstdata.presto.biac.storage.BuiltinBiacStorage.SYSTEM_ROLE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTroubleshootingWithBiac
        extends AbstractQueryTroubleshootingTest
{
    private BiacDiagnostics biacDiagnostics;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JdbcDatabaseContainer<?> container = closeAfterClass(new TestingEventLoggerPostgreSqlServer());

        DistributedQueryRunner queryRunner = StarburstQueryRunner.createStarburstQueryRunnerWithBiac(SESSION, container)
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of("starburst.access-control.authorized-users", AUTHORIZED_USER, "troubleshooting.max-access-duration", "20s"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new GeoPlugin());
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.createCatalog("postgres", "postgresql", POSTGRES_CATALOG_PROPERTIES);

        biacDiagnostics = queryRunner.getCoordinator().getInstance(Key.get(BiacDiagnostics.class));
        RbacBiacService rbacBiacService = queryRunner.getCoordinator().getInstance(Key.get(RbacBiacService.class));
        BiacStorage biacStorage = queryRunner.getCoordinator().getInstance(Key.get(BiacStorage.class));
        BiacSession session = new BiacSession(rbacBiacService, Identity.ofUser(AUTHORIZED_USER));
        // troubleshooting needs to set some system session properties, but that's not allowed by default:
        biacStorage.createGrant(session, new Grant(PUBLIC_ROLE_ID, SET, Entity.allSystemSessionProperties(), ALLOW));

        return queryRunner;
    }

    @Override
    protected List<Identity> getIdentitiesOfAuthorizedUsers()
    {
        return ImmutableList.of(
                Identity.forUser(AUTHORIZED_USER)
                        .withEnabledRoles(ImmutableSet.of(SYSTEM_ROLE.get().getName()))
                        .build());
    }

    @Override
    protected List<Identity> getIdentitiesOfUnauthorizedUsers()
    {
        return ImmutableList.of(
                // unauthorized, obviously:
                Identity.ofUser(NOT_AUTHORIZED_USER),
                // authorized, but with sysadmin role not enabled:
                Identity.ofUser(AUTHORIZED_USER));
    }

    @Override
    protected void assertAccessControlConfig(Unzipped coordinatorConfig)
    {
        BiacStoragePayload expected = biacDiagnostics.getStorage();
        assertThat(coordinatorConfig.contents())
                .hasEntrySatisfying(
                        "coordinator/biac.json",
                        value -> assertThat(jsonCodec(BiacStoragePayload.class).fromJson(value))
                                .usingRecursiveComparison()
                                .ignoringCollectionOrder()
                                .ignoringFields("timestamp") // This field is set to the current timestamp every time BiacDiagnostics.getStorage is called.
                                .isEqualTo(expected));
    }
}
