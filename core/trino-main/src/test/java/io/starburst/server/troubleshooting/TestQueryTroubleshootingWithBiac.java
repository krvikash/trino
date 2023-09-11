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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.starburstdata.presto.biac.BiacSession;
import com.starburstdata.presto.biac.RbacBiacService;
import com.starburstdata.presto.biac.model.Entity;
import com.starburstdata.presto.biac.model.Grant;
import com.starburstdata.presto.biac.storage.BiacStorage;
import com.starburstdata.presto.server.StarburstQueryRunner;
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Map;

import static com.starburstdata.presto.biac.model.Action.SET;
import static com.starburstdata.presto.biac.model.Effect.ALLOW;
import static com.starburstdata.presto.biac.storage.BuiltinBiacStorage.PUBLIC_ROLE_ID;
import static com.starburstdata.presto.biac.storage.BuiltinBiacStorage.SYSTEM_ROLE;

public class TestQueryTroubleshootingWithBiac
        extends AbstractQueryTroubleshootingTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JdbcDatabaseContainer<?> container = closeAfterClass(new TestingEventLoggerPostgreSqlServer());

        DistributedQueryRunner queryRunner = StarburstQueryRunner.createStarburstQueryRunnerWithBiac(SESSION, container)
                .setCoordinatorProperties(Map.of("starburst.access-control.authorized-users", AUTHORIZED_USER, "troubleshooting.max-access-duration", "20s"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        RbacBiacService rbacBiacService = queryRunner.getCoordinator().getInstance(Key.get(RbacBiacService.class));
        BiacStorage biacStorage = queryRunner.getCoordinator().getInstance(Key.get(BiacStorage.class));
        BiacSession session = new BiacSession(rbacBiacService, Identity.ofUser(AUTHORIZED_USER));
        // troubleshooting needs to set some system session properties, but that's not allowed by default:
        biacStorage.createGrant(session, new Grant(PUBLIC_ROLE_ID, SET, Entity.allSystemSessionProperties(), ALLOW));

        return queryRunner;
    }

    @Override
    protected Identity getIdentityOfAuthorizedUser()
    {
        return Identity.forUser(AUTHORIZED_USER)
                .withEnabledRoles(ImmutableSet.of(SYSTEM_ROLE.get().getName()))
                .build();
    }
}
