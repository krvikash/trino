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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.server.StarburstQueryRunner;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestQueryTroubleshooting
        extends AbstractQueryTroubleshootingTest
{
    protected static final String USER_WITHIN_AUTHORIZED_GROUP = "charlie";
    protected static final String AUTHORIZED_GROUP = "groupA";

    protected static final String USER_WITHIN_UNAUTHORIZED_GROUP = "alice";
    protected static final String UNAUTHORIZED_GROUP = "groupB";

    protected static final Map<String, Set<String>> USER_GROUPS = ImmutableMap.of(
            USER_WITHIN_AUTHORIZED_GROUP, ImmutableSet.of(AUTHORIZED_GROUP),
            USER_WITHIN_UNAUTHORIZED_GROUP, ImmutableSet.of(UNAUTHORIZED_GROUP));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of(
                        "insights.authorized-users", AUTHORIZED_USER,
                        "insights.authorized-groups", String.join(",", USER_GROUPS.get(USER_WITHIN_AUTHORIZED_GROUP)),
                        "troubleshooting.max-access-duration", "20s"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.createCatalog("postgres", "postgresql", POSTGRES_CATALOG_PROPERTIES);

        GroupProvider groupProvider = user -> USER_GROUPS.getOrDefault(user, ImmutableSet.of());
        queryRunner.getGroupProvider().setConfiguredGroupProvider(groupProvider);

        return queryRunner;
    }

    @Override
    protected List<Identity> getIdentitiesOfAuthorizedUsers()
    {
        return ImmutableList.<Identity>builder()
                .addAll(super.getIdentitiesOfAuthorizedUsers())
                .add(Identity.ofUser(USER_WITHIN_AUTHORIZED_GROUP))
                .build();
    }

    @Override
    protected List<Identity> getIdentitiesOfUnauthorizedUsers()
    {
        return ImmutableList.<Identity>builder()
                .addAll(super.getIdentitiesOfUnauthorizedUsers())
                .add(Identity.ofUser(USER_WITHIN_UNAUTHORIZED_GROUP))
                .build();
    }
}
