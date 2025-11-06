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
import com.google.inject.Key;
import com.starburstdata.presto.server.StarburstQueryRunner;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.configdump.ForAccessControlConfigDump;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
import static com.starburstdata.presto.testing.FileUtils.createTempFileForTesting;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;

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

    private Path accessControlPropertiesFile;
    private Path accessControlRulesFile;
    private byte[] rulesFileContent;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        accessControlRulesFile = Paths.get(TestQueryTroubleshooting.class.getClassLoader().getResource("file-access/access-control-rules.json").toURI());
        accessControlPropertiesFile = createTempFileForTesting();
        rulesFileContent = Files.readAllBytes(accessControlRulesFile);
        try (OutputStream outputStream = Files.newOutputStream(accessControlPropertiesFile)) {
            outputStream.write("access-control.name=file".getBytes(ISO_8859_1));
            outputStream.write('\n');
            outputStream.write("security.config-file".getBytes(ISO_8859_1));
            outputStream.write('=');
            outputStream.write(accessControlRulesFile.toString().getBytes(ISO_8859_1));
            outputStream.write('\n');
        }

        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of(
                        "insights.authorized-users", AUTHORIZED_USER,
                        "insights.authorized-groups", String.join(",", USER_GROUPS.get(USER_WITHIN_AUTHORIZED_GROUP)),
                        "troubleshooting.max-access-duration", "20s"))
                .setAdditionalModule(binder -> newOptionalBinder(binder, Key.get(Path.class, ForAccessControlConfigDump.class))
                        .setBinding()
                        .toInstance(accessControlPropertiesFile))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new GeoPlugin());
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

    @Override
    protected void assertAccessControlConfig(Unzipped coordinatorConfig)
    {
        assertThat(coordinatorConfig.contents())
                .hasEntrySatisfying(
                        "coordinator/file_access_control.properties",
                        value -> assertPropertyExists(value, "security.config-file=%s".formatted(accessControlRulesFile)))
                .hasEntrySatisfying(
                        "coordinator/file_access_control_rules.json",
                        value -> assertThat(value).isEqualTo(rulesFileContent));
    }
}
