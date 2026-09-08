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
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.configdump.ForAccessControlConfigDump;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
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

    private Path accessControlRulesFile1;
    private Path accessControlRulesFile2;
    private byte[] rulesFileContent1;
    private byte[] rulesFileContent2;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        accessControlRulesFile1 = Path.of(TestQueryTroubleshooting.class.getClassLoader().getResource("file-access/access-control-rules.json").toURI());
        accessControlRulesFile2 = Path.of(TestQueryTroubleshooting.class.getClassLoader().getResource("file-access/access-control-rules-2.json").toURI());
        rulesFileContent1 = Files.readAllBytes(accessControlRulesFile1);
        rulesFileContent2 = Files.readAllBytes(accessControlRulesFile2);

        Path accessControlPropertiesFile1 = writeAccessControlProperties(createTempDirectoryForTesting(), accessControlRulesFile1);
        Path accessControlPropertiesFile2 = writeAccessControlProperties(createTempDirectoryForTesting(), accessControlRulesFile2);

        DistributedQueryRunner queryRunner = TpchQueryRunner.builder()
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of(
                        "troubleshooting.max-access-duration", "20s"))
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        newOptionalBinder(binder, Key.get(new TypeLiteral<List<Path>>() {}, ForAccessControlConfigDump.class))
                                .setBinding()
                                .toInstance(ImmutableList.of(accessControlPropertiesFile1, accessControlPropertiesFile2));
                        binder.bind(TroubleshootingAccessControl.class)
                                .toInstance(identity -> AUTHORIZED_USER.equals(identity.getUser()) || identity.getGroups().contains(AUTHORIZED_GROUP));
                        install(new TroubleshootingModule());
                    }
                })
                .build();

        queryRunner.installPlugin(new GeoPlugin());
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("postgres", "postgresql", POSTGRES_CATALOG_PROPERTIES);

        GroupProvider groupProvider = user -> USER_GROUPS.getOrDefault(user, ImmutableSet.of());
        queryRunner.getGroupProvider().setConfiguredGroupProvider(groupProvider);

        return queryRunner;
    }

    private static Path writeAccessControlProperties(Path directory, Path rulesFile)
            throws Exception
    {
        Path propertiesFile = directory.resolve("access-control.properties");
        try (OutputStream outputStream = Files.newOutputStream(propertiesFile)) {
            outputStream.write("access-control.name=file".getBytes(ISO_8859_1));
            outputStream.write('\n');
            outputStream.write("security.config-file".getBytes(ISO_8859_1));
            outputStream.write('=');
            outputStream.write(rulesFile.toString().getBytes(ISO_8859_1));
            outputStream.write('\n');
        }
        return propertiesFile;
    }

    private static Path createTempDirectoryForTesting()
            throws IOException
    {
        Path directory = Files.createTempDirectory(null);
        directory.toFile().deleteOnExit();
        return directory;
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
                        "coordinator/access_control/access-control.properties",
                        value -> assertPropertyExists(value, "security.config-file=%s".formatted(accessControlRulesFile1)))
                .hasEntrySatisfying(
                        "coordinator/access_control/access-control-rules.json",
                        value -> assertThat(value).isEqualTo(rulesFileContent1))
                .hasEntrySatisfying(
                        "coordinator/access_control/access-control_2.properties",
                        value -> assertPropertyExists(value, "security.config-file=%s".formatted(accessControlRulesFile2)))
                .hasEntrySatisfying(
                        "coordinator/access_control/access-control-rules-2.json",
                        value -> assertThat(value).isEqualTo(rulesFileContent2));
    }
}
