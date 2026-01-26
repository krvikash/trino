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

import com.google.inject.Binder;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.configdump.ForResourceGroupConfigDump;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.starburst.server.troubleshooting.DistributedTroubleshootingTestHelper.getTroubleshootingDataForQuery;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findConfigZips;
import static io.trino.client.AdditionalClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.testing.FileUtils.createTempFileForTesting;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTroubleshootingResourceGroups
{
    private static final String AUTHORIZED_USER = "bob";
    private static final Session SESSION = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setIdentity(Identity.ofUser(AUTHORIZED_USER))
            .setCatalog("tpch")
            .build();

    @TempDir
    private Path tmpDir;

    @Test
    public void testFileBasedResourceGroups()
            throws Exception
    {
        Path resourceGroupsConfigFile = Path.of(TestTroubleshootingResourceGroups.class.getClassLoader().getResource("resource_groups_config_simple.json").toURI());
        Path resourceGroupsPropertiesFile = createTempFileForTesting();
        byte[] resourceGroupsConfigFileContent = Files.readAllBytes(resourceGroupsConfigFile);
        try (OutputStream outputStream = Files.newOutputStream(resourceGroupsPropertiesFile)) {
            outputStream.write("resource-groups.config-file".getBytes(ISO_8859_1));
            outputStream.write('=');
            outputStream.write(resourceGroupsConfigFile.toString().getBytes(ISO_8859_1));
            outputStream.write('\n');
        }

        try (DistributedQueryRunner queryRunner = TpchQueryRunner.builder()
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of("troubleshooting.max-access-duration", "20s"))
                .setAdditionalModule(new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        newOptionalBinder(binder, Key.get(Path.class, ForResourceGroupConfigDump.class))
                                .setBinding()
                                .toInstance(resourceGroupsPropertiesFile);
                        binder.bind(TroubleshootingAccessControl.class)
                                .toInstance(identity -> AUTHORIZED_USER.equals(identity.getUser()));
                        install(new TroubleshootingModule());
                    }
                })
                .build()) {
            Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, SESSION, "SHOW CATALOGS", tmpDir);
            List<Unzipped> coordinatorConfigs = findConfigZips(inputsMap, "coordinator", tmpDir);
            assertThat(coordinatorConfigs.size()).isEqualTo(1);
            assertThat(coordinatorConfigs.getFirst().contents())
                    .hasEntrySatisfying(
                            "coordinator/%s".formatted(resourceGroupsPropertiesFile.getFileName()),
                            value -> assertPropertyExists(value, "resource-groups.config-file=%s".formatted(resourceGroupsConfigFile)))
                    .hasEntrySatisfying(
                            "coordinator/file_resource_groups.json",
                            value -> assertThat(value).isEqualTo(resourceGroupsConfigFileContent));
        }
    }
}
