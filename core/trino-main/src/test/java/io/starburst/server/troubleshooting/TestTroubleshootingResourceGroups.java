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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import com.starburstdata.presto.server.StarburstQueryRunner;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.configdump.ForResourceGroupConfigDump;
import com.starburstdata.presto.server.workload.resourcegroups.rest.BuiltinResourceGroupConfigurationSpecDto;
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.insights.InsightsTestUtils.authenticate;
import static com.starburstdata.presto.insights.InsightsTestUtils.jsonBody;
import static com.starburstdata.presto.protocol.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.assertPropertyExists;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.findConfigZips;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.getTroubleshootingDataForQuery;
import static com.starburstdata.presto.server.workload.resourcegroups.rest.BuiltinResourceGroupConfigurationSpecDto.configuration;
import static com.starburstdata.presto.server.workload.resourcegroups.rest.BuiltinResourceGroupSpecDto.Builder.resourceGroup;
import static com.starburstdata.presto.server.workload.resourcegroups.rest.BuiltinSelectorSpecDto.Builder.selector;
import static com.starburstdata.presto.testing.FileUtils.createTempFileForTesting;
import static io.airlift.json.JsonCodec.jsonCodec;
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
    public void testBuiltInResourceGroups()
            throws Exception
    {
        try (JdbcDatabaseContainer<?> container = new TestingEventLoggerPostgreSqlServer();
                DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                        .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                        .setInsightsProperties(container)
                        .setCoordinatorProperties(
                                ImmutableMap.<String, String>builder()
                                        .put("starburst.workload-manager.enabled", "true")
                                        .put("insights.authorized-users", AUTHORIZED_USER)
                                        .put("troubleshooting.max-access-duration", "20s")
                                        .buildOrThrow())
                        .build()) {
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("starburst", ImmutableMap.of());

            BuiltinResourceGroupConfigurationSpecDto configuration = configuration(
                    resourceGroup("first_group")
                            .setMaxQueued(100)
                            .setHardConcurrencyLimit(10)
                            .addSelector(selector().setUser(AUTHORIZED_USER).build())
                            .build(),
                    resourceGroup("second_group")
                            .setMaxQueued(100)
                            .setHardConcurrencyLimit(10)
                            .addSelector(selector().setUser("alice").build()).build());
            updateResourceGroupConfiguration(configuration, queryRunner);

            Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, SESSION, "SHOW CATALOGS", tmpDir);
            List<Unzipped> coordinatorConfigs = findConfigZips(inputsMap, "coordinator", tmpDir);
            assertThat(coordinatorConfigs.size()).isEqualTo(1);
            assertThat(coordinatorConfigs.getFirst().contents())
                    .hasEntrySatisfying(
                            "coordinator/builtin_resource_groups.json",
                            value -> assertThat(jsonCodec(BuiltinResourceGroupConfigurationSpecDto.class).fromJson(value)).isEqualTo(configuration));
        }
    }

    private void updateResourceGroupConfiguration(BuiltinResourceGroupConfigurationSpecDto configuration, DistributedQueryRunner queryRunner)
            throws IOException
    {
        OkHttpClient client = authenticate(AUTHORIZED_USER, queryRunner);
        Call call = client.newCall(
                new Request.Builder()
                        .url(queryRunner.getCoordinator().resolve("/ui/api/v1/wlm/configuration").toString())
                        .put(jsonBody(configuration, jsonCodec(BuiltinResourceGroupConfigurationSpecDto.class)))
                        .build());
        try (Response execute = call.execute()) {
            assertThat(execute.isSuccessful()).isTrue();
        }
    }

    @Test
    public void testFileBasedResourceGroups()
            throws Exception
    {
        File resourceGroupsConfigFile = new File(TestTroubleshootingResourceGroups.class.getClassLoader().getResource("resource_groups_config.json").toURI());
        File resourceGroupsPropertiesFile = createTempFileForTesting().toFile();
        byte[] resourceGroupsConfigFileContent = Files.readAllBytes(resourceGroupsConfigFile.toPath());
        try (OutputStream outputStream = new FileOutputStream(resourceGroupsPropertiesFile)) {
            outputStream.write("resource-groups.config-file".getBytes(ISO_8859_1));
            outputStream.write('=');
            outputStream.write(resourceGroupsConfigFile.getPath().getBytes(ISO_8859_1));
            outputStream.write('\n');
        }

        try (DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(SESSION)
                .addExtraProperty("troubleshooting.jfr.max-recording-size", "8MB")
                .setCoordinatorProperties(Map.of(
                        "insights.authorized-users", AUTHORIZED_USER,
                        "troubleshooting.max-access-duration", "20s"))
                .setAdditionalModule(binder -> newOptionalBinder(binder, Key.get(File.class, ForResourceGroupConfigDump.class))
                        .setBinding()
                        .toInstance(resourceGroupsPropertiesFile))
                .build()) {
            Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, SESSION, "SHOW CATALOGS", tmpDir);
            List<Unzipped> coordinatorConfigs = findConfigZips(inputsMap, "coordinator", tmpDir);
            assertThat(coordinatorConfigs.size()).isEqualTo(1);
            assertThat(coordinatorConfigs.getFirst().contents())
                    .hasEntrySatisfying(
                            "coordinator/%s".formatted(resourceGroupsPropertiesFile.getName()),
                            value -> assertPropertyExists(value, "resource-groups.config-file=%s".formatted(resourceGroupsConfigFile.getPath())))
                    .hasEntrySatisfying(
                            "coordinator/file_resource_groups.json",
                            value -> assertThat(value).isEqualTo(resourceGroupsConfigFileContent));
        }
    }
}
