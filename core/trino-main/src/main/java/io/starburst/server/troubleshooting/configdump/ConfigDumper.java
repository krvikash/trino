/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;
import io.airlift.configuration.ConfigurationInspector.ConfigAttribute;
import io.airlift.configuration.ConfigurationInspector.ConfigRecord;
import io.trino.node.InternalNode;
import io.trino.security.AccessControlConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.starburst.server.troubleshooting.configdump.ConnectorSensitiveProperties.SENSITIVE_PROPERTIES_PER_CONNECTOR;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ConfigDumper
{
    private static final String ACCESS_CONTROL_NAME_PROPERTY = "access-control.name";
    private static final Path JVM_CONFIG_FILE = Paths.get("etc", "jvm.config");
    private static final byte[] SECURITY_SENSITIVE_PROPERTY_VALUE = "[REDACTED]".getBytes(ISO_8859_1);

    private final ConfigurationFactory configurationFactory;
    private final InternalNode currentNode;
    private final CatalogConfigProvider catalogConfigProvider;
    private final Path resourceGroupsConfigFile;
    private final List<Path> accessControlConfigFiles;
    private final Set<BuiltInFeatureConfigDumper> builtInFeatureConfigDumpers;

    @Inject
    public ConfigDumper(
            ConfigurationFactory configurationFactory,
            InternalNode currentNode,
            CatalogConfigProvider catalogConfigProvider,
            @ForResourceGroupConfigDump Path resourceGroupsConfigFile,
            @ForAccessControlConfigDump Path defaultAccessControlConfigFile,
            AccessControlConfig accessControlConfig,
            Set<BuiltInFeatureConfigDumper> builtInFeatureConfigDumpers)
    {
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.catalogConfigProvider = requireNonNull(catalogConfigProvider, "catalogConfigProvider is null");
        this.resourceGroupsConfigFile = requireNonNull(resourceGroupsConfigFile, "resourceGroupsConfigFile is null");
        requireNonNull(accessControlConfig, "accessControlConfig is null");
        this.accessControlConfigFiles = resolveConfigFiles(accessControlConfig.getAccessControlFiles(), defaultAccessControlConfigFile);
        this.builtInFeatureConfigDumpers = requireNonNull(builtInFeatureConfigDumpers, "builtInFeatureConfigDumpers is null");
    }

    private static List<Path> resolveConfigFiles(List<File> configFiles, Path defaultConfigFile)
    {
        requireNonNull(defaultConfigFile, "defaultConfigFile is null");
        if (configFiles.isEmpty()) {
            if (Files.exists(defaultConfigFile)) {
                return ImmutableList.of(defaultConfigFile);
            }
        }
        return configFiles.stream().map(File::toPath).collect(toImmutableList());
    }

    public InputStream dumpLocalConfig()
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream)) {
            String directoryName = createRootDirectoryName();
            ZipEntry directory = new ZipEntry(directoryName + "/");
            zipOutputStream.putNextEntry(directory);

            dumpServerConfiguration(zipOutputStream, directoryName);
            dumpJvmConfig(zipOutputStream, directoryName);
            dumpCatalogConfigurations(zipOutputStream, directoryName);
            dumpFileBasedResourceGroupConfig(zipOutputStream, directoryName);
            dumpFileBasedAccessControlConfig(zipOutputStream, directoryName);
            dumpBuiltInFeatureConfigs(zipOutputStream, directoryName);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to dump local configuration", e);
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private String createRootDirectoryName()
    {
        if (currentNode.isCoordinator()) {
            return "coordinator";
        }
        return "worker-" + currentNode.getNodeIdentifier();
    }

    private void dumpServerConfiguration(ZipOutputStream outputStream, String directoryName)
            throws IOException
    {
        ConfigurationInspector configurationInspector = new ConfigurationInspector();
        ZipEntry configFile = new ZipEntry(directoryName + "/" + "config.properties");
        outputStream.putNextEntry(configFile);
        for (ConfigRecord<?> record : configurationInspector.inspect(configurationFactory)) {
            for (ConfigAttribute attribute : record.getAttributes()) {
                String propertyName = attribute.getPropertyName();
                outputStream.write(propertyName.getBytes(ISO_8859_1));
                outputStream.write('=');
                String currentValue = attribute.getCurrentValue();
                outputStream.write(currentValue.getBytes(ISO_8859_1));
                outputStream.write('\n');
            }
        }
    }

    private void dumpJvmConfig(ZipOutputStream outputStream, String directoryName)
            throws IOException
    {
        ZipEntry jvmConfig = new ZipEntry(directoryName + "/" + JVM_CONFIG_FILE.getFileName());
        outputStream.putNextEntry(jvmConfig);
        for (String jvmArgument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            outputStream.write(jvmArgument.getBytes(UTF_8));
            outputStream.write('\n');
        }
    }

    private void dumpCatalogConfigurations(ZipOutputStream outputStream, String directoryName)
    {
        Collection<CatalogConfig> propertiesPerCatalog = catalogConfigProvider.loadCatalogConfigs();
        for (CatalogConfig catalogConfig : propertiesPerCatalog) {
            String catalogName = catalogConfig.catalogName();
            ZipEntry catalogZipEntry = new ZipEntry("%s/catalog/%s.properties".formatted(directoryName, catalogName));
            try {
                outputStream.putNextEntry(catalogZipEntry);
                dumpCatalogProperties(outputStream, catalogConfig);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to dump configuration for the catalog \"%s\".".formatted(catalogName), e);
            }
        }
    }

    private void dumpCatalogProperties(ZipOutputStream outputStream, CatalogConfig catalogConfig)
            throws IOException
    {
        String connectorName = catalogConfig.connectorName();
        Set<String> sensitivePropertyNames = SENSITIVE_PROPERTIES_PER_CONNECTOR.getOrDefault(connectorName, Collections.emptySet());
        for (Map.Entry<String, String> property : catalogConfig.properties().entrySet()) {
            String propertyName = property.getKey();
            outputStream.write(propertyName.getBytes(ISO_8859_1));
            outputStream.write('=');
            if (isSecuritySensitiveProperty(propertyName, sensitivePropertyNames)) {
                outputStream.write(SECURITY_SENSITIVE_PROPERTY_VALUE);
            }
            else {
                String propertyValue = property.getValue();
                outputStream.write(propertyValue.getBytes(ISO_8859_1));
            }
            outputStream.write('\n');
        }
    }

    private static boolean isSecuritySensitiveProperty(String propertyName, Set<String> sensitivePropertyNames)
    {
        return sensitivePropertyNames.stream().anyMatch(propertyName::endsWith);
    }

    private void dumpFileBasedResourceGroupConfig(ZipOutputStream outputStream, String directoryName)
    {
        if (currentNode.isCoordinator()) {
            Map<String, String> properties = loadProperties(resourceGroupsConfigFile);
            dumpProperties(properties, resourceGroupsConfigFile.getFileName().toString(), outputStream, directoryName);
            String configFilePath = properties.get("resource-groups.config-file");
            if (configFilePath != null) {
                dumpFileIfExists(Paths.get(configFilePath), "file_resource_groups.json", outputStream, directoryName);
            }
        }
    }

    private void dumpFileBasedAccessControlConfig(ZipOutputStream outputStream, String directoryName)
    {
        for (Path configFile : accessControlConfigFiles) {
            Map<String, String> properties = loadProperties(configFile);

            String name = properties.get(ACCESS_CONTROL_NAME_PROPERTY);
            checkState(!isNullOrEmpty(name), "Configuration file '%s' does not contain property '%s'", configFile, ACCESS_CONTROL_NAME_PROPERTY);

            dumpProperties(properties, "%s_access_control.properties".formatted(name), outputStream, directoryName);
            String configFilePath = properties.get("security.config-file");
            if (configFilePath != null) {
                dumpFileIfExists(Paths.get(configFilePath), "%s_access_control_rules.json".formatted(name), outputStream, directoryName);
            }
        }
    }

    private void dumpBuiltInFeatureConfigs(ZipOutputStream outputStream, String directoryName)
    {
        for (BuiltInFeatureConfigDumper dumper : builtInFeatureConfigDumpers) {
            BuiltInFeatureConfigDump config = dumper.dumpConfig();
            ZipEntry zipEntry = new ZipEntry("%s/%s".formatted(directoryName, config.fileName()));
            try {
                outputStream.putNextEntry(zipEntry);
                outputStream.write(config.serializedConfig());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to dump \"%s\".".formatted(config.fileName()), e);
            }
        }
    }

    private static void dumpProperties(Map<String, String> properties, String fileName, ZipOutputStream outputStream, String directoryName)
    {
        if (properties.isEmpty()) {
            return;
        }
        ZipEntry configFile = new ZipEntry("%s/%s".formatted(directoryName, fileName));
        try {
            outputStream.putNextEntry(configFile);
            for (Map.Entry<String, String> property : properties.entrySet()) {
                String propertyName = property.getKey();
                outputStream.write(propertyName.getBytes(ISO_8859_1));
                outputStream.write('=');
                String currentValue = property.getValue();
                outputStream.write(currentValue.getBytes(ISO_8859_1));
                outputStream.write('\n');
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to dump \"%s\".".formatted(fileName), e);
        }
    }

    private static Map<String, String> loadProperties(Path file)
    {
        if (!Files.exists(file)) {
            return ImmutableMap.of();
        }
        try {
            return loadPropertiesFrom(file.toString());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to load \"%s\".".formatted(file), e);
        }
    }

    private static void dumpFileIfExists(Path configFile, String targetFileName, ZipOutputStream outputStream, String directoryName)
    {
        if (!Files.exists(configFile)) {
            return;
        }
        try {
            byte[] fileContent = Files.readAllBytes(configFile);
            ZipEntry zipEntry = new ZipEntry("%s/%s".formatted(directoryName, targetFileName));
            outputStream.putNextEntry(zipEntry);
            outputStream.write(fileContent);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to dump \"%s\".".formatted(configFile), e);
        }
    }
}
