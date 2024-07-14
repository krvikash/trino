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

import com.google.inject.Inject;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;
import io.airlift.configuration.ConfigurationInspector.ConfigAttribute;
import io.airlift.configuration.ConfigurationInspector.ConfigRecord;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.starburst.server.troubleshooting.configdump.ConnectorSensitiveProperties.SENSITIVE_PROPERTIES_PER_CONNECTOR;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ConfigDumper
{
    private static final File JVM_CONFIG_FILE = new File("etc/jvm.config");
    private static final byte[] SECURITY_SENSITIVE_PROPERTY_VALUE = "[REDACTED]".getBytes(ISO_8859_1);

    private final ConfigurationFactory configurationFactory;
    private final InternalNodeManager nodeManager;
    private final CatalogConfigProvider catalogConfigProvider;

    @Inject
    public ConfigDumper(
            ConfigurationFactory configurationFactory,
            InternalNodeManager nodeManager,
            CatalogConfigProvider catalogConfigProvider)
    {
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.catalogConfigProvider = requireNonNull(catalogConfigProvider, "catalogConfigProvider is null");
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
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to dump local configuration", e);
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private String createRootDirectoryName()
    {
        InternalNode currentNode = nodeManager.getCurrentNode();
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
        ZipEntry jvmConfig = new ZipEntry(directoryName + "/" + JVM_CONFIG_FILE.getName());
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
}
