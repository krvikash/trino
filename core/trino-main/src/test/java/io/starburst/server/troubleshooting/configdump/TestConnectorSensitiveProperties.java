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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.AnnotationParameterValueList;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.trino.server.PluginLoader;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.server.troubleshooting.configdump.ConnectorSensitiveProperties.SENSITIVE_PROPERTIES_PER_CONNECTOR;
import static com.starburstdata.presto.testing.FileUtils.findRepositoryRoot;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectorSensitiveProperties
{
    @Test
    public void testSensitivePropertySetIsComplete()
            throws IOException
    {
        Map<String, Set<String>> expectedPropertiesPerConnector = findSensitivePropertiesPerConnector();

        Set<String> actualConnectors = SENSITIVE_PROPERTIES_PER_CONNECTOR.keySet();
        Set<String> expectedConnectors = expectedPropertiesPerConnector.keySet();
        Set<String> connectorsOnlyInActualSet = Sets.difference(expectedConnectors, actualConnectors);
        Set<String> connectorsOnlyInExpectedSet = Sets.difference(actualConnectors, expectedConnectors);
        assertThat(connectorsOnlyInActualSet)
                .withFailMessage("Missing connectors in the current set: %s. Consider updating the set with:\n%s",
                        connectorsOnlyInActualSet, buildPropertyDefinitions(connectorsOnlyInActualSet, expectedPropertiesPerConnector))
                .isEmpty();
        assertThat(connectorsOnlyInExpectedSet)
                .withFailMessage("Unexpected connectors in the current set: %s", connectorsOnlyInExpectedSet)
                .isEmpty();
        for (Map.Entry<String, Set<String>> connectorProperties : expectedPropertiesPerConnector.entrySet()) {
            String connectorName = connectorProperties.getKey();
            Set<String> expectedProperties = connectorProperties.getValue();
            Set<String> actualProperties = SENSITIVE_PROPERTIES_PER_CONNECTOR.get(connectorName);
            assertThat(actualProperties)
                    .withFailMessage("Current sensitive property set for the %s connector is different than expected. Consider updating the set with:\n%s",
                            connectorName, buildPropertyDefinitions(ImmutableSet.of(connectorName), expectedPropertiesPerConnector))
                    .isEqualTo(expectedProperties);
        }
    }

    private static Map<String, Set<String>> findSensitivePropertiesPerConnector()
            throws IOException
    {
        Map<String, Set<String>> sensitiveProperties = new HashMap<>();
        File pluginsDir = prepareInstalledPluginsDir();
        List<Plugin> plugins = PluginLoader.loadPlugins(pluginsDir);
        for (Plugin plugin : plugins) {
            for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
                String connectorName = connectorFactory.getName();
                ClassLoader classLoader = connectorFactory.getClass().getClassLoader();
                Set<String> properties = findSensitiveProperties(classLoader);
                checkState(sensitiveProperties.putIfAbsent(connectorName, properties) == null, "Multiple connectors with the name \"%s\".", connectorName);
            }
        }
        return sensitiveProperties;
    }

    private static Set<String> findSensitiveProperties(ClassLoader classLoader)
    {
        try (ScanResult scanResult = new ClassGraph()
                .overrideClassLoaders(classLoader)
                .enableAllInfo()
                .scan()) {
            return scanResult.getClassesWithMethodAnnotation(ConfigSecuritySensitive.class).stream()
                    .flatMap(classInfo -> classInfo.getMethodInfo().stream())
                    .filter(methodInfo -> methodInfo.hasAnnotation(ConfigSecuritySensitive.class))
                    .map(methodInfo -> {
                        AnnotationInfo annotationInfo = methodInfo.getAnnotationInfo(Config.class);
                        checkState(annotationInfo != null, "Missing @Config annotation for %s", methodInfo);
                        AnnotationParameterValueList parameterValues = annotationInfo.getParameterValues();
                        checkState(parameterValues.size() == 1, "Expected exactly one parameter for %s", annotationInfo);
                        return (String) parameterValues.getFirst().getValue();
                    })
                    .collect(toImmutableSet());
        }
    }

    private static File prepareInstalledPluginsDir()
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = Resources.getResource("trino-dependency-version.properties").openStream()) {
            properties.load(inputStream);
        }
        String sepVersion = properties.getProperty("project.version");

        File rootDir = findRepositoryRoot().toFile();
        File pluginDir = new File(rootDir, "/core/starburst-enterprise/target/starburst-enterprise-" + sepVersion + "-hardlinks/plugin");
        checkState(pluginDir.exists(), "The \"plugin\" directory does not exist: %s. " +
                "Before running this test the project has to be built so that the final .tar.gz, produced by the \"provisio:provision\" Maven goal, is available.",
                pluginDir.getAbsolutePath());
        return pluginDir;
    }

    private static String buildPropertyDefinitions(Set<String> connectors, Map<String, Set<String>> expectedPropertiesPerConnector)
    {
        return connectors.stream()
                .sorted()
                .map(connectorName -> {
                    Set<String> properties = expectedPropertiesPerConnector.get(connectorName);
                    String propertiesDefinition = properties.stream()
                            .sorted()
                            .map("                            \"%s\""::formatted)
                            .collect(joining(",\n"));
                    return """
                                        .put("%s",
                                                ImmutableSet.of(
                                                        %s))
                            """.formatted(connectorName, propertiesDefinition.trim());
                })
                .collect(joining());
    }
}
