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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.AnnotationParameterValueList;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginLoader;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.server.troubleshooting.configdump.ConnectorSensitiveProperties.SENSITIVE_PROPERTIES_PER_CONNECTOR;
import static java.util.stream.Collectors.joining;

@ExtendWith(SoftAssertionsExtension.class)
public class TestConnectorSensitiveProperties
{
    @Test
    public void testSensitivePropertySetIsComplete(SoftAssertions softly)
            throws IOException
    {
        testSensitivePropertySetIsComplete(softly, SENSITIVE_PROPERTIES_PER_CONNECTOR, prepareInstalledPluginsDir());
    }

    public static void testSensitivePropertySetIsComplete(SoftAssertions softly, Map<String, Set<String>> sensitivePropertiesPerConnector, Path pluginsDir)
            throws IOException
    {
        Map<String, Set<String>> expectedPropertiesPerConnector = findSensitivePropertiesPerConnector(pluginsDir);

        Set<String> actualConnectors = sensitivePropertiesPerConnector.keySet();
        Set<String> expectedConnectors = expectedPropertiesPerConnector.keySet();
        Set<String> connectorsOnlyInActualSet = Sets.difference(expectedConnectors, actualConnectors);
        Set<String> connectorsOnlyInExpectedSet = Sets.difference(actualConnectors, expectedConnectors);
        softly.assertThat(connectorsOnlyInActualSet)
                .withFailMessage("Missing connectors in the current set: %s. Consider updating the set with:\n%s",
                        connectorsOnlyInActualSet, buildPropertyDefinitions(connectorsOnlyInActualSet, expectedPropertiesPerConnector))
                .isEmpty();
        softly.assertThat(connectorsOnlyInExpectedSet)
                .withFailMessage("Unexpected connectors in the current set: %s", connectorsOnlyInExpectedSet)
                .isEmpty();
        for (Map.Entry<String, Set<String>> connectorProperties : expectedPropertiesPerConnector.entrySet()) {
            String connectorName = connectorProperties.getKey();
            Set<String> expectedProperties = connectorProperties.getValue();
            Set<String> actualProperties = sensitivePropertiesPerConnector.get(connectorName);
            softly.assertThat(actualProperties)
                    .withFailMessage("Current sensitive property set for the %s connector is different than expected. Consider updating the set with:\n%s",
                            connectorName, buildPropertyDefinitions(ImmutableSet.of(connectorName), expectedPropertiesPerConnector))
                    .isEqualTo(expectedProperties);
        }
    }

    private static Map<String, Set<String>> findSensitivePropertiesPerConnector(Path pluginsDir)
            throws IOException
    {
        Map<String, Set<String>> sensitiveProperties = new HashMap<>();
        List<Plugin> plugins = PluginLoader.loadPlugins(ImmutableList.of(pluginsDir));
        for (Plugin plugin : plugins) {
            for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
                String connectorName = connectorFactory.getName();
                Set<Path> classpath = buildClasspath(connectorFactory);
                Set<String> properties = findSensitiveProperties(classpath);
                checkState(sensitiveProperties.putIfAbsent(connectorName, properties) == null, "Multiple connectors with the name \"%s\".", connectorName);
            }
        }
        return sensitiveProperties;
    }

    private static Set<Path> buildClasspath(ConnectorFactory connectorFactory)
            throws IOException
    {
        ClassLoader classLoader = connectorFactory.getClass().getClassLoader();
        if (!(classLoader instanceof PluginClassLoader pluginClassLoader)) {
            throw new UnsupportedOperationException("Unsupported classloader type: " + classLoader.getClass().getName());
        }
        ImmutableSet.Builder<Path> classpath = ImmutableSet.builder();
        List<Path> pluginClasspath = Arrays.stream(pluginClassLoader.getURLs())
                .map(url -> {
                    try {
                        return Path.of(url.toURI());
                    }
                    catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toImmutableList());
        classpath.addAll(pluginClasspath);
        if (!pluginClasspath.isEmpty()) {
            Path hdfsDirectory = pluginClasspath.getFirst().getParent().resolve("hdfs");
            if (Files.exists(hdfsDirectory) && Files.isDirectory(hdfsDirectory)) {
                try (Stream<Path> hdfsClasspath = Files.list(hdfsDirectory)) {
                    hdfsClasspath.forEach(classpath::add);
                }
            }
        }
        return classpath.build();
    }

    private static Set<String> findSensitiveProperties(Set<Path> classpath)
    {
        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        List<Path> annotations = classpath.stream()
                .filter(path -> path.getFileName().toString().startsWith("config-"))
                .toList();
        for (Path path : classpath) {
            try (ScanResult scanResult = new ClassGraph()
                    .overrideClasspath(ImmutableList.builder()
                            .addAll(annotations)
                            .add(path)
                            .build())
                    .enableAllInfo()
                    .scan()) {
                result.addAll(scanResult.getClassesWithMethodAnnotation(ConfigSecuritySensitive.class).stream()
                        .flatMap(classInfo -> classInfo.getMethodInfo().stream())
                        .filter(methodInfo -> methodInfo.hasAnnotation(ConfigSecuritySensitive.class))
                        .map(methodInfo -> {
                            AnnotationInfo annotationInfo = methodInfo.getAnnotationInfo(Config.class);
                            checkState(annotationInfo != null, "Missing @Config annotation for %s", methodInfo);
                            AnnotationParameterValueList parameterValues = annotationInfo.getParameterValues();
                            checkState(parameterValues.size() == 1, "Expected exactly one parameter for %s", annotationInfo);
                            return (String) parameterValues.getFirst().getValue();
                        })
                        .collect(toImmutableSet()));
            }
        }
        return result.build();
    }

    private static Path prepareInstalledPluginsDir()
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = TestConnectorSensitiveProperties.class.getResourceAsStream("/trino-testing.properties")) {
            properties.load(inputStream);
        }
        String trinoVersion = properties.getProperty("project.version");
        Path rootDir = findRepositoryRoot();
        Path pluginDir = rootDir.resolve("core/trino-server/target/trino-server-" + trinoVersion + "-hardlinks/plugin");
        checkState(Files.exists(pluginDir), "The \"plugin\" directory does not exist: %s. " +
                        "Before running this test the project has to be built so that the final .tar.gz, produced by the \"provisio:provision\" Maven goal, is available.",
                pluginDir.toAbsolutePath());
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

    private static Path findRepositoryRoot()
    {
        Path workingDirectory = Path.of("").toAbsolutePath();
        for (Path path = workingDirectory; path != null; path = path.getParent()) {
            if (Files.isDirectory(path.resolve(".git"))) {
                return path;
            }
        }
        throw new RuntimeException("Failed to find repository root from " + workingDirectory);
    }
}
