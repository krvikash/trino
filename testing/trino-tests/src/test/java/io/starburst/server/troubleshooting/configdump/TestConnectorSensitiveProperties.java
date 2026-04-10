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
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginLoader;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.troubleshooting.configdump.ConnectorSensitiveProperties.SENSITIVE_PROPERTIES_PER_CONNECTOR;
import static java.util.stream.Collectors.joining;

@ExtendWith(SoftAssertionsExtension.class)
public class TestConnectorSensitiveProperties
{
    @Test
    public void testSensitivePropertiesSorted()
    {
        // Keeping properties sorted ensures that subsequent updates do not include spurious changes.

        Iterator<String> keySet = SENSITIVE_PROPERTIES_PER_CONNECTOR.keySet().iterator();
        String previous = keySet.next();
        while (keySet.hasNext()) {
            String next = keySet.next();
            checkState(
                    previous.compareTo(next) < 0,
                    "SENSITIVE_PROPERTIES_PER_CONNECTOR key set is not sorted at %s >= %s",
                    previous, next);
            previous = next;
        }

        SENSITIVE_PROPERTIES_PER_CONNECTOR.forEach((connector, properties) -> {
            if (!Ordering.natural().isStrictlyOrdered(properties)) {
                throw new IllegalStateException(
                        "The sensitive properties for the %s connector are not sorted. Consider sorting them with:\n%s".formatted(
                                connector, buildPropertyDefinitions(ImmutableSet.of(connector), Map.of(connector, properties))));
            }
        });
    }

    @Test
    public void testSensitivePropertySetIsComplete(SoftAssertions softly)
            throws IOException
    {
        testSensitivePropertySetIsComplete(softly, SENSITIVE_PROPERTIES_PER_CONNECTOR, prepareInstalledPluginsDir());
    }

    public static void testSensitivePropertySetIsComplete(SoftAssertions softly, Map<String, Set<String>> sensitivePropertiesPerConnector, Path pluginsDir)
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
    {
        Map<String, Set<String>> sensitiveProperties = new ConcurrentHashMap<>();
        List<Plugin> plugins = PluginLoader.loadPlugins(ImmutableList.of(pluginsDir));
        plugins.stream()
                .parallel()
                .forEach(plugin -> {
                    try {
                        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
                            String connectorName = connectorFactory.getName();
                            Set<Path> classpath = buildClasspath(connectorFactory);
                            Set<String> properties = findSensitiveProperties(classpath);
                            checkState(sensitiveProperties.putIfAbsent(connectorName, properties) == null, "Multiple connectors with the name \"%s\".", connectorName);
                        }
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
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
            throws IOException
    {
        Set<String> sensitiveProperties = new HashSet<>();
        for (Path path : classpath) {
            if (Files.isDirectory(path)) {
                scanDirectory(path, sensitiveProperties);
            }
            else if (path.toString().endsWith(".jar")) {
                scanJarFile(path, sensitiveProperties);
            }
        }
        return ImmutableSet.copyOf(sensitiveProperties);
    }

    private static void scanDirectory(Path directory, Set<String> sensitiveProperties)
            throws IOException
    {
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.filter(path -> path.toString().endsWith(".class"))
                    .forEach(classFile -> {
                        try {
                            scanClassFile(classFile, sensitiveProperties);
                        }
                        catch (IOException e) {
                            throw new RuntimeException("Failed to scan class file: " + classFile, e);
                        }
                    });
        }
    }

    private static void scanJarFile(Path jarPath, Set<String> sensitiveProperties)
            throws IOException
    {
        try (InputStream fileInputStream = Files.newInputStream(jarPath);
                JarInputStream jarInputStream = new JarInputStream(fileInputStream)) {
            JarEntry jarEntry;
            while ((jarEntry = jarInputStream.getNextJarEntry()) != null) {
                if (jarEntry.getName().endsWith(".class")) {
                    scanClassStream(jarInputStream, sensitiveProperties);
                }
            }
        }
    }

    private static void scanClassFile(Path classFile, Set<String> sensitiveProperties)
            throws IOException
    {
        try (InputStream inputStream = Files.newInputStream(classFile)) {
            scanClassStream(inputStream, sensitiveProperties);
        }
    }

    private static void scanClassStream(InputStream inputStream, Set<String> sensitiveProperties)
            throws IOException
    {
        ClassReader classReader = new ClassReader(inputStream);
        classReader.accept(new SensitivePropertyScanner(sensitiveProperties), 0);
    }

    private static class SensitivePropertyScanner
            extends ClassVisitor
    {
        private final Set<String> sensitiveProperties;

        public SensitivePropertyScanner(Set<String> sensitiveProperties)
        {
            super(Opcodes.ASM9);
            this.sensitiveProperties = sensitiveProperties;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
        {
            return new SensitiveMethodScanner(sensitiveProperties);
        }
    }

    private static class SensitiveMethodScanner
            extends MethodVisitor
    {
        private static final String CONFIG_SECURITY_SENSITIVE_DESC = Type.getDescriptor(ConfigSecuritySensitive.class);
        private static final String CONFIG_DESC = Type.getDescriptor(Config.class);

        private final Set<String> sensitiveProperties;
        private boolean hasConfigSecuritySensitive;
        private String configValue;

        public SensitiveMethodScanner(Set<String> sensitiveProperties)
        {
            super(Opcodes.ASM9);
            this.sensitiveProperties = sensitiveProperties;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible)
        {
            if (descriptor.equals(CONFIG_SECURITY_SENSITIVE_DESC)) {
                hasConfigSecuritySensitive = true;
                return super.visitAnnotation(descriptor, visible);
            }
            if (descriptor.equals(CONFIG_DESC)) {
                return new AnnotationVisitor(Opcodes.ASM9)
                {
                    @Override
                    public void visit(String name, Object value)
                    {
                        if ("value".equals(name) && value instanceof String string) {
                            configValue = string;
                        }
                        super.visit(name, value);
                    }
                };
            }
            return super.visitAnnotation(descriptor, visible);
        }

        @Override
        public void visitEnd()
        {
            if (hasConfigSecuritySensitive) {
                checkState(configValue != null, "Missing @Config annotation on a method annotated with @ConfigSecuritySensitive");
                sensitiveProperties.add(configValue);
            }
            super.visitEnd();
        }
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
