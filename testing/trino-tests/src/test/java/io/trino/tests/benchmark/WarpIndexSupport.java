/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.benchmark;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Creates the warp_speed catalog backing the {@code --warp-index} arm. Warp indexes the columns
 * its queries touch and proxies everything else to the workload's own connector, which reads the
 * same tables through the same filesystem cache as the baseline arm.
 */
final class WarpIndexSupport
{
    private static final Logger log = Logger.get(WarpIndexSupport.class);

    private static final String NATIVE_LIBRARY = System.mapLibraryName("presto-varada-jni");

    /**
     * Catalog carrying the warp_speed connector. Benchmark queries run against this name while
     * the workload's own catalog stays in place for table setup, verification and ANALYZE.
     */
    static final String WARP_CATALOG = "warp";

    private WarpIndexSupport() {}

    /**
     * Adds a warp_speed catalog proxying {@code proxiedConnector} over the workload's connector
     * properties. The proxied connector is the production one, so it needs the native local
     * filesystem rooted where the workload's testing catalog roots it, and the metastore
     * directory expressed relative to that root.
     */
    static void createWarpCatalog(
            DistributedQueryRunner runner,
            String proxiedConnector,
            Map<String, String> proxiedConnectorProperties,
            Path localFileSystemRoot,
            Path metastoreCatalogDirectory,
            Path localStoreDirectory)
    {
        Path metastoreCatalogLocation = localFileSystemRoot.toAbsolutePath().relativize(metastoreCatalogDirectory.toAbsolutePath());
        Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .putAll(proxiedConnectorProperties)
                .put("hive.metastore.catalog.dir", "local:///" + metastoreCatalogLocation)
                // the workload writes local:// locations, so serve that scheme from the same root
                .put("fs.local.enabled", "true")
                .put("local.location", localFileSystemRoot.toAbsolutePath().toString())
                .put("warp-speed.proxied-connector", proxiedConnector)
                .put("warp-speed.local-store.path", localStoreDirectory.toAbsolutePath().toString())
                // the initial warm storm exhausts the default of two retries
                .put("warp-speed.config.max-warm-retries", "10")
                .put("warp-speed.config.warm-retry-backoff-factor-in-millis", "250")
                .put("warp-speed.cluster-uuid", "benchmark")
                .put("warp-speed.config.http-rest-port-enabled", "false")
                .put("node.environment", "warp")
                .buildKeepingLast();
        installNativeLibraries();
        runner.installPlugin(new WarpPlugin());
        runner.createCatalog(WARP_CATALOG, "warp_speed", catalogProperties);
        // the warm-up stage reads warp's warming counters over jmx
        runner.installPlugin(new JmxPlugin());
        runner.createCatalog("jmx", "jmx", ImmutableMap.of());
    }

    /**
     * Copies the native libraries next to the warp core jar. The plugin build unpacks them under
     * its own target directory, while the engine loads them from an architecture directory beside
     * the core jar, which on the benchmark classpath is the maven repository. Runs before the
     * engine classes load, because loading them loads the libraries.
     */
    private static void installNativeLibraries()
    {
        // the engine appends an sve suffix to this only while WarpNativeStorageEngineModule.SKIP_SVE is false
        String architecture = System.getProperty("os.arch");
        Path target = warpCoreDirectory().resolve(architecture);
        // the harness runs from testing/trino-tests/target/test-classes, so the source tree is four levels up
        Path pluginTarget = classLocation(BenchmarkRunner.class)
                .resolve("../../../../plugin/starburst-warp-speed/starburst-warp-speed-plugin/target")
                .normalize();
        checkState(Files.isDirectory(pluginTarget), "No %s, build the starburst-warp-speed-plugin module", pluginTarget);
        try (Stream<Path> pluginDirectories = Files.list(pluginTarget)) {
            Path source = pluginDirectories
                    .map(directory -> directory.resolve(architecture))
                    .filter(directory -> Files.exists(directory.resolve(NATIVE_LIBRARY)))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No %s directory holding %s under %s".formatted(architecture, NATIVE_LIBRARY, pluginTarget)));
            Files.createDirectories(target);
            // copied on every run, so a rebuilt library replaces the one an earlier run installed
            try (Stream<Path> libraries = Files.list(source)) {
                for (Path library : libraries.toList()) {
                    Files.copy(library, target.resolve(library.getFileName()), REPLACE_EXISTING);
                }
            }
            log.info("Copied warp native libraries from %s to %s", source, target);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Directory the engine resolves native libraries against, being the directory holding the warp
     * core classes.
     */
    private static Path warpCoreDirectory()
    {
        Path location = classLocation(SharedConfig.class);
        if (Files.isDirectory(location)) {
            return location;
        }
        return location.getParent();
    }

    private static Path classLocation(Class<?> clazz)
    {
        try {
            return Path.of(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
