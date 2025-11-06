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
import io.airlift.log.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TroubleshootingTestHelper
{
    private static final Logger log = Logger.get(TroubleshootingTestHelper.class);

    private TroubleshootingTestHelper() {}

    public static Unzipped zipInputStreamToMap(InputStream inputStream, Path tmpDir)
    {
        try {
            ImmutableMap.Builder<String, byte[]> mapBuilder = ImmutableMap.builder();
            Path tmpFile = Files.createTempFile(tmpDir, "troubleshooting-", ".zip");
            log.debug("File with troubleshooting information was stored at: %s", tmpFile);
            try (var outputStream = Files.newOutputStream(tmpFile)) {
                inputStream.transferTo(outputStream);
            }
            try (var zipFile = new ZipFile(tmpFile.toFile())) {
                var e = zipFile.entries();
                while (e.hasMoreElements()) {
                    ZipEntry entry = e.nextElement();
                    mapBuilder.put(entry.getName(), zipFile.getInputStream(entry).readAllBytes());
                }
                return new Unzipped(mapBuilder.buildOrThrow(), tmpFile);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public record Unzipped(Map<String, byte[]> contents, Path tmpFile) {}

    public static void assertPropertyExists(byte[] actual, String property)
    {
        assertThat(new String(actual, ISO_8859_1)).contains(property);
    }

    public static void assertJvmConfig(byte[] actual)
    {
        String jvmConfig = String.join("\n", ManagementFactory.getRuntimeMXBean().getInputArguments()) + "\n";
        assertThat(actual).isEqualTo(jvmConfig.getBytes(UTF_8));
    }

    public static List<Unzipped> findConfigZips(Unzipped inputsMap, String zipPrefix, Path tmpDir)
    {
        return inputsMap.contents().entrySet().stream()
                .filter(entry -> {
                    String path = entry.getKey();
                    return path.contains("/configs/" + zipPrefix) && path.endsWith(".zip");
                })
                .map(Map.Entry::getValue)
                .map(zipBytes -> zipInputStreamToMap(new ByteArrayInputStream(zipBytes), tmpDir))
                .toList();
    }

    public static String findWorkerConfigDirectoryName(Unzipped workerConfigs)
    {
        Set<String> paths = workerConfigs.contents().keySet();
        return paths.stream()
                .filter(path -> path.startsWith("worker-") && path.endsWith("/"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No worker config directory found. Scanned paths: %s.".formatted(paths)));
    }
}
