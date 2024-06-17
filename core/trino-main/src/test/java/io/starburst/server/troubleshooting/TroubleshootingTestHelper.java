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
import com.google.common.io.ByteStreams;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.io.File.createTempFile;

class TroubleshootingTestHelper
{
    private static final Logger log = Logger.get(TroubleshootingTestHelper.class);

    private TroubleshootingTestHelper() {}

    public static Unzipped zipInputStreamToMap(InputStream inputStream, Path tmpDir)
    {
        try {
            ImmutableMap.Builder<String, byte[]> mapBuilder = ImmutableMap.builder();
            File tmpFile = createTempFile("troubleshooting-", ".zip", tmpDir.toFile());
            log.debug("File with troubleshooting information was stored at: %s", tmpFile);
            try (var outputStream = new FileOutputStream(tmpFile)) {
                ByteStreams.copy(inputStream, outputStream);
            }
            try (var zipFile = new ZipFile(tmpFile)) {
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

    public static Unzipped getTroubleshootingDataForQuery(DistributedQueryRunner queryRunner, Session session, @Language("SQL") String query, Path tmpDir)
            throws Exception
    {
        try (TestingTrinoClient client = new TestingTrinoClient(queryRunner.getCoordinator(), session)) {
            TroubleshootingContextManager troubleshootingContextManager = queryRunner.getCoordinator().getInstance(Key.get(TroubleshootingContextManager.class));
            InputStream inputStream = troubleshootingContextManager.getArchive(client.execute(query).getQueryId()).orElseThrow().get(10, TimeUnit.SECONDS);
            return zipInputStreamToMap(inputStream, tmpDir);
        }
    }

    public record Unzipped(Map<String, byte[]> contents, File tmpFile) {}
}
