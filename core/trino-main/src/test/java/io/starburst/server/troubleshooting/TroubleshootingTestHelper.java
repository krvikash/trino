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

import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.io.File.createTempFile;

class TroubleshootingTestHelper
{
    private static final Logger log = Logger.get(TroubleshootingTestHelper.class);

    private TroubleshootingTestHelper()
    {
    }

    public static Unzipped zipInputStreamToMap(InputStream stream, Path tmpDir)
    {
        try {
            // Due to this bug we cannot do in-memory with ZipInputStream: https://bugs.openjdk.org/browse/JDK-4201267
            File tmpFile = createTempFile("troubleshooting-", ".zip", tmpDir.toFile());
            log.info("temporary file: %s", tmpFile);
            tmpFile.deleteOnExit();
            try (var os = new FileOutputStream(tmpFile)) {
                ByteStreams.copy(stream, os);
            }
            try (var zipFile = new ZipFile(tmpFile)) {
                Unzipped result = new Unzipped();
                for (var e = zipFile.entries(); e.hasMoreElements(); ) {
                    ZipEntry entry = e.nextElement();
                    result.zipEntryContents.put(entry.getName(), zipFile.getInputStream(entry).readAllBytes());
                }
                return result;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Unzipped
    {
        public Map<String, byte[]> zipEntryContents = new HashMap<>();
    }
}
