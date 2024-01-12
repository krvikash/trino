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
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.trino.spi.QueryId;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class TroubleshootingArchiver
{
    private static final Logger log = Logger.get(TroubleshootingArchiver.class);

    private final Set<TroubleshootingProvider> dataProviders;
    private final ExecutorService executor;

    @Inject
    public TroubleshootingArchiver(Set<TroubleshootingProvider> dataProviders)
    {
        this.dataProviders = requireNonNull(dataProviders, "dataProviders is null");
        this.executor = newFixedThreadPool(4);
    }

    public InputStream execute(TroubleshootingContext context)
    {
        PipedInputStream is = new PipedInputStream();
        executor.execute(() -> archiveAsynchronously(context, is));
        return is;
    }

    @PreDestroy
    public void close()
    {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating the troubleshooting archive", e);
        }
    }

    private void archiveAsynchronously(TroubleshootingContext context, PipedInputStream stream)
    {
        try (PipedOutputStream os = new PipedOutputStream(stream); ZipOutputStream archive = new ZipOutputStream(os)) {
            for (TroubleshootingProvider dataProvider : dataProviders) {
                writeProviderDataToArchive(dataProvider, context, archive);
                archive.closeEntry();
                archive.flush();
            }
            archive.finish();
        }
        catch (Exception e) {
            throw new RuntimeException("Encountered error while archiving troubleshooting information", e);
        }
    }

    private void writeProviderDataToArchive(TroubleshootingProvider dataProvider, TroubleshootingContext context, ZipOutputStream archive)
    {
        String dataProviderName = dataProvider.getClass().getName();
        Map<String, InputStream> filenameInputStreamMap;
        try {
            filenameInputStreamMap = dataProvider.getInputStreams(context);
        }
        catch (Exception e) {
            log.warn(e, "%s.getInputStreams() failed for query with id: %s", dataProviderName, context.getQueryId().getId());
            return;
        }
        writeInputStreamToArchiveEntry(filenameInputStreamMap, archive, context.getQueryId(), dataProviderName);
    }

    private void writeInputStreamToArchiveEntry(Map<String, InputStream> filenameInputStreamMap, ZipOutputStream archive, QueryId queryId, String dataProviderName)
    {
        for (Map.Entry<String, InputStream> entry : filenameInputStreamMap.entrySet()) {
            try {
                archive.putNextEntry(new ZipEntry(String.format("%s/%s", queryId.getId(), entry.getKey())));
            }
            catch (IOException e) {
                log.warn(e, "Failed while writing a new entry to zip file from provider: %s", dataProviderName);
                throw new RuntimeException(e);
            }
            try {
                long copied = ByteStreams.copy(entry.getValue(), archive);
                log.debug("copied %d bytes for stream %s from provider %s", copied, entry.getKey(), dataProviderName);
            }
            catch (Exception e) {
                log.warn(e, "Failed while writing to a zip file from provider: %s", dataProviderName);
            }
            finally {
                try {
                    archive.closeEntry();
                }
                catch (IOException e) {
                    log.warn(e, "Failed while closing entry for a zip file from provider: %s, will show up as an empty file", dataProviderName);
                }
            }
        }
    }
}
