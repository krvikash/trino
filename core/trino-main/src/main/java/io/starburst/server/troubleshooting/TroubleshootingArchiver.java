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

import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import jakarta.annotation.PreDestroy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.starburst.server.troubleshooting.TroubleshootingContext.State.INVALID;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class TroubleshootingArchiver
{
    private static final Logger log = Logger.get(TroubleshootingArchiver.class);

    public static final String TOP_LEVEL_ERRORS_FILENAME = "top-level.errors";

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
        PipedOutputStream outputStreamClosableByReceiver;
        PipedInputStream inputStream = new PipedInputStream();
        try {
            outputStreamClosableByReceiver = new PipedOutputStream(inputStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        executor.execute(() -> archiveAsynchronously(context, outputStreamClosableByReceiver));
        return inputStream;
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

    private void archiveAsynchronously(TroubleshootingContext context, PipedOutputStream outputStreamCloseableByReceiver)
    {
        try (ZipOutputStream archive = new ZipOutputStream(outputStreamCloseableByReceiver)) {
            if (INVALID != context.getState().get()) {
                for (TroubleshootingProvider dataProvider : dataProviders) {
                    writeProviderDataToArchive(dataProvider, context, archive);
                    archive.closeEntry();
                    archive.flush();
                }
            }

            writeErrorsToArchive(context, archive);
            archive.finish();
        }
        catch (Exception e) {
            throw new RuntimeException("Encountered error while archiving troubleshooting information", e);
        }
        finally {
            // In case of an error `close` method on `ZipOutputStream` will try to end ZIP archive properly.
            // The `close` method can throw an IOException and in turn passed `PipedOutputStream` will not be closed.
            // In effect the client will wait for incoming data forever.
            try {
                outputStreamCloseableByReceiver.close();
            }
            catch (IOException e) {
                log.error(e, "Failed to close archiving output stream");
            }
        }
    }

    private void writeErrorsToArchive(TroubleshootingContext context, ZipOutputStream archive)
    {
        List<Exception> topLevelErrors = context.getTopLevelErrors();
        if (!topLevelErrors.isEmpty()) {
            try {
                archive.putNextEntry(new ZipEntry(format("%s/%s", context.getQueryId().getId(), TOP_LEVEL_ERRORS_FILENAME)));
                for (Exception e : topLevelErrors) {
                    archive.write(ExceptionUtils.getStackTrace(e).getBytes(UTF_8));
                }
            }
            catch (IOException e) {
                log.error(e, "Error while writing top-level.errors to zip");
            }
        }

        Map<String, Exception> errors = context.getErrors();
        for (String key : errors.keySet()) {
            try {
                archive.putNextEntry(new ZipEntry(format("%s/%s.errors", context.getQueryId().getId(), key)));
                archive.write(ExceptionUtils.getStackTrace(errors.get(key)).getBytes(UTF_8));
            }
            catch (IOException e) {
                log.error(e, "Error while writing %s/%s.errors", context.getQueryId().getId(), key);
            }
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
            context.addGeneralError(e);
            log.warn(e, "%s.getInputStreams() failed for query with id: %s", dataProviderName, context.getQueryId().getId());
            return;
        }
        try (Closer closer = Closer.create()) {
            filenameInputStreamMap.values().forEach(closer::register);
            writeInputStreamToArchiveEntry(filenameInputStreamMap, archive, context, dataProviderName);
        }
        catch (IOException _) {
            // all streams are closed (except those that throw) and it's all that matters
        }
    }

    private void writeInputStreamToArchiveEntry(Map<String, InputStream> filenameInputStreamMap, ZipOutputStream archive, TroubleshootingContext context, String dataProviderName)
    {
        for (Map.Entry<String, InputStream> entry : filenameInputStreamMap.entrySet()) {
            try {
                archive.putNextEntry(new ZipEntry(format("%s/%s", context.getQueryId().getId(), entry.getKey())));
            }
            catch (IOException e) {
                log.warn(e, "Failed while writing a new entry to zip file from provider: %s", dataProviderName);
                context.addError(entry.getKey(), e);
                return;
            }
            try {
                long copied = entry.getValue().transferTo(archive);
                log.debug("copied %d bytes for stream %s from provider %s", copied, entry.getKey(), dataProviderName);
            }
            catch (Exception e) {
                log.warn(e, "Failed while writing to a zip file from provider: %s", dataProviderName);
                context.addError(entry.getKey(), e);
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
