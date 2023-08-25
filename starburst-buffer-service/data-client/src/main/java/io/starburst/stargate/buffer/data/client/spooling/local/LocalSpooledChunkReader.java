/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.local;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import jakarta.annotation.PreDestroy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;

public class LocalSpooledChunkReader
        implements SpooledChunkReader
{
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public LocalSpooledChunkReader(DataApiConfig config)
    {
        this.dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        URI uri = URI.create(spooledChunk.location());
        int length = spooledChunk.length();

        String scheme = uri.getScheme();
        checkArgument(scheme == null || scheme.equals("file"), "Unexpected storage scheme %s for LocalSpooledChunkReader, expecting null/file");

        byte[] bytes = new byte[length];
        try (InputStream inputStream = new FileInputStream(Paths.get(spooledChunk.location()).toFile())) {
            int bytesRead = inputStream.read(bytes);
            verify(bytesRead == length, "bytesRead %s not equal to length %s", bytesRead, length);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(toDataPages(bytes, dataIntegrityVerificationEnabled));
    }

    @PreDestroy
    @Override
    public void close()
            throws Exception
    {
    }
}
