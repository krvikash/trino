/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.trinofs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkNotFoundException;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReaderException;
import io.starburst.stargate.buffer.data.spooling.trinofs.ForTrinoFsSpooling;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static java.util.Objects.requireNonNull;

/**
 * Reads spooled chunks via the synchronous {@link TrinoFileSystem} API. The blocking
 * read is offloaded to a {@link ListeningExecutorService} so the {@link SpooledChunkReader}
 * contract (asynchronous {@link ListenableFuture}) is satisfied.
 *
 * <p>The {@link TrinoFileSystem} is provisioned by the embedder (typically
 * {@code TrinoFsClientModule} installed by starburst-trino-main) and bound under
 * {@link ForTrinoFsSpooling}. A single filesystem is picked at startup based on the
 * configured {@code spooling-storage-type}; all spooled-chunk locations are expected
 * to be reachable through it.
 */
public class TrinoFsSpooledChunkReader
        implements SpooledChunkReader
{
    private final boolean dataIntegrityVerificationEnabled;
    private final TrinoFileSystem fileSystem;
    private final ListeningExecutorService executor;

    @Inject
    public TrinoFsSpooledChunkReader(
            DataApiConfig dataApiConfig,
            @ForTrinoFsSpooling TrinoFileSystem fileSystem,
            @ForTrinoFsSpooling ListeningExecutorService executor)
    {
        this.dataIntegrityVerificationEnabled = dataApiConfig.isDataIntegrityVerificationEnabled();
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        return executor.submit(() -> {
            String location = spooledChunk.location();
            try {
                TrinoInputFile input = fileSystem.newInputFile(Location.of(location));
                byte[] bytes = new byte[spooledChunk.length()];
                try (TrinoInput in = input.newInput()) {
                    in.readFully(spooledChunk.offset(), bytes, 0, bytes.length);
                }
                return toDataPages(bytes, dataIntegrityVerificationEnabled);
            }
            catch (FileNotFoundException e) {
                throw new SpooledChunkNotFoundException(e);
            }
            catch (InterruptedIOException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            catch (IOException | RuntimeException e) {
                throw new SpooledChunkReaderException(
                        "unexpected exception reading spooled chunk %s/%s/%s".formatted(
                                spooledChunk.location(), spooledChunk.offset(), spooledChunk.length()),
                        e);
            }
        });
    }

    @Override
    public void close() {}
}
