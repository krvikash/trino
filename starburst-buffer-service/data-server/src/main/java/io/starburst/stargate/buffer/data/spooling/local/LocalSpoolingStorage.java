/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.local;

import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.CHUNK_FILE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class LocalSpoolingStorage
        implements SpoolingStorage
{
    private final URI spoolingDirectory;

    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    @Inject
    public LocalSpoolingStorage(ChunkManagerConfig config)
    {
        List<URI> spoolingDirectories = requireNonNull(config.getSpoolingDirectories(), "spoolingDirectory is null");
        if (spoolingDirectories.size() != 1) {
            throw new IllegalArgumentException("Multiple spooling directories not supported");
        }
        this.spoolingDirectory = spoolingDirectories.get(0);
    }

    @Override
    public SpoolingFile getSpoolingFile(long bufferNodeId, String exchangeId, long chunkId)
    {
        File file = getFilePath(exchangeId, chunkId, bufferNodeId).toFile();
        if (!file.exists()) {
            throw new DataServerException(CHUNK_NOT_FOUND, "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(bufferNodeId, exchangeId, chunkId));
        }
        int sizeInBytes = toIntExact(file.length());
        verify(sizeInBytes > CHUNK_FILE_HEADER_SIZE,
                "length %s should be larger than CHUNK_FILE_HEADER_SIZE", sizeInBytes, CHUNK_FILE_HEADER_SIZE);
        return new SpoolingFile(file.getPath(), sizeInBytes);
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataHolder chunkDataHolder)
    {
        checkArgument(!chunkDataHolder.chunkSlices().isEmpty(), "unexpected empty chunk when spooling");

        File file = getFilePath(exchangeId, chunkId, bufferNodeId).toFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            // It is possible that directory does not exist when first
            // `parent.exists()` is called but then is created by some other thread and
            // `parent.mkdirs()` returns false. To treat such race condition as success
            // we add yet another call to `parent.exists()` at the end of the chain.
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        try (SliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(file))) {
            sliceOutput.writeLong(chunkDataHolder.checksum());
            sliceOutput.writeInt(chunkDataHolder.numDataPages());
            chunkDataHolder.chunkSlices().forEach(sliceOutput::writeBytes);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        counts.computeIfAbsent(exchangeId, ignored -> new AtomicInteger()).incrementAndGet();
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        counts.remove(exchangeId);
        for (String prefixedDirectory : getPrefixedDirectories(exchangeId)) {
            Path path = Paths.get(spoolingDirectory.resolve(prefixedDirectory).getPath());
            if (Files.exists(path)) {
                try {
                    MoreFiles.deleteRecursively(path, ALLOW_INSECURE);
                }
                catch (IOException e) {
                    return immediateFailedFuture(e);
                }
            }
        }

        return immediateVoidFuture();
    }

    @Override
    public int getSpooledChunks()
    {
        return counts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    private Path getFilePath(String exchangeId, long chunkId, long bufferNodeId)
    {
        return Paths.get(spoolingDirectory.resolve(getFileName(exchangeId, chunkId, bufferNodeId)).getPath());
    }

    @PreDestroy
    @Override
    public void close()
            throws Exception
    {
    }
}
