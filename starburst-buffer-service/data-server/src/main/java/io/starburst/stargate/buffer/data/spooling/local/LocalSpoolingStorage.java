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

import com.google.common.io.ByteStreams;
import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;
import io.starburst.stargate.buffer.data.spooling.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.CHUNK_FILE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getChunkDataHolder;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class LocalSpoolingStorage
        implements SpoolingStorage
{
    private final MemoryAllocator memoryAllocator;
    private final URI spoolingDirectory;
    private final ExecutorService executor;

    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    @Inject
    public LocalSpoolingStorage(
            MemoryAllocator memoryAllocator,
            ChunkManagerConfig config,
            ExecutorService executor)
    {
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingDirectory = requireNonNull(config.getSpoolingDirectory(), "spoolingDirectory is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ChunkDataLease readChunk(String exchangeId, long chunkId, long bufferNodeId)
    {
        File file = getFilePath(exchangeId, chunkId, bufferNodeId).toFile();
        if (!file.exists()) {
            throw new DataServerException(CHUNK_NOT_FOUND, "No closed chunk found for exchange %s, chunk %d, bufferNodeId %d".formatted(exchangeId, chunkId, bufferNodeId));
        }
        int fileLength = toIntExact(file.length());
        verify(fileLength > CHUNK_FILE_HEADER_SIZE,
                "fileLength %s should be larger than CHUNK_FILE_HEADER_SIZE", fileLength, CHUNK_FILE_HEADER_SIZE);
        return ChunkDataLease.forSliceLease(
                new SliceLease(memoryAllocator, fileLength),
                slice -> {
                    int bytesRead;
                    try (InputStream inputStream = new FileInputStream(file)) {
                        bytesRead = ByteStreams.read(inputStream, slice.byteArray(), 0, slice.length());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    verify(bytesRead == fileLength, "bytesRead %s not equal to fileLength %s for file %s",
                            bytesRead, fileLength, file.getName());
                    return getChunkDataHolder(slice);
                },
                executor);
    }

    @Override
    public ListenableFuture<Void> writeChunk(String exchangeId, long chunkId, long bufferNodeId, ChunkDataHolder chunkDataHolder)
    {
        File file = getFilePath(exchangeId, chunkId, bufferNodeId).toFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
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
