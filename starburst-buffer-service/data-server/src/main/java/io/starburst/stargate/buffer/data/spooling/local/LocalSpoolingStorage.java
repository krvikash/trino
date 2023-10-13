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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.CHUNK_FILE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getMetadataFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class LocalSpoolingStorage
        implements SpoolingStorage
{
    private final URI spoolingDirectory;
    private final boolean chunkSpoolMergeEnabled;
    private final MergedFileNameGenerator mergedFileNameGenerator;

    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    @Inject
    public LocalSpoolingStorage(
            ChunkManagerConfig config,
            MergedFileNameGenerator mergedFileNameGenerator)
    {
        this.spoolingDirectory = requireNonNull(config.getSpoolingDirectory(), "spoolingDirectory is null");
        this.chunkSpoolMergeEnabled = config.isChunkSpoolMergeEnabled();
        this.mergedFileNameGenerator = requireNonNull(mergedFileNameGenerator, "mergedFileNameGenerator is null");
    }

    @Override
    public SpooledChunk getSpooledChunk(long chunkBufferNodeId, String exchangeId, long chunkId)
    {
        if (chunkSpoolMergeEnabled) {
            throw new DataServerException(CHUNK_NOT_FOUND,
                    "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(chunkBufferNodeId, exchangeId, chunkId));
        }

        File file = getFilePath(chunkBufferNodeId, exchangeId, chunkId).toFile();
        if (!file.exists()) {
            throw new DataServerException(CHUNK_NOT_FOUND, "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(chunkBufferNodeId, exchangeId, chunkId));
        }
        int sizeInBytes = toIntExact(file.length());
        verify(sizeInBytes > CHUNK_FILE_HEADER_SIZE,
                "length %s should be larger than CHUNK_FILE_HEADER_SIZE", sizeInBytes, CHUNK_FILE_HEADER_SIZE);
        return new SpooledChunk(file.getPath(), 0, sizeInBytes);
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease)
    {
        checkArgument(!chunkDataLease.chunkSlices().isEmpty(), "unexpected empty chunk when spooling");

        File file = getFilePath(bufferNodeId, exchangeId, chunkId).toFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            // It is possible that directory does not exist when first
            // `parent.exists()` is called but then is created by some other thread and
            // `parent.mkdirs()` returns false. To treat such race condition as success
            // we add yet another call to `parent.exists()` at the end of the chain.
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        try (SliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(file))) {
            sliceOutput.writeLong(chunkDataLease.checksum());
            sliceOutput.writeInt(chunkDataLease.numDataPages());
            chunkDataLease.chunkSlices().forEach(sliceOutput::writeBytes);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        counts.computeIfAbsent(exchangeId, ignored -> new AtomicInteger()).incrementAndGet();
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Map<Long, SpooledChunk>> writeMergedChunks(long bufferNodeId, String exchangeId, Map<Chunk, ChunkDataLease> chunkDataLeaseMap, long contentLength)
    {
        File mergedFile = getMergedFilePath(bufferNodeId, exchangeId).toFile();
        File parent = mergedFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            // It is possible that directory does not exist when first
            // `parent.exists()` is called but then is created by some other thread and
            // `parent.mkdirs()` returns false. To treat such race condition as success
            // we add yet another call to `parent.exists()` at the end of the chain.
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }

        long offset = 0;
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
        try (SliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(mergedFile))) {
            for (Map.Entry<Chunk, ChunkDataLease> entry : chunkDataLeaseMap.entrySet()) {
                Chunk chunk = entry.getKey();
                ChunkDataLease chunkDataLease = entry.getValue();
                if (chunkDataLease == null) {
                    continue;
                }
                int length = chunkDataLease.serializedSizeInBytes();
                spooledChunkMap.put(chunk.getChunkId(), new SpooledChunk(mergedFile.getPath(), offset, length));
                offset += length;
                sliceOutput.writeLong(chunkDataLease.checksum());
                sliceOutput.writeInt(chunkDataLease.numDataPages());
                chunkDataLease.chunkSlices().forEach(sliceOutput::writeBytes);
            }
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        counts.computeIfAbsent(exchangeId, ignored -> new AtomicInteger()).incrementAndGet();
        return immediateFuture(spooledChunkMap.build());
    }

    @Override
    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        counts.remove(exchangeId);
        for (String prefixedDirectory : getPrefixedDirectories(bufferNodeId, exchangeId)) {
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
    public ListenableFuture<Void> writeMetadataFile(long bufferNodeId, Slice metadataSlice)
    {
        File file = getPath(getMetadataFileName(bufferNodeId)).toFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            // It is possible that directory does not exist when first
            // `parent.exists()` is called but then is created by some other thread and
            // `parent.mkdirs()` returns false. To treat such race condition as success
            // we add yet another call to `parent.exists()` at the end of the chain.
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        try (SliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(file))) {
            sliceOutput.writeBytes(metadataSlice);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Slice> readMetadataFile(long bufferNodeId)
    {
        File metadataFile = getPath(getMetadataFileName(bufferNodeId)).toFile();
        try (FileInputStream inputStream = new FileInputStream(metadataFile)) {
            return immediateFuture(Slices.wrappedBuffer(inputStream.readAllBytes()));
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
    }

    @Override
    public int getSpooledChunks()
    {
        return counts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    private Path getFilePath(long bufferNodeId, String exchangeId, long chunkId)
    {
        return getPath(getFileName(bufferNodeId, exchangeId, chunkId));
    }

    private Path getMergedFilePath(long bufferNodeId, String exchangeId)
    {
        return getPath(mergedFileNameGenerator.getNextMergedFileName(bufferNodeId, exchangeId));
    }

    private Path getPath(String fileName)
    {
        return Paths.get(spoolingDirectory.resolve(fileName).getPath());
    }

    @PreDestroy
    @Override
    public void close()
            throws Exception
    {
    }
}
