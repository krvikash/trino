/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.DiskChunkDataLease;
import io.starburst.stargate.buffer.data.execution.MemoryChunkDataLease;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.AbstractSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.CHUNK_FILE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getMetadataFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.translateFailures;
import static java.util.Objects.requireNonNull;

/**
 * TrinoFileSystem-backed spooling storage. {@code TrinoFileSystem} is synchronous, so all I/O
 * is offloaded to executors. Read/write traffic and delete traffic use <strong>separate</strong>
 * pools so deletes (best-effort cleanup) cannot starve the request path.
 *
 * <p>Cancellation: callers may cancel the returned futures with {@code cancel(true)}; this
 * interrupts the worker thread executing the blocking {@code TrinoFileSystem} call. Whether
 * that interrupt actually unblocks the SDK is implementation-dependent, but we propagate it
 * unconditionally — never swallow {@code InterruptedException}.
 */
public class TrinoFsSpoolingStorage
        extends AbstractSpoolingStorage
{
    private final TrinoFileSystem fileSystem;
    private final ListeningExecutorService executor;
    private final ListeningExecutorService deleteExecutor;
    private final URI rootUri;

    @Inject
    public TrinoFsSpoolingStorage(
            BufferNodeId bufferNodeId,
            SpoolingDirectoryConfig spoolingDirectoryConfig,
            MergedFileNameGenerator mergedFileNameGenerator,
            DataServerStats dataServerStats,
            @ForTrinoFsSpooling TrinoFileSystem fileSystem,
            @ForTrinoFsSpooling ListeningExecutorService executor,
            @ForTrinoFsSpoolingDelete ListeningExecutorService deleteExecutor)
    {
        super(bufferNodeId, mergedFileNameGenerator, dataServerStats);
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.deleteExecutor = requireNonNull(deleteExecutor, "deleteExecutor is null");
        this.rootUri = requireNonNull(spoolingDirectoryConfig.getSpoolingDirectory(), "spoolingDirectory is null");
        checkArgument(rootUri.toString().endsWith(PATH_SEPARATOR), "rootUri must end with '%s': %s", PATH_SEPARATOR, rootUri);
    }

    @Override
    protected String getLocation(String fileName)
    {
        return rootUri.toString() + fileName;
    }

    @Override
    protected ListenableFuture<Map<Long, SpooledChunk>> putStorageObject(
            String fileName,
            Map<Chunk, ChunkDataLease> chunkDataLeaseMap,
            long contentLength)
    {
        String location = getLocation(fileName);
        return translateFailures(executor.submit(() -> {
            ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
            // One Slice per chunk header + one per backing chunk slice. The header is a fresh
            // small allocation; the backing slices reference the existing chunk byte[]s.
            List<Slice> segments = new ArrayList<>();
            long offset = 0;
            for (Map.Entry<Chunk, ChunkDataLease> entry : chunkDataLeaseMap.entrySet()) {
                Chunk chunk = entry.getKey();
                ChunkDataLease lease = entry.getValue();
                MemoryChunkDataLease memoryLease = switch (lease) {
                    case MemoryChunkDataLease m -> m;
                    case DiskChunkDataLease ignored -> throw new UnsupportedOperationException("disk chunk lease not supported for spooling");
                };
                int length = memoryLease.serializedSizeInBytes();
                spooledChunkMap.put(chunk.getChunkId(), new SpooledChunk(location, offset, length));
                offset += length;
                Slice header = Slices.allocate(CHUNK_FILE_HEADER_SIZE);
                SliceOutput headerOutput = header.getOutput();
                headerOutput.writeLong(memoryLease.getChecksum());
                headerOutput.writeInt(memoryLease.getNumDataPages());
                segments.add(header);
                segments.addAll(memoryLease.getChunkSlices());
            }
            TrinoOutputFile output = fileSystem.newOutputFile(Location.of(location));
            // The supplier may be invoked more than once (e.g. AWS SDK retries); each call rebuilds
            // fresh ByteArrayInputStream views over the same underlying chunk byte[]s — still zero-copy.
            output.createOrOverwrite(() -> chunkBody(segments), contentLength);
            return spooledChunkMap.buildOrThrow();
        }));
    }

    private static InputStream chunkBody(List<Slice> segments)
    {
        InputStream[] streams = new InputStream[segments.size()];
        for (int i = 0; i < segments.size(); i++) {
            Slice s = segments.get(i);
            streams[i] = new ByteArrayInputStream(s.byteArray(), s.byteArrayOffset(), s.length());
        }
        return new SequenceInputStream(Collections.enumeration(Arrays.asList(streams)));
    }

    @Override
    protected ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
    {
        ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();
        for (String directoryName : directoryNames) {
            String dirLocation = rootUri.toString() + directoryName;
            futures.add(deleteExecutor.submit(() -> {
                fileSystem.deleteDirectory(Location.of(dirLocation));
                return null;
            }));
        }
        return translateFailures(asVoid(Futures.allAsList(futures.build())));
    }

    @Override
    public ListenableFuture<Void> writeMetadataFile(long bufferNodeId, Slice metadataSlice)
    {
        String location = getLocation(getMetadataFileName(bufferNodeId));
        return translateFailures(asVoid(executor.submit(() -> {
            TrinoOutputFile output = fileSystem.newOutputFile(Location.of(location));
            output.createOrOverwrite(metadataSlice.getBytes());
            return null;
        })));
    }

    @Override
    public ListenableFuture<Slice> readMetadataFile(long bufferNodeId)
    {
        String location = getLocation(getMetadataFileName(bufferNodeId));
        return translateFailures(executor.submit(() -> {
            TrinoInputFile input = fileSystem.newInputFile(Location.of(location));
            try (TrinoInputStream stream = input.newStream()) {
                return Slices.wrappedBuffer(stream.readAllBytes());
            }
        }));
    }

    @Override
    public void close() {}
}
