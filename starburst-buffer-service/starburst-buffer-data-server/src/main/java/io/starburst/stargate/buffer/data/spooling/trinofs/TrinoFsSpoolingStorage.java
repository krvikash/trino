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
import io.airlift.units.Duration;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
    private final DataServerStats dataServerStats;
    private final TrinoFileSystem fileSystem;
    private final ListeningExecutorService executor;
    private final ListeningExecutorService deleteExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final Duration operationTimeout;
    private final URI rootUri;

    @Inject
    public TrinoFsSpoolingStorage(
            BufferNodeId bufferNodeId,
            SpoolingDirectoryConfig spoolingDirectoryConfig,
            MergedFileNameGenerator mergedFileNameGenerator,
            DataServerStats dataServerStats,
            @ForTrinoFsSpooling TrinoFileSystem fileSystem,
            @ForTrinoFsSpooling ListeningExecutorService executor,
            @ForTrinoFsSpoolingDelete ListeningExecutorService deleteExecutor,
            TrinoFsSpoolingConfig config,
            @ForTrinoFsSpooling ScheduledExecutorService timeoutExecutor)
    {
        super(bufferNodeId, mergedFileNameGenerator, dataServerStats);
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.deleteExecutor = requireNonNull(deleteExecutor, "deleteExecutor is null");
        this.operationTimeout = requireNonNull(config, "config is null").getOperationTimeout();
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.rootUri = requireNonNull(spoolingDirectoryConfig.getSpoolingDirectory(), "spoolingDirectory is null");
        checkArgument(rootUri.toString().endsWith(PATH_SEPARATOR), "rootUri must end with '%s': %s", PATH_SEPARATOR, rootUri);
    }

    private <T> ListenableFuture<T> withOperationTimeout(ListenableFuture<T> future)
    {
        // Futures.withTimeout cancels the delegate with mayInterruptIfRunning=true, which
        // interrupts the pool thread blocked inside the TrinoFileSystem call.
        return translateFailures(Futures.withTimeout(future, operationTimeout.toMillis(), TimeUnit.MILLISECONDS, timeoutExecutor));
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
        return withOperationTimeout(executor.submit(() -> {
            ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
            // Lazy stream suppliers, one per chunk header plus the chunk body. Memory chunks supply
            // zero-copy views over their existing byte[]s; disk chunks stream straight from the file
            // channel in bounded blocks so a large chunk is never re-buffered whole in heap.
            List<Supplier<InputStream>> segmentSuppliers = new ArrayList<>();
            long offset = 0;
            for (Map.Entry<Chunk, ChunkDataLease> entry : chunkDataLeaseMap.entrySet()) {
                Chunk chunk = entry.getKey();
                ChunkDataLease lease = entry.getValue();
                int serializedSize = lease.serializedSizeInBytes();
                spooledChunkMap.put(chunk.getChunkId(), new SpooledChunk(location, offset, serializedSize));
                offset += serializedSize;
                Slice header = Slices.allocate(CHUNK_FILE_HEADER_SIZE);
                SliceOutput headerOutput = header.getOutput();
                headerOutput.writeLong(lease.getChecksum());
                headerOutput.writeInt(lease.getNumDataPages());
                segmentSuppliers.add(sliceStreamSupplier(header));
                switch (lease) {
                    case MemoryChunkDataLease memoryLease -> {
                        for (Slice slice : memoryLease.getChunkSlices()) {
                            segmentSuppliers.add(sliceStreamSupplier(slice));
                        }
                        dataServerStats.recordMemorySpooledBytes(lease.serializedSizeInBytes());
                    }
                    case DiskChunkDataLease diskLease -> {
                        segmentSuppliers.add(() -> new DiskChunkInputStream(diskLease));
                        dataServerStats.recordDiskSpooledBytes(lease.serializedSizeInBytes());
                    }
                }
            }
            TrinoOutputFile output = fileSystem.newOutputFile(Location.of(location));
            // The supplier may be invoked more than once (e.g. AWS SDK retries); each call rebuilds
            // fresh streams over the same memory byte[]s / disk files.
            output.createOrOverwrite(() -> chunkBody(segmentSuppliers), contentLength);
            return spooledChunkMap.buildOrThrow();
        }));
    }

    private static Supplier<InputStream> sliceStreamSupplier(Slice slice)
    {
        return () -> new ByteArrayInputStream(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private static InputStream chunkBody(List<Supplier<InputStream>> segmentSuppliers)
    {
        InputStream[] streams = new InputStream[segmentSuppliers.size()];
        for (int i = 0; i < segmentSuppliers.size(); i++) {
            streams[i] = segmentSuppliers.get(i).get();
        }
        return new SequenceInputStream(Collections.enumeration(Arrays.asList(streams)));
    }

    // Streams a disk chunk straight from its file channel in bounded blocks via positional reads,
    // so spooling a large chunk never materializes the whole chunk in heap.
    private static final class DiskChunkInputStream
            extends InputStream
    {
        private final DiskChunkDataLease lease;
        private final int length;
        private long position;
        private final byte[] single = new byte[1];

        private DiskChunkInputStream(DiskChunkDataLease lease)
        {
            this.lease = requireNonNull(lease, "lease is null");
            this.length = lease.length();
        }

        @Override
        public int read()
                throws IOException
        {
            int read = read(single, 0, 1);
            return read < 0 ? -1 : single[0] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int len)
                throws IOException
        {
            if (position >= length) {
                return -1;
            }
            int toRead = (int) Math.min(len, length - position);
            ByteBuffer buffer = ByteBuffer.wrap(destination, offset, toRead);
            int read = lease.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF reading disk chunk at file position " + position);
            }
            position += read;
            return read;
        }
    }

    @Override
    protected ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
    {
        ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();
        for (String directoryName : directoryNames) {
            String dirLocation = rootUri.toString() + directoryName;
            futures.add(withOperationTimeout(deleteExecutor.submit(() -> {
                fileSystem.deleteDirectory(Location.of(dirLocation));
                return null;
            })));
        }
        return asVoid(Futures.allAsList(futures.build()));
    }

    @Override
    public ListenableFuture<Void> writeMetadataFile(long bufferNodeId, Slice metadataSlice)
    {
        String location = getLocation(getMetadataFileName(bufferNodeId));
        return asVoid(withOperationTimeout(executor.submit(() -> {
            TrinoOutputFile output = fileSystem.newOutputFile(Location.of(location));
            output.createOrOverwrite(metadataSlice.getBytes());
            return null;
        })));
    }

    @Override
    public ListenableFuture<Slice> readMetadataFile(long bufferNodeId)
    {
        String location = getLocation(getMetadataFileName(bufferNodeId));
        return withOperationTimeout(executor.submit(() -> {
            TrinoInputFile input = fileSystem.newInputFile(Location.of(location));
            try (TrinoInputStream stream = input.newStream()) {
                return Slices.wrappedBuffer(stream.readAllBytes());
            }
        }));
    }

    @Override
    public void close() {}
}
