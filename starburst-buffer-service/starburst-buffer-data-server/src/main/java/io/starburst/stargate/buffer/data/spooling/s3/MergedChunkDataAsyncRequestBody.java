/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.DiskChunkDataLease;
import io.starburst.stargate.buffer.data.execution.MemoryChunkDataLease;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.internal.async.ByteBuffersAsyncRequestBody;
import software.amazon.awssdk.core.internal.util.Mimetype;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.CHUNK_FILE_HEADER_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * This class mimics the implementation of {@link ByteBuffersAsyncRequestBody} except for we directly
 * write chunkDataLeases to avoid unnecessary memory copy, and fill in metadata in spooledChunkMap
 *
 * An implementation of {@link AsyncRequestBody} for providing data from memory.
 */
public class MergedChunkDataAsyncRequestBody
        implements AsyncRequestBody
{
    private static final Logger log = Logger.get(MergedChunkDataAsyncRequestBody.class);

    private final String location;
    private final ImmutableList<Map.Entry<Chunk, ChunkDataLease>> chunkDataLeaseList;
    private final AtomicReference<Map<Long, SpooledChunk>> spooledChunkMapRef;
    private final String mimetype;
    private final long contentLength;

    public MergedChunkDataAsyncRequestBody(
            String location,
            Map<Chunk, ChunkDataLease> chunkDataLeaseMap,
            long contentLength,
            AtomicReference<Map<Long, SpooledChunk>> spooledChunkMapRef,
            String mimetype)
    {
        this.location = requireNonNull(location, "location is null");
        requireNonNull(chunkDataLeaseMap, "chunkDataLeaseMap is null");
        this.chunkDataLeaseList = chunkDataLeaseMap.entrySet().stream().collect(toImmutableList());
        this.spooledChunkMapRef = requireNonNull(spooledChunkMapRef, "spooledChunkMap is null");
        this.mimetype = requireNonNull(mimetype, "mimeType is null");
        this.contentLength = contentLength;
    }

    @Override
    public Optional<Long> contentLength()
    {
        return Optional.of(contentLength);
    }

    @Override
    public String contentType()
    {
        return mimetype;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s)
    {
        // As per rule 1.9 we must throw NullPointerException if the subscriber parameter is null
        if (s == null) {
            throw new NullPointerException("Subscription MUST NOT be null.");
        }

        // As per 2.13, this method must return normally (i.e. not throw).
        try {
            s.onSubscribe(
                    new Subscription()
                    {
                        private final AtomicLong fileOffset = new AtomicLong(0);
                        private final AtomicInteger chunkOffset = new AtomicInteger(0);
                        private final AtomicInteger sliceOffset = new AtomicInteger(0);
                        private final AtomicBoolean done = new AtomicBoolean(false);
                        private final AtomicBoolean cancelled = new AtomicBoolean(false);
                        private final ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();

                        // As per 3.2, it should be possible to call request() from onNext(). This implies that offsets need to be advanced before calls to onNext().
                        @Override
                        public void request(long consumerCallLimit)
                        {
                            if (done.get()) {
                                return;
                            }
                            if (consumerCallLimit <= 0) {
                                s.onError(new IllegalArgumentException("§3.9: non-positive requests are not allowed!"));
                                return;
                            }
                            int consumerCallCount = 0;
                            while (!cancelled.get() && consumerCallCount < consumerCallLimit && chunkOffset.get() < chunkDataLeaseList.size()) {
                                Map.Entry<Chunk, ChunkDataLease> entry = chunkDataLeaseList.get(chunkOffset.get());
                                ChunkDataLease rawLease = entry.getValue();
                                MemoryChunkDataLease chunkDataLease = switch (rawLease) {
                                    case MemoryChunkDataLease lease -> lease;
                                    case DiskChunkDataLease ignored -> throw new UnsupportedOperationException("disk chunk lease not supported for spooling");
                                };
                                int localSliceOffset = sliceOffset.get();
                                if (localSliceOffset == 0) {
                                    SliceOutput sliceOutput = Slices.allocate(CHUNK_FILE_HEADER_SIZE).getOutput();
                                    sliceOutput.writeLong(chunkDataLease.getChecksum());
                                    sliceOutput.writeInt(chunkDataLease.getNumDataPages());
                                    sliceOffset.set(1);
                                    localSliceOffset++;
                                    s.onNext(ByteBuffer.wrap(sliceOutput.slice().byteArray()));
                                    consumerCallCount++;
                                }
                                // Since every slice has a header, chunkSliceOffset is tracked as 1 relative.
                                while (!cancelled.get() && consumerCallCount < consumerCallLimit && localSliceOffset <= chunkDataLease.getChunkSlices().size()) {
                                    if (localSliceOffset == chunkDataLease.getChunkSlices().size()) {
                                        // This is the last slice in the chunk, all thread safe updates must be done before calling onNext() which may start a new (nested) request.
                                        int length = chunkDataLease.serializedSizeInBytes();
                                        Chunk chunk = entry.getKey();
                                        spooledChunkMap.put(chunk.getChunkId(), new SpooledChunk(location, fileOffset.get(), length));
                                        fileOffset.addAndGet(length);
                                        sliceOffset.set(0);
                                        chunkOffset.incrementAndGet();
                                    }
                                    else {
                                        sliceOffset.incrementAndGet();
                                    }
                                    Slice chunkSlice = chunkDataLease.getChunkSlices().get(localSliceOffset - 1);
                                    s.onNext(ByteBuffer.wrap(chunkSlice.byteArray(), chunkSlice.byteArrayOffset(), chunkSlice.length()));
                                    localSliceOffset++;
                                    consumerCallCount++;
                                }
                            }
                            if (chunkOffset.get() == chunkDataLeaseList.size() && done.compareAndSet(false, true)) {
                                Map<Long, SpooledChunk> map = spooledChunkMap.buildOrThrow();
                                spooledChunkMapRef.set(map);
                                s.onComplete();
                            }
                        }

                        @Override
                        public void cancel()
                        {
                            cancelled.set(true);
                        }
                    });
        }
        catch (Throwable ex) {
            log.error(ex, " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.");
        }
    }

    static AsyncRequestBody fromChunks(
            String location,
            Map<Chunk, ChunkDataLease> chunkDataLeaseMap,
            long contentLength,
            AtomicReference<Map<Long, SpooledChunk>> spooledChunkMap)
    {
        return new MergedChunkDataAsyncRequestBody(location, chunkDataLeaseMap, contentLength, spooledChunkMap, Mimetype.MIMETYPE_OCTET_STREAM);
    }
}
