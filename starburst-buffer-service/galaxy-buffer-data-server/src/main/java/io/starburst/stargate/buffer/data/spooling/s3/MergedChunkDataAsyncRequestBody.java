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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingUtils;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.internal.async.ByteBuffersAsyncRequestBody;
import software.amazon.awssdk.core.internal.util.Mimetype;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

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
    private final Map<Chunk, ChunkDataLease> chunkDataLeaseMap;
    private final ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap;
    private final String mimetype;
    private final long contentLength;

    public MergedChunkDataAsyncRequestBody(
            String location,
            Map<Chunk, ChunkDataLease> chunkDataLeaseMap,
            long contentLength,
            ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap,
            String mimetype)
    {
        this.location = requireNonNull(location, "location is null");
        this.chunkDataLeaseMap = requireNonNull(chunkDataLeaseMap, "chunkDataLeaseMap is null");
        this.spooledChunkMap = requireNonNull(spooledChunkMap, "spooledChunkMap is null");
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
                    new Subscription() {
                        private boolean done;
                        private long offset;

                        @Override
                        public void request(long n)
                        {
                            if (done) {
                                return;
                            }
                            if (n > 0) {
                                done = true;
                                for (Map.Entry<Chunk, ChunkDataLease> entry : chunkDataLeaseMap.entrySet()) {
                                    Chunk chunk = entry.getKey();
                                    ChunkDataLease chunkDataLease = entry.getValue();
                                    SpoolingUtils.writeChunkDataLease(chunkDataLease, s::onNext);
                                    int length = chunkDataLease.serializedSizeInBytes();
                                    spooledChunkMap.put(chunk.getChunkId(), new SpooledChunk(location, offset, length));
                                    offset += length;
                                }
                                s.onComplete();
                            }
                            else {
                                s.onError(new IllegalArgumentException("§3.9: non-positive requests are not allowed!"));
                            }
                        }

                        @Override
                        public void cancel()
                        {
                            synchronized (this) {
                                if (!done) {
                                    done = true;
                                }
                            }
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
            ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap)
    {
        return new MergedChunkDataAsyncRequestBody(location, chunkDataLeaseMap, contentLength, spooledChunkMap, Mimetype.MIMETYPE_OCTET_STREAM);
    }
}
