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

import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingUtils;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncRequestBody;
import software.amazon.awssdk.core.internal.util.Mimetype;

import java.nio.ByteBuffer;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This class mimics the implementation of {@link ByteArrayAsyncRequestBody} except for we directly
 * write chunkDataLease to avoid unnecessary memory copy
 *
 * An implementation of {@link AsyncRequestBody} for providing data from memory.
 */
public class ChunkDataAsyncRequestBody
        implements AsyncRequestBody
{
    private static final Logger log = Logger.get(ChunkDataAsyncRequestBody.class);

    private final ChunkDataLease chunkDataLease;
    private final String mimetype;
    private final long contentLength;

    public ChunkDataAsyncRequestBody(ChunkDataLease chunkDataLease, String mimetype)
    {
        this.chunkDataLease = requireNonNull(chunkDataLease, "chunkDataLease is null");
        this.mimetype = requireNonNull(mimetype, "mimeType is null");
        this.contentLength = chunkDataLease.serializedSizeInBytes();
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

                        @Override
                        public void request(long n)
                        {
                            if (done) {
                                return;
                            }
                            if (n > 0) {
                                done = true;
                                SpoolingUtils.writeChunkDataLease(chunkDataLease, s::onNext);
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

    static AsyncRequestBody fromChunkDataLease(ChunkDataLease chunkDataLease)
    {
        return new ChunkDataAsyncRequestBody(chunkDataLease, Mimetype.MIMETYPE_OCTET_STREAM);
    }
}
