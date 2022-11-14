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

import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getChunkDataHolder;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

/**
 *  An implementation of {@link AsyncResponseTransformer} that writes data from the start of the given Slice and
 *  derives a ChunkDataHolder from the written Slice.
 *  This class mimics the implementation of {@link ByteArrayAsyncResponseTransformer} but avoids memory copying.
 *
 * @param <ResponseT> Response POJO type.
 */
public final class ChunkDataAsyncResponseTransformer<ResponseT>
        implements AsyncResponseTransformer<ResponseT, ChunkDataHolder>
{
    private final Slice slice;

    private volatile CompletableFuture<ChunkDataHolder> cf;

    public ChunkDataAsyncResponseTransformer(Slice slice)
    {
        this.slice = requireNonNull(slice, "slice is null");
    }

    @Override
    public CompletableFuture<ChunkDataHolder> prepare()
    {
        cf = new CompletableFuture<>();
        return cf;
    }

    @Override
    public void onResponse(ResponseT response)
    {
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher)
    {
        publisher.subscribe(new BufferSubscriber(slice, cf));
    }

    @Override
    public void exceptionOccurred(Throwable throwable)
    {
        cf.completeExceptionally(throwable);
    }

    /**
     * {@link Subscriber} implementation that writes chunks to given range of a buffer.
     */
    static class BufferSubscriber
            implements Subscriber<ByteBuffer>
    {
        private int offset;

        private final CompletableFuture<ChunkDataHolder> future;
        private final Slice slice;

        private Subscription subscription;

        BufferSubscriber(Slice slice, CompletableFuture<ChunkDataHolder> future)
        {
            this.future = requireNonNull(future, "future is null");
            this.slice = requireNonNull(slice, "slice is null");
        }

        @Override
        public void onSubscribe(Subscription s)
        {
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer)
        {
            invokeSafely(() -> {
                int readableBytes = byteBuffer.remaining();
                if (byteBuffer.hasArray()) {
                    arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), slice.byteArray(), offset, readableBytes);
                }
                else {
                    byteBuffer.asReadOnlyBuffer().get(slice.byteArray(), offset, readableBytes);
                }
                offset += readableBytes;
            });
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable)
        {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onComplete()
        {
            verify(offset == slice.length(), "offset %s isn't equal to slice.length() %s", offset, slice.length());
            future.complete(getChunkDataHolder(slice));
        }
    }

    static <ResponseT> AsyncResponseTransformer<ResponseT, ChunkDataHolder> toChunkDataHolder(Slice slice)
    {
        return new ChunkDataAsyncResponseTransformer<>(slice);
    }
}
