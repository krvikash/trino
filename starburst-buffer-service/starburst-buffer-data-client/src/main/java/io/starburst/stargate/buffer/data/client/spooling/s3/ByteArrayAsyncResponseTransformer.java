/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

/**
 *  An implementation of {@link AsyncResponseTransformer} that dumps content into a byte array, with data length known beforehand.
 *  This class mimics the implementation of {@link software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer}
 *  but avoids memory copying.
 *
 * @param <ResponseT> Response POJO type.
 */
public final class ByteArrayAsyncResponseTransformer<ResponseT>
        implements AsyncResponseTransformer<ResponseT, byte[]>
{
    private final int length;

    private volatile CompletableFuture<byte[]> cf;

    public ByteArrayAsyncResponseTransformer(int length)
    {
        this.length = length;
    }

    @Override
    public CompletableFuture<byte[]> prepare()
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
        publisher.subscribe(new BufferSubscriber(length, cf));
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

        private final byte[] buffer;
        private final CompletableFuture<byte[]> future;

        private Subscription subscription;

        BufferSubscriber(int length, CompletableFuture<byte[]> future)
        {
            checkArgument(length > 0, "length should be larger than 0, but is %s", length);
            this.buffer = new byte[length];
            this.future = requireNonNull(future, "future is null");
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
                    arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), buffer, offset, readableBytes);
                }
                else {
                    byteBuffer.asReadOnlyBuffer().get(buffer, offset, readableBytes);
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
            verify(offset == buffer.length, "offset %s isn't equal to buffer length %s", offset, buffer.length);
            future.complete(buffer);
        }
    }

    static <ResponseT> AsyncResponseTransformer<ResponseT, byte[]> toByteArray(int length)
    {
        return new ByteArrayAsyncResponseTransformer<>(length);
    }
}
