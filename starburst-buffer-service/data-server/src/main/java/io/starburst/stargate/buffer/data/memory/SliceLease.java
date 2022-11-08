/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

/**
 * Lease class for slice allocated from {@link MemoryAllocator}. Obtained slice can be obtained via
 * {@link SliceLease#getSliceFuture} method. The slice may not be available immediately. Calling party needs to wait
 * until future returned is done.
 *
 * It is obligatory for the calling party to release all the leases they obtained via {@link SliceLease#release()}.
 */
public class SliceLease
{
    private final MemoryAllocator memoryAllocator;
    private final ListenableFuture<Slice> sliceFuture;
    private final AtomicBoolean released = new AtomicBoolean();

    public SliceLease(
            MemoryAllocator memoryAllocator,
            int sliceLength)
    {
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.sliceFuture = memoryAllocator.allocate(sliceLength);
    }

    public ListenableFuture<Slice> getSliceFuture()
    {
        return sliceFuture;
    }

    public void release()
    {
        checkState(released.compareAndSet(false, true), "already released");
        sliceFuture.cancel(true);
        if (sliceFuture.isDone() && !sliceFuture.isCancelled()) {
            memoryAllocator.release(getFutureValue(sliceFuture));
        }
    }
}
