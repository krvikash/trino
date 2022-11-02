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

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

/**
 * Lease class for slice allocated from {@link MemoryAllocator}.
 *
 * It is obligatory for the calling party to release all the leases they obtained via {@link SliceLease#release()}.
 */
public class SliceLease
{
    private final MemoryAllocator memoryAllocator;
    private final Slice slice;

    public SliceLease(
            MemoryAllocator memoryAllocator,
            Slice slice)
    {
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.slice = requireNonNull(slice, "slice is null");
    }

    public Slice getSlice()
    {
        return slice;
    }

    public void release()
    {
        memoryAllocator.release(slice);
    }
}
