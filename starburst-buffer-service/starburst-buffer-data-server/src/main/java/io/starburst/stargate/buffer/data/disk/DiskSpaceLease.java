/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DiskSpaceLease
{
    private final LocalDiskAllocator allocator;
    private final long bytes;
    private final AtomicBoolean released = new AtomicBoolean();

    DiskSpaceLease(LocalDiskAllocator allocator, long bytes)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.bytes = bytes;
    }

    public void release()
    {
        checkState(released.compareAndSet(false, true), "already released");
        allocator.release(bytes);
    }
}
