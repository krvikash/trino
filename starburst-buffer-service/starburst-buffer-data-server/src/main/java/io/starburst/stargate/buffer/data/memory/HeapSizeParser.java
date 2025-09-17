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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import io.airlift.units.DataSize;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.util.Objects.requireNonNull;

public class HeapSizeParser
{
    private static final String RELATIVE_SUFFIX = "%";

    // Memoize the max heap memory to avoid calling Runtime.getRuntime().maxMemory() multiple times
    // and to ensure that the value will not change over JVM lifetime.
    private static final Supplier<Long> AVAILABLE_HEAP_MEMORY = Suppliers.memoize(Runtime.getRuntime()::maxMemory);

    public static final HeapSizeParser DEFAULT = new HeapSizeParser(AVAILABLE_HEAP_MEMORY);

    private final Supplier<Long> maxHeapMemory;

    @VisibleForTesting
    HeapSizeParser(Supplier<Long> maxHeapMemory)
    {
        this.maxHeapMemory = requireNonNull(maxHeapMemory, "maxHeapMemory is null");
    }

    public DataSize parse(String value)
    {
        long maxHeapMemory = this.maxHeapMemory.get();
        checkState(maxHeapMemory > 0, "maxHeapMemory must be positive");

        if (value.endsWith(RELATIVE_SUFFIX)) {
            double multiplier = parseDouble(value.substring(0, value.length() - RELATIVE_SUFFIX.length()).trim()) / 100.0;
            return checkHeapSizeMemory(DataSize.ofBytes(Math.round(maxHeapMemory * multiplier)).succinct(), maxHeapMemory);
        }

        return checkHeapSizeMemory(DataSize.valueOf(value), maxHeapMemory);
    }

    private static DataSize checkHeapSizeMemory(DataSize heapSize, long maxHeapMemory)
    {
        checkArgument(heapSize.toBytes() <= maxHeapMemory, "Heap size cannot be greater than maximum heap size");
        checkArgument(heapSize.toBytes() > 0, "Heap size cannot be less than or equal to 0");
        return heapSize;
    }
}
