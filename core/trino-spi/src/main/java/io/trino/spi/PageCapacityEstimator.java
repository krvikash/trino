/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi;

import java.util.OptionalDouble;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Adaptive capacity sizing for page builders that emit pages in batches.
 * <p>
 * Tracks the maximum bytes-per-position seen across emitted pages and sizes the
 * next batch so its page stays under {@code maxPageSizeInBytes}. Capacity doubles
 * per emission, bounded by {@code maxEntries} and the byte budget.
 * <p>
 * {@link #currentCapacity()} and {@link #isFull(int)} are per-row reads;
 * {@link #recordPage(long, int)} writes once per emitted page.
 */
public final class PageCapacityEstimator
{
    private static final int BATCH_GROWTH_FACTOR = 2;

    private final int maxEntries;
    private final int maxPageSizeInBytes;

    private int capacity;
    private OptionalDouble maxAverageBytesPerPosition = OptionalDouble.empty();

    public PageCapacityEstimator(int initialCapacity, int maxEntries, int maxPageSizeInBytes)
    {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
        }
        if (maxEntries < initialCapacity) {
            throw new IllegalArgumentException("maxEntries (" + maxEntries + ") must be >= initialCapacity (" + initialCapacity + ")");
        }
        if (maxPageSizeInBytes <= 0) {
            throw new IllegalArgumentException("maxPageSizeInBytes must be positive: " + maxPageSizeInBytes);
        }
        this.capacity = initialCapacity;
        this.maxEntries = maxEntries;
        this.maxPageSizeInBytes = maxPageSizeInBytes;
    }

    public int currentCapacity()
    {
        return capacity;
    }

    public boolean isFull(int currentPositions)
    {
        return currentPositions >= capacity;
    }

    /**
     * Records one emitted page's byte/position ratio and sizes the next batch.
     * A non-positive {@code positionCount} is a no-op.
     */
    public void recordPage(long pageSizeInBytes, int positionCount)
    {
        if (positionCount <= 0) {
            return;
        }
        double avg = (double) pageSizeInBytes / positionCount;
        if (avg > 0) {
            maxAverageBytesPerPosition = OptionalDouble.of(
                    maxAverageBytesPerPosition.isEmpty()
                            ? avg
                            : max(maxAverageBytesPerPosition.getAsDouble(), avg));
        }
        capacity = computeNextCapacity(positionCount);
    }

    private int computeNextCapacity(int observedPositions)
    {
        if (maxAverageBytesPerPosition.isEmpty()) {
            return capacity;
        }
        if (maxAverageBytesPerPosition.getAsDouble() == 0) {
            return maxEntries;
        }
        int nextCapacity = (int) min(
                maxEntries,
                min((double) BATCH_GROWTH_FACTOR * observedPositions, maxPageSizeInBytes / maxAverageBytesPerPosition.getAsDouble()));
        return max(1, nextCapacity);
    }
}
