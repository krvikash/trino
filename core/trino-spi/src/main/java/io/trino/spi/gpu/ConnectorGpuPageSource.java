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
package io.trino.spi.gpu;

import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public interface ConnectorGpuPageSource
        extends RuntimeCloseable
{
    /**
     * Execute this operation and return result indicating the operation state.
     *
     * @see Result
     */
    @Move
    Result readNext();

    /**
     * Gets the number of input bytes read from the input so far.
     * If size is not available, this method should return zero.
     */
    long getCompletedBytes();

    /**
     * Gets the number of input rows read from the input so far.
     */
    default OptionalLong getCompletedPositions()
    {
        return OptionalLong.empty();
    }

    /**
     * Gets the wall time spent reading data from the input.
     * If read time is not available, this method should return zero.
     */
    long getReadTimeNanos();

    /**
     * {@inheritDoc}
     * <p>
     * Implementation must be idempotent.
     */
    @Override
    void close();

    /**
     * Result of executing an operation.
     */
    sealed interface Result {}

    /**
     * Operation is blocked waiting for a resource or condition.
     * <p>
     * Caller should wait on the future before retrying execute().
     * The future completes when the blocking condition is resolved.
     */
    record Blocked(CompletableFuture<Void> future)
            implements Result
    {
        public Blocked
        {
            requireNonNull(future, "future is null");
        }
    }

    /**
     * Operation is finished and will produce no more data.
     * <p>
     * Subsequent execute() calls will continue to return Finished.
     */
    record Finished()
            implements Result {}

    /**
     * Operation is yielding.
     * <p>
     * Caller should retry execute().
     */
    record Yielded()
            implements Result {}

    /**
     * Operation produced data in the form of a GpuPage.
     */
    record Data(@Own MemoryAllocation allocation, @Own GpuPage page)
            implements Result
    {
        public Data
        {
            requireNonNull(allocation, "allocation is null");
            requireNonNull(page, "page is null");
        }
    }
}
