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
package io.trino.operator.gpu;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.annotation.NotThreadSafe;
import io.trino.operator.Operator;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static java.util.Objects.requireNonNull;

/**
 * GPU operation typically in a pull-based execution pipeline.
 *
 * @see Operator
 * @see io.trino.operator.WorkProcessor
 */
@NotThreadSafe
public interface GpuOperation
        extends RuntimeCloseable
{
    /**
     * Execute this operation and return result indicating the operation state.
     *
     * @see Result
     */
    @Move
    Result execute();

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
    record Blocked(ListenableFuture<Void> future)
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
    record Data(@Own GpuPage page)
            implements Result
    {
        public Data
        {
            requireNonNull(page, "page is null");
        }
    }

    interface Factory
    {
        GpuOperation create(GpuOperation source);

        Factory duplicate();

        /**
         * Called when the enclosing operator factory is closed and no more operators will be created.
         * Implementations may release shared resources that were held in anticipation of further operators.
         */
        void noMoreOperators();
    }
}
