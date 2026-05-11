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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.ReferenceCount;
import io.trino.spi.gpu.borrow.Borrow;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Lifecycle coordinator for the {@link GpuJoinBridge} shared between the build-side
 * {@link GpuJoinBuild} driver and probe-side {@link GpuLookupJoin} operators.
 * <p>
 * The build driver calls {@link #publishBridge} once it has assembled the
 * {@link GpuJoinBridge}. This class handles reference counting and completes
 * {@link #getBridgeFuture()} so probe operators can unblock.
 * <p>
 * Bridge lifetime is managed via per-operator references plus a seed reference:
 * <ul>
 *   <li>Each probe operator registers via {@link #getBridgeFuture()} when it is created.</li>
 *   <li>Each probe operator releases its reference by calling {@link #probeOperatorClosed}.</li>
 *   <li>The probe operator factory signals via {@link #probeOperatorFactoryClosed()} that
 *   there will not be any new probe operators created.</li>
 * </ul>
 */
public final class GpuJoinBridgeManager
{
    private final ReferenceCount referenceCount = new ReferenceCount(1 /* for probe operator factory */);
    private final SettableFuture<GpuJoinBridge> bridgeFuture = SettableFuture.create();

    public void probeOperatorFactoryClosed()
    {
        referenceCount.release();
    }

    /**
     * Caller must call {@link #probeOperatorClosed()} exactly once after it is done using the bridge.
     */
    public ListenableFuture<GpuJoinBridge> getBridgeFuture()
    {
        referenceCount.retain();
        return nonCancellationPropagating(bridgeFuture);
    }

    public void probeOperatorClosed()
    {
        referenceCount.release();
    }

    public void publishBridge(@Nullable @Borrow HashJoin hashJoin, @Nullable @Borrow Table buildOutputTable, Runnable onRelease)
    {
        checkState(!bridgeFuture.isDone(), "Bridge future already done");
        bridgeFuture.set(new GpuJoinBridge(hashJoin, buildOutputTable));
        referenceCount.getFreeFuture().addListener(onRelease, directExecutor());
    }

    public static final class GpuJoinBridge
    {
        private final @Nullable @Borrow HashJoin hashJoin;
        private final @Nullable @Borrow Table buildOutputTable;

        private GpuJoinBridge(
                @Nullable @Borrow HashJoin hashJoin,
                @Nullable @Borrow Table buildOutputTable)
        {
            this.hashJoin = hashJoin;
            this.buildOutputTable = buildOutputTable;
        }

        /**
         * @return the cuDF hash join, or null when the build side is empty
         */
        public @Nullable @Borrow HashJoin hashJoin()
        {
            return hashJoin;
        }

        /**
         * @return the build-side output table to gather from, or null when the join has no
         * build-side output columns or build side is empty
         */
        public @Nullable @Borrow Table buildOutputTable()
        {
            return buildOutputTable;
        }
    }
}
