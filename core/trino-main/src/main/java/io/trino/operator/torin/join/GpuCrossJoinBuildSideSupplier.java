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
package io.trino.operator.torin.join;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.ReferenceCount;
import io.trino.operator.torin.CudaStreams;
import io.trino.spi.torin.borrow.Borrow;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public final class GpuCrossJoinBuildSideSupplier
{
    public record GpuCrossJoinBuildSide(long rowCount, Optional<@Borrow Table> outputTable)
    {
        public GpuCrossJoinBuildSide
        {
            checkArgument(rowCount >= 0, "Invalid rowCount: %s", rowCount);
            requireNonNull(outputTable, "outputTable is null");
            checkArgument(rowCount > 0 || outputTable.isEmpty(), "outputTable must be empty when there are no build rows");
        }
    }

    private final ReferenceCount referenceCount = new ReferenceCount(1 /* for probe operator factory */);
    private final SettableFuture<GpuCrossJoinBuildSide> buildSideFuture = SettableFuture.create();

    public void probeOperatorFactoryDuplicated()
    {
        referenceCount.retain();
    }

    public void probeOperatorFactoryClosed()
    {
        referenceCount.release();
    }

    /**
     * Caller must call {@link #probeOperatorFinished()} exactly once after it is done using the build side.
     */
    public ListenableFuture<GpuCrossJoinBuildSide> getBuildSideFuture()
    {
        referenceCount.retain();
        return nonCancellationPropagating(buildSideFuture);
    }

    public void probeOperatorFinished()
    {
        // Sync to ensure that probe operator did indeed fully finish using the build side.
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        referenceCount.release();
    }

    public void publishBuildSide(GpuCrossJoinBuildSide buildSide, Runnable onRelease)
    {
        requireNonNull(buildSide, "buildSide is null");
        checkState(!buildSideFuture.isDone(), "Build side future already done");
        // Sync to ensure full results visibility for consuming threads.
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        buildSideFuture.set(buildSide);
        // When every probe operator and probe operator factory is already gone, for example after a
        // cancellation, the count is free and onRelease runs inline, releasing the build side right
        // after it was published. That is safe exactly because no probe is left to observe it.
        referenceCount.getFreeFuture().addListener(onRelease, directExecutor());
    }
}
