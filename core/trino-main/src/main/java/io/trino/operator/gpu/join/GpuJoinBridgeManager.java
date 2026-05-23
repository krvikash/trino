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

import ai.rapids.cudf.Cuda;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.ReferenceCount;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public final class GpuJoinBridgeManager
{
    private final ReferenceCount referenceCount = new ReferenceCount(1 /* for probe operator factory */);
    private final SettableFuture<GpuJoinBridge> bridgeFuture = SettableFuture.create();

    public void probeOperatorFactoryDuplicated()
    {
        referenceCount.retain();
    }

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

    public void publishBridge(GpuJoinBridge bridge, Runnable onRelease)
    {
        requireNonNull(bridge, "bridge is null");
        checkState(!bridgeFuture.isDone(), "Bridge future already done");
        // Sync to ensure full results visibility for consuming threads.
        Cuda.DEFAULT_STREAM.sync();
        bridgeFuture.set(bridge);
        referenceCount.getFreeFuture().addListener(onRelease, directExecutor());
    }
}
