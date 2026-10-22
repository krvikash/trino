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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.ReferenceCount;
import io.trino.operator.torin.CudaStreams;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public final class GpuJoinBridgeManager
{
    private final boolean outerDrain;
    // Each probe operator factory and probe operator counts as 1
    private final ReferenceCount probeReferenceCount;
    // Each outer-drain factory and outer-drain operator counts as 1; kept apart since the drain waits for the probes
    private final ReferenceCount outerReferenceCount;
    private final ListenableFuture<Void> whenAllFinishes;

    private final SettableFuture<GpuJoinBridge> bridgeFuture = SettableFuture.create();
    // Set when a probe closes before finishing its input; an outer drain then emits nothing
    private final AtomicBoolean matchMaskIncomplete = new AtomicBoolean();

    public GpuJoinBridgeManager(boolean outerDrain)
    {
        this.outerDrain = outerDrain;
        this.probeReferenceCount = new ReferenceCount(1 /* for the probe operator factory */);
        this.outerReferenceCount = new ReferenceCount(outerDrain ? 1 /* for the outer-drain factory */ : 0);
        this.whenAllFinishes = whenAllSucceed(probeReferenceCount.getFreeFuture(), outerReferenceCount.getFreeFuture()).call(() -> null, directExecutor());
    }

    public boolean hasOuterDrain()
    {
        return outerDrain;
    }

    public void probeOperatorFactoryDuplicated()
    {
        probeReferenceCount.retain();
    }

    public void probeOperatorFactoryClosed()
    {
        probeReferenceCount.release();
    }

    /**
     * Caller must call {@link #probeOperatorFinished(boolean)} exactly once after it is done using the bridge.
     */
    public ListenableFuture<GpuJoinBridge> acquireBridgeForProbe()
    {
        probeReferenceCount.retain();
        return nonCancellationPropagating(bridgeFuture);
    }

    /// incomplete is true when the probe is torn down before the end of its input.
    public void probeOperatorFinished(boolean incomplete)
    {
        if (incomplete) {
            // Before the release, so a drain woken by it sees the flag
            matchMaskIncomplete.set(true);
        }
        // Sync so the probe is no longer using the build side
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        probeReferenceCount.release();
    }

    /// Fires once every probe has closed; only then may a drain read the mask.
    public ListenableFuture<Void> whenAllProbesFinished()
    {
        return nonCancellationPropagating(probeReferenceCount.getFreeFuture());
    }

    public boolean isMatchMaskIncomplete()
    {
        return matchMaskIncomplete.get();
    }

    public void outerDrainFactoryClosed()
    {
        outerReferenceCount.release();
    }

    /// The caller must call {@link #outerDrainOperatorFinished()} exactly once.
    public ListenableFuture<GpuJoinBridge> acquireBridgeForOuterDrain()
    {
        outerReferenceCount.retain();
        return nonCancellationPropagating(bridgeFuture);
    }

    public void outerDrainOperatorFinished()
    {
        // Sync so the drain is no longer using the build side
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        outerReferenceCount.release();
    }

    public void publishBridge(GpuJoinBridge bridge, Runnable onRelease)
    {
        requireNonNull(bridge, "bridge is null");
        checkState(!bridgeFuture.isDone(), "Bridge future already done");
        // Sync to ensure full results visibility for consuming threads.
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
        bridgeFuture.set(bridge);
        whenAllFinishes.addListener(onRelease, directExecutor());
    }
}
