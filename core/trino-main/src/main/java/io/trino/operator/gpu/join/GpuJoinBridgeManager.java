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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.ReferenceCount;
import io.trino.spi.gpu.borrow.Borrow;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

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
 *   <li>Each probe operator registers via {@link #probeOperatorCreated()} when it is created,
 *       acquiring one reference either eagerly (if the bridge is already published) or through
 *       the initial refCount baked in at publication time.</li>
 *   <li>Each probe operator releases its reference by calling {@link #probeOperatorClosed}
 *       from its {@code close()} method.</li>
 *   <li>The manager holds one extra seed reference from publication until
 *       {@link #probeOperatorFactoryClosed()} is called (from the probe factory's
 *       {@code noMoreOperators()}), covering the case where the build finishes before any
 *       probe operators have been created.</li>
 * </ul>
 */
public final class GpuJoinBridgeManager
{
    private final SettableFuture<GpuJoinBridge> bridgeFuture = SettableFuture.create();

    @GuardedBy("this")
    private boolean bridgePublished;
    /**
     * Number of probe operators registered before the bridge was published.
     */
    @GuardedBy("this")
    private int probeOperatorCount;
    /**
     * True when the probe operator factory has been closed (no more operators will be created).
     */
    @GuardedBy("this")
    private boolean probeFactoryClosed;

    /**
     * Registers a probe operator that will use the bridge.
     * <p>
     * Every registered operator must eventually call {@link #probeOperatorClosed}.
     */
    private synchronized void probeOperatorCreated()
    {
        checkState(!probeFactoryClosed, "probeOperatorFactoryClosed already called");
        if (bridgePublished) {
            getDone(bridgeFuture).retain();
        }
        else {
            probeOperatorCount++;
        }
    }

    /**
     * Releases the reference held by a probe operator. Must be called exactly once per
     * {@link #probeOperatorCreated()} call, from {@link GpuLookupJoin#close()}.
     */
    public void probeOperatorClosed()
    {
        GpuJoinBridge bridgeToRelease;
        synchronized (this) {
            if (!bridgePublished) {
                probeOperatorCount--;
                return;
            }
            bridgeToRelease = getDone(bridgeFuture);
        }
        bridgeToRelease.release();
    }

    /**
     * Signals that no more probe operators will be created. Releases the seed reference
     * held by the manager since bridge publication, allowing the bridge to be freed once
     * all probe operators have also released their references.
     * <p>
     * Must be called exactly once, from the probe factory's {@code noMoreOperators()}.
     */
    public void probeOperatorFactoryClosed()
    {
        GpuJoinBridge bridgeToRelease;
        synchronized (this) {
            checkState(!probeFactoryClosed, "probeOperatorFactoryClosed already called");
            probeFactoryClosed = true;
            if (!bridgePublished) {
                // Build has not finished yet; publishBridge() will release the seed.
                return;
            }
            bridgeToRelease = getDone(bridgeFuture);
        }
        bridgeToRelease.release();
    }

    public ListenableFuture<GpuJoinBridge> getBridgeFuture()
    {
        probeOperatorCreated();
        return nonCancellationPropagating(bridgeFuture);
    }

    public void publishBridge(@Nullable @Borrow HashJoin hashJoin, @Nullable @Borrow Table buildOutputTable, Runnable onRelease)
    {
        GpuJoinBridge bridge = new GpuJoinBridge(hashJoin, buildOutputTable, onRelease);
        boolean probeFactoryClosed;
        synchronized (this) {
            checkState(!bridgePublished, "Bridge already published");
            bridgePublished = true;
            // Acquire one ref per probe operator registered before publish; the bridge was
            // created with initialRefCount=1 (seed only) and needs probeOperatorCount more.
            for (int i = 0; i < probeOperatorCount; i++) {
                bridge.retain();
            }
            probeFactoryClosed = this.probeFactoryClosed;
        }
        // Set the future outside the monitor so listeners don't fire while we hold the lock.
        bridgeFuture.set(bridge);
        if (probeFactoryClosed) {
            bridge.release();
        }
    }

    public static final class GpuJoinBridge
    {
        private final @Nullable @Borrow HashJoin hashJoin;
        private final @Nullable @Borrow Table buildOutputTable;
        private final ReferenceCount refCount;

        private GpuJoinBridge(
                @Nullable @Borrow HashJoin hashJoin,
                @Nullable @Borrow Table buildOutputTable,
                Runnable onRelease)
        {
            this.hashJoin = hashJoin;
            this.buildOutputTable = buildOutputTable;
            this.refCount = new ReferenceCount(1);
            this.refCount.getFreeFuture().addListener(requireNonNull(onRelease, "onRelease is null"), directExecutor());
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

        private void retain()
        {
            refCount.retain();
        }

        private void release()
        {
            refCount.release();
        }
    }
}
