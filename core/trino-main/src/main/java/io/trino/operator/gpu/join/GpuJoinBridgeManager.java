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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.gpu.borrow.Own;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getDone;

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
     * Registers a probe operator that will use the bridge. Must be called exactly once per
     * {@link GpuLookupJoin} instance, from {@link GpuLookupJoin.Factory#create}.
     * <p>
     * If the bridge has already been published, this method acquires an additional reference
     * immediately. Otherwise, the reference is pre-allocated as part of the initial refCount
     * that {@link #publishBridge} will set.
     * <p>
     * Every registered operator must eventually call {@link #probeOperatorClosed}.
     */
    public synchronized void probeOperatorCreated()
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
        return bridgeFuture;
    }

    /**
     * Called by the build driver once it has assembled the {@link GpuJoinBridge}. Acquires
     * one reference per registered probe operator (plus the seed), then publishes the bridge
     * by completing {@link #getBridgeFuture()}.
     * <p>
     * {@code bridgeFuture.set()} is called outside the monitor so that any listeners
     * attached to the future do not fire while the lock is held.
     */
    public void publishBridge(@Own GpuJoinBridge bridge)
    {
        boolean seedRelease;
        synchronized (this) {
            checkState(!bridgePublished, "Bridge already published");
            bridgePublished = true;
            // Acquire one ref per probe operator registered before publish; the bridge was
            // created with initialRefCount=1 (seed only) and needs probeOperatorCount more.
            for (int i = 0; i < probeOperatorCount; i++) {
                bridge.retain();
            }
            seedRelease = probeFactoryClosed;
        }
        // Set the future outside the monitor so listeners don't fire while we hold the lock.
        bridgeFuture.set(bridge);
        if (seedRelease) {
            bridge.release();
        }
    }
}
