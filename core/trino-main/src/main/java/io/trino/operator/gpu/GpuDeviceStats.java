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

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.CudaMemInfo;
import ai.rapids.cudf.Rmm;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.DistributionStat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * JMX-exported GPU memory metrics sourced from RMM ({@code ai.rapids.cudf.Rmm})
 * and CUDA ({@code ai.rapids.cudf.Cuda#memGetInfo}).
 *
 * <h2>Metrics exposed</h2>
 *
 * <ul>
 *   <li><b>Device total</b> ({@link #getDeviceTotalBytes()}): physical device
 *       memory capacity. Constant after initialization; read once at startup.</li>
 *   <li><b>RMM allocated</b> ({@link #getRmmAllocatedBytes()} and
 *       {@link #getRmmAllocatedDistribution()}): bytes currently held by active
 *       RMM allocations (logical layer). Sampled by a background poller to feed
 *       a {@link DistributionStat} histogram in addition to the point-in-time
 *       accessor.</li>
 *   <li><b>RMM peak</b> ({@link #getRmmPeakAllocatedBytes()}): high-water mark
 *       of RMM allocations since initialization. Point-in-time only.</li>
 *   <li><b>Device used</b> ({@link #getDeviceUsedBytes()} and
 *       {@link #getDeviceUsedDistribution()}): physical bytes in use
 *       ({@code total - free} from {@code cudaMemGetInfo}). Sampled alongside
 *       RMM allocated by the same background poller.</li>
 * </ul>
 *
 * <h2>RMM allocated vs. device used</h2>
 *
 * <p>These two metrics measure memory at different layers. RMM allocated is what
 * Trino's GPU operators have requested from RMM. Device used is everything
 * consuming physical device memory — including CUDA context overhead and any
 * non-RMM allocations. In async allocation mode ({@code CUDA_ASYNC}) there is no
 * fixed pre-allocated pool, so the async allocator retains or releases physical
 * memory independently of logical RMM allocations.
 *
 * <h2>Graceful degradation</h2>
 *
 * <p>All accessors return {@code -1} if RMM is not initialized or the underlying
 * CUDA call fails.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Bound as an eager singleton in {@code ServerMainModule} when GPU execution
 * is enabled. The sampling interval is configurable via
 * {@code gpu.device-stats.sampling-interval}; see {@link GpuConfig}.
 */
public class GpuDeviceStats
{
    private static final Logger log = Logger.get(GpuDeviceStats.class);

    private final long pollIntervalMillis;
    private final ScheduledExecutorService poller;

    /**
     * Physical device memory capacity — constant after init, cached at startup.
     */
    private volatile long deviceTotalBytes = -1;

    private final DistributionStat rmmAllocatedDistribution = new DistributionStat();
    private final DistributionStat deviceUsedDistribution = new DistributionStat();

    @Inject
    public GpuDeviceStats(GpuConfig config, @ForGpuDeviceStats ScheduledExecutorService poller)
    {
        requireNonNull(config, "config is null");
        this.pollIntervalMillis = config.getDeviceStatsSamplingInterval().toMillis();
        this.poller = requireNonNull(poller, "poller is null");
    }

    @PostConstruct
    public void start()
    {
        CudaMemInfo info = memGetInfo();
        if (info != null) {
            deviceTotalBytes = info.total;
        }
        poller.scheduleAtFixedRate(this::sample, 0, pollIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        poller.shutdownNow();
    }

    /**
     * Physical device memory capacity in bytes. Constant after initialization;
     * cached at startup. Returns {@code -1} if the initial {@code cudaMemGetInfo}
     * call failed.
     */
    @Managed
    public long getDeviceTotalBytes()
    {
        return deviceTotalBytes;
    }

    /**
     * Bytes currently held by active RMM allocations (logical layer).
     * Returns {@code -1} if the call fails.
     */
    @Managed
    public long getRmmAllocatedBytes()
    {
        try {
            return Rmm.getTotalBytesAllocated();
        }
        catch (Exception e) {
            log.warn(e, "Failed to read RMM allocated bytes");
            return -1;
        }
    }

    /**
     * Histogram of {@link #getRmmAllocatedBytes()} samples taken since startup.
     */
    @Managed
    @Nested
    public DistributionStat getRmmAllocatedDistribution()
    {
        return rmmAllocatedDistribution;
    }

    /**
     * Peak bytes simultaneously allocated by RMM since initialization.
     * Returns {@code -1} if the call fails.
     */
    @Managed
    public long getRmmPeakAllocatedBytes()
    {
        try {
            return Rmm.getMaximumTotalBytesAllocated();
        }
        catch (Exception e) {
            log.warn(e, "Failed to read RMM peak allocated bytes");
            return -1;
        }
    }

    /**
     * Physical device memory in use ({@code total - free}) from
     * {@code cudaMemGetInfo}. Returns {@code -1} if the call fails.
     */
    @Managed
    public long getDeviceUsedBytes()
    {
        CudaMemInfo info = memGetInfo();
        return info == null ? -1 : info.total - info.free;
    }

    /**
     * Histogram of {@link #getDeviceUsedBytes()} samples taken since startup.
     */
    @Managed
    @Nested
    public DistributionStat getDeviceUsedDistribution()
    {
        return deviceUsedDistribution;
    }

    /**
     * Poller body: samples RMM allocated and device used bytes and feeds both
     * {@link DistributionStat} histograms. On failure the tick is skipped and
     * the next scheduled tick tries again.
     */
    private void sample()
    {
        try {
            rmmAllocatedDistribution.add(Rmm.getTotalBytesAllocated());
        }
        catch (Exception e) {
            log.warn(e, "Failed to sample RMM allocated bytes");
        }

        CudaMemInfo info = memGetInfo();
        if (info != null) {
            deviceUsedDistribution.add(info.total - info.free);
        }
    }

    private static CudaMemInfo memGetInfo()
    {
        try {
            return Cuda.memGetInfo();
        }
        catch (Exception e) {
            log.warn(e, "cudaMemGetInfo failed");
            return null;
        }
    }
}
