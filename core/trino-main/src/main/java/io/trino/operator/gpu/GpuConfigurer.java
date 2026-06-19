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
import ai.rapids.cudf.Rmm.LogConf;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.operator.gpu.GpuConfig.AllocationMode;
import jakarta.annotation.PostConstruct;

import java.nio.file.Path;
import java.util.Optional;

import static com.clearspring.analytics.util.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

public class GpuConfigurer
        implements GpuNodeSetup
{
    private static final Logger log = Logger.get(GpuConfigurer.class);

    // Static lock for tests' sake. Outside of tests there is single Guice context within a JVM.
    private static final Object initializationLock = new Object();

    // 512-byte alignment allegedly required by some RMM allocators
    private static final long ALIGNMENT_MASK = ~511L;

    private final AllocationMode allocationMode;
    private final Optional<DataSize> poolSize;
    private final DataSize deviceMemoryReserve;
    private final double deviceMemoryFraction;
    private final Optional<Path> rmmLogPath;
    private final DataSize aggregationCompactionThreshold;

    @Inject
    public GpuConfigurer(GpuConfig config, @RmmLogPath Optional<Path> rmmLogPath)
    {
        this.allocationMode = config.getAllocationMode();
        this.poolSize = config.getPoolSize();
        this.deviceMemoryReserve = config.getDeviceMemoryReserve();
        this.deviceMemoryFraction = config.getDeviceMemoryFraction();
        this.rmmLogPath = requireNonNull(rmmLogPath, "rmmLogPath is null");
        this.aggregationCompactionThreshold = config.getAggregationCompactionThreshold();
    }

    @PostConstruct
    void setup()
    {
        var _ = Rmm.isInitialized(); // trigger static initializer before taking the lock
        // PTDS (per thread default stream) affects multi-threading. Fail loud if new cudf dependency is built differently.
        checkState(Cuda.isPtdsEnabled(), "PTDS must be enabled in the cuDF native library; current build uses legacy default stream");
        synchronized (initializationLock) {
            if (Rmm.isInitialized()) {
                // This is normal in tests, but not normal in production
                log.warn("RMM is already initialized; skipping GPU configuration");
                return;
            }

            int cudfAllocationMode = allocationMode.cudfAllocationMode();
            long poolSize = poolSizeBytes();
            checkArgument(poolSize >= 0, "GPU pool size must not be negative, got %s bytes", poolSize);

            long compactionThreshold = aggregationCompactionThreshold.toBytes();
            checkArgument(
                    compactionThreshold <= poolSize,
                    "gpu.aggregation.compaction-threshold (%s) must not exceed GPU pool size (%s)",
                    succinctBytes(compactionThreshold),
                    succinctBytes(poolSize));

            log.info("Initializing RMM: allocationMode=%s, poolSize=%s", allocationMode, succinctBytes(poolSize));
            LogConf logConf = rmmLogPath
                    .map(path -> {
                        log.info("RMM log: %s", path);
                        return Rmm.logTo(path.toFile());
                    })
                    .orElse(null);
            Rmm.initialize(cudfAllocationMode, logConf, poolSize);
        }
    }

    private long poolSizeBytes()
    {
        if (poolSize.isPresent()) {
            return poolSize.get().toBytes();
        }

        CudaMemInfo info = Cuda.memGetInfo();
        long reserve = deviceMemoryReserve.toBytes();
        checkArgument(
                info.total >= reserve,
                "GPU total memory (%s) is smaller than reserve (%s)",
                succinctBytes(info.total),
                succinctBytes(reserve));
        long poolSize = (long) ((info.total - reserve) * deviceMemoryFraction);
        poolSize = poolSize & ALIGNMENT_MASK;
        checkState(
                info.free >= poolSize,
                "GPU free memory (%s) is smaller than effective pool size (%s)",
                succinctBytes(info.free),
                succinctBytes(poolSize));
        return poolSize;
    }

    @Override
    public boolean isNodeGpuExecutionEnabled()
    {
        return true;
    }
}
