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

import static com.clearspring.analytics.util.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

public class GpuConfigurer
{
    private static final Logger log = Logger.get(GpuConfigurer.class);

    private static final Object initializationLock = new Object();

    // 512-byte alignment allegedly required by some RMM allocators
    private static final long ALIGNMENT_MASK = ~511L;

    @Inject
    public GpuConfigurer(GpuConfig config)
    {
        requireNonNull(config, "config is null");

        var _ = Rmm.isInitialized(); // trigger static initializer before taking the lock
        synchronized (initializationLock) {
            if (Rmm.isInitialized()) {
                // This is normal in tests, but not normal in production
                log.warn("RMM is already initialized; skipping GPU configuration");
                return;
            }

            int allocationMode = config.getAllocationMode().cudfAllocationMode();
            long poolSize = poolSizeBytes(config);
            checkArgument(poolSize >= 0, "GPU pool size must not be negative, got %s bytes", poolSize);

            log.info("Initializing RMM: allocationMode=%s, poolSize=%s; PTDS=%s", config.getAllocationMode(), succinctBytes(poolSize), Cuda.isPtdsEnabled());
            Rmm.initialize(allocationMode, null, poolSize);
        }
    }

    private static long poolSizeBytes(GpuConfig config)
    {
        if (config.getPoolSize().isPresent()) {
            return config.getPoolSize().get().toBytes();
        }

        CudaMemInfo info = Cuda.memGetInfo();
        long reserve = config.getReserve().toBytes();
        checkArgument(
                info.total >= reserve,
                "GPU total memory (%s) is smaller than reserve (%s)",
                succinctBytes(info.total),
                succinctBytes(reserve));
        long poolSize = (long) ((info.total - reserve) * config.getAllocFraction());
        poolSize = poolSize & ALIGNMENT_MASK;
        checkState(
                info.free >= poolSize,
                "GPU free memory (%s) is smaller than effective pool size (%s)",
                succinctBytes(info.free),
                succinctBytes(poolSize));
        return poolSize;
    }
}
