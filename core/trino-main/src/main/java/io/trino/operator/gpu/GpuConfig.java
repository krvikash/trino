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

import ai.rapids.cudf.RmmAllocationMode;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class GpuConfig
{
    public enum AllocationMode
    {
        // The values model how the allocationMode bitmask is unpacked in ai.rapids.cudf.Rmm#initialize
        CUDA_DEFAULT(RmmAllocationMode.CUDA_DEFAULT),
        POOL(RmmAllocationMode.POOL),
        POOL_MANAGED(RmmAllocationMode.POOL | RmmAllocationMode.CUDA_MANAGED_MEMORY),
        ARENA(RmmAllocationMode.ARENA),
        ARENA_MANAGED(RmmAllocationMode.ARENA | RmmAllocationMode.CUDA_MANAGED_MEMORY),
        ASYNC(RmmAllocationMode.CUDA_ASYNC),
        ASYNC_FABRIC(RmmAllocationMode.CUDA_ASYNC_FABRIC),
        MANAGED(RmmAllocationMode.CUDA_MANAGED_MEMORY),
        /**/;

        private final int cudfAllocationMode;

        AllocationMode(int cudfAllocationMode)
        {
            this.cudfAllocationMode = cudfAllocationMode;
        }

        public int cudfAllocationMode()
        {
            return cudfAllocationMode;
        }
    }

    // Default ASYNC as recommended by NVIDIA team https://starburstdata.slack.com/archives/C0AHAUL34CC/p1776804643112849?thread_ts=1776415978.579519&cid=C0AHAUL34CC
    // according to https://starburstdata.slack.com/archives/C0AHAUL34CC/p1776889382679829?thread_ts=1776415978.579519&cid=C0AHAUL34CC, this does not need to be configurable
    private AllocationMode allocationMode = AllocationMode.ASYNC;
    private Optional<DataSize> poolSize = Optional.empty();
    private double allocFraction = 1.0;
    private DataSize reserve = DataSize.of(640, MEGABYTE);
    private DataSize aggregationCompactionThreshold = DataSize.of(4, GIGABYTE);

    @NotNull
    public AllocationMode getAllocationMode()
    {
        return allocationMode;
    }

    @Config("gpu.memory.allocation-mode")
    @ConfigDescription("RMM allocation mode used for GPU memory")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setAllocationMode(AllocationMode allocationMode)
    {
        this.allocationMode = allocationMode;
        return this;
    }

    @NotNull
    public Optional<DataSize> getPoolSize()
    {
        return poolSize;
    }

    @Config("gpu.memory.pool-size")
    @ConfigDescription("Explicit GPU memory pool size; auto-sized from free device memory when unset")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setPoolSize(DataSize poolSize)
    {
        this.poolSize = Optional.ofNullable(poolSize);
        return this;
    }

    @DecimalMin("0")
    @DecimalMax("1")
    public double getAllocFraction()
    {
        return allocFraction;
    }

    @Config("gpu.memory.alloc-fraction")
    @ConfigDescription("Fraction of (free - reserve) device memory to place in the RMM pool")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setAllocFraction(double allocFraction)
    {
        this.allocFraction = allocFraction;
        return this;
    }

    @NotNull
    public DataSize getReserve()
    {
        return reserve;
    }

    @Config("gpu.memory.reserve")
    @ConfigDescription("Device memory held back from the pool for non-RMM allocations (kernel launches, cuBLAS workspace, etc.)")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setReserve(DataSize reserve)
    {
        this.reserve = reserve;
        return this;
    }

    @NotNull
    public DataSize getAggregationCompactionThreshold()
    {
        return aggregationCompactionThreshold;
    }

    @Config("gpu.aggregation.compaction-threshold")
    @ConfigDescription("GPU memory threshold that triggers compaction in aggregation operators")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setAggregationCompactionThreshold(DataSize aggregationCompactionThreshold)
    {
        this.aggregationCompactionThreshold = aggregationCompactionThreshold;
        return this;
    }
}
