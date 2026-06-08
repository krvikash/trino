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
import jakarta.validation.constraints.AssertTrue;
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
    private DataSize deviceMemoryReserve = DataSize.of(640, MEGABYTE);
    private double deviceMemoryFraction = 1.0;

    private DataSize offHeapMemoryPoolSize = DataSize.of(8, GIGABYTE);

    private Optional<DataSize> maxQueryGpuMemoryPerNode = Optional.empty();
    private Optional<DataSize> maxQueryOffHeapMemoryPerNode = Optional.empty();

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

    @NotNull
    public DataSize getDeviceMemoryReserve()
    {
        return deviceMemoryReserve;
    }

    @Config("gpu.memory.device-memory-reserve")
    @ConfigDescription("Device memory held back from the pool for non-RMM allocations (kernel launches, cuBLAS workspace, etc.)")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setDeviceMemoryReserve(DataSize deviceMemoryReserve)
    {
        this.deviceMemoryReserve = deviceMemoryReserve;
        return this;
    }

    @DecimalMin("0")
    @DecimalMax("1")
    public double getDeviceMemoryFraction()
    {
        return deviceMemoryFraction;
    }

    @Config("gpu.memory.device-memory-fraction")
    @ConfigDescription("Fraction of (free - reserve) device memory to place in the RMM pool")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setDeviceMemoryFraction(double deviceMemoryFraction)
    {
        this.deviceMemoryFraction = deviceMemoryFraction;
        return this;
    }

    @NotNull
    public DataSize getOffHeapMemoryPoolSize()
    {
        return offHeapMemoryPoolSize;
    }

    @Config("memory.off-heap.pool-size")
    @ConfigDescription("Maximum off-heap host memory the worker may reserve across all queries")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setOffHeapMemoryPoolSize(DataSize offHeapMemoryPoolSize)
    {
        this.offHeapMemoryPoolSize = offHeapMemoryPoolSize;
        return this;
    }

    public Optional<DataSize> getMaxQueryGpuMemoryPerNode()
    {
        return maxQueryGpuMemoryPerNode;
    }

    @Config("query.max-gpu-memory-per-node")
    @ConfigDescription("Maximum GPU device memory a single query may reserve on this node")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setMaxQueryGpuMemoryPerNode(DataSize maxQueryGpuMemoryPerNode)
    {
        this.maxQueryGpuMemoryPerNode = Optional.ofNullable(maxQueryGpuMemoryPerNode);
        return this;
    }

    @NotNull
    public DataSize getMaxQueryOffHeapMemoryPerNode()
    {
        return maxQueryOffHeapMemoryPerNode.orElse(offHeapMemoryPoolSize);
    }

    @Config("query.max-off-heap-memory-per-node")
    @ConfigDescription("Maximum off-heap host memory a single query may reserve on this node")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuConfig setMaxQueryOffHeapMemoryPerNode(DataSize maxQueryOffHeapMemoryPerNode)
    {
        this.maxQueryOffHeapMemoryPerNode = Optional.ofNullable(maxQueryOffHeapMemoryPerNode);
        return this;
    }

    @AssertTrue(message = "query.max-off-heap-memory-per-node must not exceed memory.off-heap.pool-size")
    public boolean isOffHeapQueryLimitWithinPool()
    {
        return getMaxQueryOffHeapMemoryPerNode().toBytes() <= getOffHeapMemoryPoolSize().toBytes();
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
