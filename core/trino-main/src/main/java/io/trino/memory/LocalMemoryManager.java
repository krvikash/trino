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
package io.trino.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.memory.MemoryConfig;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public final class LocalMemoryManager
{
    private final MemoryPool memoryPool;

    private static final Supplier<Integer> AVAILABLE_PROCESSORS = Suppliers
            .memoizeWithExpiration(Runtime.getRuntime()::availableProcessors, 30, TimeUnit.SECONDS);

    @Inject
    public LocalMemoryManager(NodeMemoryConfig config, Optional<MemoryConfig> bufferServiceMemoryConfig)
    {
        this(config, bufferServiceMemoryConfig, Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    public LocalMemoryManager(NodeMemoryConfig config)
    {
        this(config, Optional.empty(), Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    public LocalMemoryManager(NodeMemoryConfig config, long availableMemory)
    {
        this(config, Optional.empty(), availableMemory);
    }

    @VisibleForTesting
    public LocalMemoryManager(NodeMemoryConfig config, Optional<MemoryConfig> bufferServiceMemoryConfig, long availableMemory)
    {
        validateHeapHeadroom(config, bufferServiceMemoryConfig, availableMemory);
        long heapHeadRoom = config.getHeapHeadroom().toBytes();
        long bufferServiceMemory = bufferServiceMemoryConfig.map(MemoryConfig::getBaseMemory).orElse(DataSize.ofBytes(0)).toBytes();
        DataSize memoryPoolSize = DataSize.ofBytes(availableMemory - heapHeadRoom - bufferServiceMemory);
        verify(memoryPoolSize.toBytes() > 0, "memory pool size is 0");
        memoryPool = new MemoryPool(memoryPoolSize);
    }

    private void validateHeapHeadroom(NodeMemoryConfig config, Optional<MemoryConfig> bufferServiceMemoryConfig, long availableMemory)
    {
        long maxQueryTotalMemoryPerNode = config.getMaxQueryMemoryPerNode().toBytes();
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        long bufferServiceMemory = bufferServiceMemoryConfig.map(MemoryConfig::getBaseMemory).orElse(DataSize.ofBytes(0)).toBytes();
        // (availableMemory - maxQueryTotalMemoryPerNode) bytes will be available for the memory pool and the
        // headroom/untracked allocations, so the heapHeadroom cannot be larger than that space.
        if (heapHeadroom < 0 || bufferServiceMemory < 0 || heapHeadroom + bufferServiceMemory + maxQueryTotalMemoryPerNode > availableMemory) {
            throw new IllegalArgumentException(
                    format("Invalid memory configuration. The sum of max query memory per node (%s) heap headroom (%s) and buffer service memory (%s) cannot be larger than the available heap memory (%s)",
                            maxQueryTotalMemoryPerNode,
                            heapHeadroom,
                            bufferServiceMemory,
                            availableMemory));
        }
    }

    public MemoryInfo getInfo()
    {
        return new MemoryInfo(AVAILABLE_PROCESSORS.get(), memoryPool.getInfo());
    }

    public MemoryPool getMemoryPool()
    {
        return memoryPool;
    }
}
