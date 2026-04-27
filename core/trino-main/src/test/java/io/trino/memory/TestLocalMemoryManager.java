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

import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.memory.MemoryConfig;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocalMemoryManager
{
    @Test
    public void testNotEnoughAvailableMemory()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom("1MB")
                .setMaxQueryMemoryPerNode("4MB");

        // 4 MB heap is not sufficient for 1 MB heap headroom and 4 MB query.max-memory-per-node
        assertThatThrownBy(() -> new LocalMemoryManager(config, DataSize.of(4, MEGABYTE).toBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid memory configuration\\. The sum of max query memory per node .* heap headroom .*" +
                        "cannot be larger than the available heap memory .*");
    }

    @Test
    public void testNotEnoughAvailableMemoryWithBufferService()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom("1MB")
                .setMaxQueryMemoryPerNode("4MB");

        MemoryConfig bufferMemoryConfig = new MemoryConfig()
        {
            @Override
            public DataSize getBaseMemory()
            {
                return DataSize.of(2, MEGABYTE);
            }

            @Override
            public DataSize getChunksMemory()
            {
                return DataSize.ofBytes(0);
            }
        };

        // 6 MB heap is not sufficient for 1 MB heap headroom, 2 MB buffer service memory and 4 MB query.max-memory-per-node
        assertThatThrownBy(() -> new LocalMemoryManager(
                config,
                Optional.of(bufferMemoryConfig),
                DataSize.of(6, MEGABYTE).toBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid memory configuration\\. The sum of max query memory per node .* heap headroom .*" +
                        "cannot be larger than the available heap memory .*");

        LocalMemoryManager localMemoryManager = new LocalMemoryManager(config, Optional.of(bufferMemoryConfig), DataSize.of(10, MEGABYTE).toBytes());
        assertThat(localMemoryManager.getMemoryPool().getMaxBytes()).isEqualTo(DataSize.of(7, MEGABYTE).toBytes()); // 10MB heap - 1MB headroom - 2MB buffer service
    }
}
