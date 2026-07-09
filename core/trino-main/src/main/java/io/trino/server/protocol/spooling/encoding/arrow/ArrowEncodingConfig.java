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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ArrowEncodingConfig
{
    private DataSize allocatorMemoryLimit = DataSize.of(256, MEGABYTE);
    private DataSize maxBatchSize = DataSize.of(32, MEGABYTE);

    public DataSize getAllocatorMemoryLimit()
    {
        return allocatorMemoryLimit;
    }

    @Config("protocol.spooling.encoding.arrow.memory-limit")
    @ConfigHidden
    public ArrowEncodingConfig setAllocatorMemoryLimit(DataSize allocatorMemoryLimit)
    {
        this.allocatorMemoryLimit = allocatorMemoryLimit;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("protocol.spooling.encoding.arrow.max-batch-size")
    @ConfigDescription("Target size of a single Arrow record batch; result pages are split into row ranges so wide tables do not allocate all columns for the whole page at once")
    @ConfigHidden
    public ArrowEncodingConfig setMaxBatchSize(DataSize maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }
}
