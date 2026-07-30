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
package io.trino.plugin.hive.parquet;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

public class GpuParquetConfig
{
    private DataSize maxPageSize = DataSize.of(512, DataSize.Unit.MEGABYTE);

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxPageSize()
    {
        return maxPageSize;
    }

    @Config("gpu.scan.max-page-size")
    @ConfigDescription("Maximum size of a single GPU page produced by the GPU Parquet scan; larger splits are decoded into multiple pages to bound device memory")
    @ConfigHidden // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles
    public GpuParquetConfig setMaxPageSize(DataSize maxPageSize)
    {
        this.maxPageSize = maxPageSize;
        return this;
    }
}
