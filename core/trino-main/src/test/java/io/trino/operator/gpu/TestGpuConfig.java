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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.DataSize;
import io.trino.operator.gpu.GpuConfig.AllocationMode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestGpuConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GpuConfig.class)
                .setAllocationMode(AllocationMode.ASYNC)
                .setPoolSize(null)
                .setAllocFraction(1.0)
                .setReserve(DataSize.of(640, MEGABYTE))
                .setAggregationCompactionThreshold(DataSize.of(4, GIGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gpu.memory.allocation-mode", "POOL")
                .put("gpu.memory.pool-size", "20GB")
                .put("gpu.memory.alloc-fraction", "0.75")
                .put("gpu.memory.reserve", "1GB")
                .put("gpu.aggregation.compaction-threshold", "2GB")
                .buildOrThrow();

        GpuConfig expected = new GpuConfig()
                .setAllocationMode(AllocationMode.POOL)
                .setPoolSize(DataSize.of(20, GIGABYTE))
                .setAllocFraction(0.75)
                .setReserve(DataSize.of(1, GIGABYTE))
                .setAggregationCompactionThreshold(DataSize.of(2, GIGABYTE));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testAllHiddenForNow()
    {
        // TODO (https://starburstdata.atlassian.net/browse/ENG-9839) officialize config toggles and remove this test
        for (Method method : GpuConfig.class.getMethods()) {
            if (method.isAnnotationPresent(Config.class) && !method.isAnnotationPresent(ConfigHidden.class)) {
                throw new IllegalArgumentException("Methods annotated with @Config must be annotated with @ConfigHidden for now and this one is not: " + method);
            }
        }
    }
}
