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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestDynamicFilterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicFilterConfig.class)
                .setEnableDynamicFiltering(true)
                .setEnableDynamicRowFiltering(true)
                .setDynamicRowFilterSelectivityThreshold(0.7)
                .setSmallDynamicFilterWaitTimeout(new Duration(20, TimeUnit.SECONDS))
                .setSmallDynamicFilterMaxRowCount(100_000)
                .setSmallDynamicFilterMaxNdvCount(500)
                .setLargeMaxDistinctValuesPerDriver(50_000)
                .setLargeMaxSizePerDriver(DataSize.of(4, MEGABYTE))
                .setLargeMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(20_000)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(200, KILOBYTE))
                .setLargePartitionedMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargeMaxSizePerFilter(DataSize.of(10, MEGABYTE))
                .setBloomFilterMaxDistinctValuesPerDriver(100_000)
                .setPartitionedBloomFilterMaxDistinctValuesPerDriver(25_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("enable-dynamic-filtering", "false")
                .put("enable-dynamic-row-filtering", "false")
                .put("dynamic-row-filtering.selectivity-threshold", "0.8")
                .put("small-dynamic-filter.wait-timeout", "50s")
                .put("small-dynamic-filter.max-row-count", "500000")
                .put("small-dynamic-filter.max-ndv-count", "2000")
                .put("dynamic-filtering.large.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large.max-size-per-operator", "642kB")
                .put("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large-partitioned.max-size-per-operator", "643kB")
                .put("dynamic-filtering.large.max-size-per-filter", "3411kB")
                .put("dynamic-filtering.bloom-filter.max-distinct-values-per-driver", "15000")
                .put("dynamic-filtering.partitioned-bloom-filter.max-distinct-values-per-driver", "5000")
                .buildOrThrow();

        DynamicFilterConfig expected = new DynamicFilterConfig()
                .setEnableDynamicFiltering(false)
                .setEnableDynamicRowFiltering(false)
                .setDynamicRowFilterSelectivityThreshold(0.8)
                .setSmallDynamicFilterMaxRowCount(500_000)
                .setSmallDynamicFilterMaxNdvCount(2000)
                .setSmallDynamicFilterWaitTimeout(new Duration(50, TimeUnit.SECONDS))
                .setLargeMaxDistinctValuesPerDriver(256)
                .setLargeMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargeMaxSizePerOperator(DataSize.of(642, KILOBYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(256)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargePartitionedMaxSizePerOperator(DataSize.of(643, KILOBYTE))
                .setLargeMaxSizePerFilter(DataSize.of(3411, KILOBYTE))
                .setBloomFilterMaxDistinctValuesPerDriver(15000)
                .setPartitionedBloomFilterMaxDistinctValuesPerDriver(5000);

        assertFullMapping(properties, expected);
    }
}
