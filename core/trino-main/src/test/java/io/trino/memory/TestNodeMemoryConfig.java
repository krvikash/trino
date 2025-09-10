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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestNodeMemoryConfig
{
    private static final long AVAILABLE_HEAP_MEMORY = Runtime.getRuntime().maxMemory();

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NodeMemoryConfig.class)
                .setMaxQueryMemoryPerNode(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3)).toString())
                .setHeapHeadroom(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3)).toString())
                .setRssMemorySampleInterval(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.max-memory-per-node", "1GB")
                .put("memory.heap-headroom-per-node", "1GB")
                .put("memory.rss-memory-monitor-interval", "10s")
                .buildOrThrow();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode("1GB")
                .setHeapHeadroom("1GB")
                .setRssMemorySampleInterval(new Duration(10, SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsWithRelativeValues()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.max-memory-per-node", "50%")
                .put("memory.heap-headroom-per-node", "25%")
                .put("memory.rss-memory-monitor-interval", "10m")
                .buildOrThrow();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.5)).toString())
                .setHeapHeadroom(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.25)).toString())
                .setRssMemorySampleInterval(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }
}
