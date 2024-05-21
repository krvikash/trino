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
package io.trino.plugin.warp.execution.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.gen.stats.TestStats;
import io.trino.plugin.warp.metrics.MetricsRegistry;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestsStats
{
    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void init()
    {
        objectMapper = new ObjectMapperProvider().get();
    }

    @Test
    public void testSerializedStatObject()
            throws JsonProcessingException
    {
        TestStats dummyNotInNode = new TestStats("group2");
        dummyNotInNode.addparam1(9); //persist
        dummyNotInNode.addparam3(9); //not persist
        JsonNode jsonNode = objectMapper.readerFor(List.class).readTree(objectMapper.writeValueAsString(dummyNotInNode));
        String res = objectMapper.writeValueAsString(dummyNotInNode);
        assertThat(jsonNode.get("param1").asLong()).isEqualTo(9);
        TestStats deserializeObject = objectMapper.readerFor(TestStats.class).readValue(res);
        assertThat(deserializeObject.getparam1()).isEqualTo(9);
        assertThat(deserializeObject.getparam3()).isEqualTo(0);
    }

    @Test
    public void testMergePersistentStat()
    {
        MetricsRegistry metricRegistry = new MetricsRegistry(new CatalogNameProvider("catalog-name"), new MetricsConfig());
        long hotQueryVal = 50;
        long expectedResultAfterMerge = hotQueryVal * 2;
        TestStats stat = new TestStats("group2");
        stat.addparam1(hotQueryVal); //persist
        stat.addparam3(20); //persist
        metricRegistry.registerMetric(stat);

        metricRegistry.mergeMetrics(Lists.newArrayList(stat));

        TestStats materializedView = (TestStats) metricRegistry.get(stat.getJmxKey());
        assertThat(materializedView.getparam1()).isEqualTo(expectedResultAfterMerge);
        assertThat(materializedView.getparam3()).isEqualTo(20);
    }
}
