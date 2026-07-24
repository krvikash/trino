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
package io.trino.plugin.warp.metrics;

import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomStatsContextTest
{
    private MetricsManager metricsManager;
    private CustomStatsContext customStatsContext;

    @BeforeEach
    public void before()
    {
        metricsManager = new MetricsManager(new MetricsRegistry(new CatalogNameProvider("catalog-name"), new MetricsConfig()));
        customStatsContext = new CustomStatsContext(metricsManager);
    }

    @Test
    public void getOrRegisterTest()
    {
        String jmxKey = "test";
        WarpStatsBase stats = new WarpTestStats(jmxKey);
        assertThat(customStatsContext.getStat(jmxKey)).isNull();
        customStatsContext.getOrRegister(stats);
        assertThat(customStatsContext.getStat(jmxKey)).isNotNull();

        WarpStatsBase statsNew = new WarpTestStats(jmxKey);
        // does not create new instance
        assertThat(customStatsContext.getOrRegister(statsNew)).isEqualTo(stats);
    }

    @Test
    public void copyStatsToGlobalMetricsManagerTest()
    {
        final String jmxKey = "test";
        WarpTestStats testStatsContext = new WarpTestStats(jmxKey);
        customStatsContext.getOrRegister(testStatsContext);
        WarpTestStats testStatsGlobal = new WarpTestStats(jmxKey);
        metricsManager.registerMetric(testStatsGlobal);
        testStatsContext.incCounter();
        customStatsContext.copyStatsToGlobalMetricsManager();
        assertThat(((WarpTestStats) metricsManager.get(jmxKey)).getCounter()).isEqualTo(((WarpTestStats) customStatsContext.getStat(jmxKey)).getCounter());

        CustomStatsContext customStatsContext2 = new CustomStatsContext(metricsManager);
        WarpTestStats testStatsContext2 = new WarpTestStats(jmxKey);
        customStatsContext2.getOrRegister(testStatsContext2);
        testStatsContext2.incCounter();
        customStatsContext2.copyStatsToGlobalMetricsManager();
        assertThat(((WarpTestStats) metricsManager.get(jmxKey)).getCounter()).isGreaterThan(((WarpTestStats) customStatsContext2.getStat(jmxKey)).getCounter());
    }

    static class WarpTestStats
            extends WarpStatsBase
    {
        final LongAdder counter = new LongAdder();

        WarpTestStats(String jmxKey)
        {
            super(jmxKey, WarpStatType.Worker);
        }

        public void incCounter()
        {
            this.counter.increment();
        }

        public long getCounter()
        {
            return this.counter.longValue();
        }

        @Override
        public void mergeStats(WarpStatsBase warpStatsBase)
        {
            if (warpStatsBase == null) {
                return;
            }
            WarpTestStats other = (WarpTestStats) warpStatsBase;
            this.counter.add(other.counter.longValue());
        }
    }
}
