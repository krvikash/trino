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
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PrintMetricsTimerTaskTest
{
    private static final String CATALOG_NAME = "catalog-name";
    private MetricsManager metricsManager;
    private PrintMetricsTimerTask printMetricsTimerTask;
    private MetricsRegistry metricsRegistry;

    @BeforeEach
    public void before()
    {
        MetricsConfig metricsConfig = new MetricsConfig();
        metricsRegistry = new MetricsRegistry(new CatalogNameProvider(CATALOG_NAME), metricsConfig);
        metricsManager = new MetricsManager(metricsRegistry);
        printMetricsTimerTask = new PrintMetricsTimerTask(
                metricsConfig,
                metricsManager,
                new CatalogNameProvider("warp"),
                new ScheduledMetricsHandler(),
                new ShapingLoggerFactory(new CatalogName(CATALOG_NAME), new SharedConfig()));
    }

    @AfterEach
    public void after()
    {
        metricsRegistry.getAll().values().forEach(stat -> metricsManager.unregisterMetric(stat.getJmxKey()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDumpStats()
    {
        metricsManager.registerMetric(WarmupDemoterStats.create());
        WarmingServiceStats warmingService = metricsManager.registerMetric(WarmingServiceStats.create());
        metricsManager.registerMetric(DispatcherPageSourceStats.create());
        metricsManager.registerMetric(WarmupExportServiceStats.create());
        metricsManager.registerMetric(WarmupImportServiceStats.create());
        warmingService.incdeleted_row_group_count();
        Map<String, Object> fullJson = printMetricsTimerTask.buildJsonDump(Optional.empty());
        Map<String, Object> metrics = (Map<String, Object>) fullJson.get(PrintMetricsTimerTask.STATS);
        assertThat(metrics.containsKey(metricsRegistry.getKey(WarmupExportServiceStats.createKey()))).isTrue();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WarmupImportServiceStats.createKey()))).isTrue();
        warmingService.incdeleted_row_group_count();
        warmingService.adddeleted_warmup_elements_count(-5);
        metrics = printMetricsTimerTask.getMetricsDump();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WarmingServiceStats.createKey()))).isTrue();
        Map<String, Map<String, Long>> m = (Map<String, Map<String, Long>>) metrics.get(metricsRegistry.getKey(WarmingServiceStats.createKey()));
        Map<String, Long> diffPositive = m.get("deleted_row_group_count");
        assertThat(diffPositive.get("d").equals(1L)).isTrue();
        assertThat(diffPositive.get("t").equals(2L)).isTrue();
        Map<String, Long> diffNegative = m.get("deleted_warmup_elements_count");
        assertThat(diffNegative.get("d").equals(-5L)).isTrue();
        assertThat(diffNegative.get("t").equals(-5L)).isTrue();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WarmupExportServiceStats.createKey()))).isTrue();
    }
}
