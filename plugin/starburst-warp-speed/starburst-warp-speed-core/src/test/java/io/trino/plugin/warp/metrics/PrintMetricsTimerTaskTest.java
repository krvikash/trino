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
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.STATS_DISPATCHER_KEY;
import static io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
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
        printMetricsTimerTask = new PrintMetricsTimerTask(metricsConfig,
                metricsManager,
                new CatalogNameProvider("warp"));
    }

    @AfterEach
    public void after()
    {
        metricsRegistry.getAll().values().forEach((stat) -> metricsManager.unregisterMetric(stat.getJmxKey()));
    }

    @Test
    public void testDumpStats()
    {
        metricsManager.registerMetric(WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP));
        WarmingServiceStats warmingService = metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
        metricsManager.registerMetric(DispatcherPageSourceStats.create(STATS_DISPATCHER_KEY));
        metricsManager.registerMetric(WarmupExportServiceStats.create(WARMUP_EXPORTER_STAT_GROUP));
        metricsManager.registerMetric(WarmupImportServiceStats.create(WARMUP_IMPORTER_STAT_GROUP));
        warmingService.incdeleted_row_group_count();
        Map<String, Object> fullJson = printMetricsTimerTask.buildJsonDump();
        Map<String, Object> metrics = (Map<String, Object>) fullJson.get(PrintMetricsTimerTask.STATS);
        assertThat(metrics.containsKey(metricsRegistry.getKey(WARMUP_DEMOTER_STAT_GROUP))).isTrue();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WARMING_SERVICE_STAT_GROUP))).isTrue();
        warmingService.incdeleted_row_group_count();
        warmingService.adddeleted_warmup_elements_count(-5);
        metrics = printMetricsTimerTask.getMetricsDump();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WARMING_SERVICE_STAT_GROUP))).isTrue();
        Map m = (Map) metrics.get(metricsRegistry.getKey(WARMING_SERVICE_STAT_GROUP));
        assertThat(m.get("deleted_row_group_count").equals("+1 (2)")).isTrue();
        assertThat(m.get("deleted_warmup_elements_count").equals("-5 (-5)")).isTrue();
        assertThat(metrics.containsKey(metricsRegistry.getKey(WARMUP_EXPORTER_STAT_GROUP))).isTrue();
    }
}
