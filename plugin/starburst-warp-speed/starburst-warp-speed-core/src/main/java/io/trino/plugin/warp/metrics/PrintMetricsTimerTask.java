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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.tools.CatalogNameProvider;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
public class PrintMetricsTimerTask
        extends MetricsTimerTask
{
    static final String STATS = "stats";

    private static final Logger dumpLogger = Logger.get("METRICS-DUMP");
    private static final Logger logger = Logger.get(PrintMetricsTimerTask.class);
    private static final String TIMESTAMP = "timestamp";
    private static final String CATALOG = "catalog";

    private final ShapingLogger shapingLogger;
    private final MetricsManager metricsManager;
    private final JsonMapper jsonMapper = new JsonMapperProvider().get();
    private final CatalogNameProvider catalogNameProvider;

    @Inject
    public PrintMetricsTimerTask(
            MetricsConfig metricsConfig,
            MetricsManager metricsManager,
            CatalogNameProvider catalogNameProvider,
            ScheduledMetricsHandler scheduledMetricsHandler,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(metricsConfig);
        this.metricsManager = requireNonNull(metricsManager);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        scheduledMetricsHandler.scheduleMetricsTimerTask(this);
        this.shapingLogger = shapingLoggerFactory.getInstance(
                this.getClass(),
                dumpLogger,
                Integer.MAX_VALUE,
                Duration.ofMinutes(1), // at maximum, we want to print once a minute
                1,
                ShapingLogger.MODE.FORMAT);

        logger.debug("PrintMetricsTimerTask constructor %s", catalogNameProvider.get());
    }

    @Override
    public Duration getInterval()
    {
        return metricsConfig.getPrintMetricsDuration();
    }

    @Override
    public void run()
    {
        print(true, Optional.empty());
    }

    public void print(boolean isScheduledPrint, Optional<String> message)
    {
        if (logger.isInfoEnabled()) {
            Map<String, Object> fullJson = buildJsonDump(message);
            if (!fullJson.isEmpty()) {
                try {
                    if (isScheduledPrint) {
                        dumpLogger.info(jsonMapper.writeValueAsString(fullJson));
                    }
                    else {
                        shapingLogger.info(jsonMapper.writeValueAsString(fullJson));
                    }
                }
                catch (JsonProcessingException e) {
                    logger.debug("failed writing metrics map %s", fullJson);
                }
            }
        }
    }

    Map<String, Object> buildJsonDump(Optional<String> message)
    {
        Map<String, Object> fullJson = new HashMap<>();
        Map<String, Object> metricsDump = getMetricsDump();
        if (!metricsDump.isEmpty()) {
            message.ifPresent(msg -> fullJson.put("MESSAGE", msg));
            fullJson.put(TIMESTAMP, System.currentTimeMillis());
            fullJson.put(STATS, metricsDump);
            fullJson.put(CATALOG, catalogNameProvider.get());
            MemoryUsage nonHeapMemoryUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
            fullJson.put("off-heap-committed", nonHeapMemoryUsage.getCommitted());
            fullJson.put("off-heap-used", nonHeapMemoryUsage.getUsed());
        }
        return fullJson;
    }

    Map<String, Object> getMetricsDump()
    {
        Map<String, Object> res = new HashMap<>();
        metricsManager.getAll().forEach((statsGroup, stats) -> {
            Map<String, Object> statsMap = stats.printStatsMap();
            if (!statsMap.isEmpty()) {
                String statsGroupName = statsGroup.replace("_" + catalogNameProvider.get(), "");
                res.put(statsGroupName, statsMap);
            }
        });
        return res;
    }
}
