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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import org.weakref.jmx.MBeanExporter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@Singleton
public class MetricsRegistry
{
    private static final Logger logger = Logger.get(MetricsRegistry.class);
    private final CatalogNameProvider catalogNameProvider;
    private final MBeanExporter exporter;
    private final MetricsConfig metricsConfig;
    private final Map<String, WarpStatsBase> metricsRegistry;

    @Inject
    MetricsRegistry(
            CatalogNameProvider catalogNameProvider,
            MBeanExporter exporter,
            MetricsConfig metricsConfig)
    {
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.exporter = requireNonNull(exporter);
        this.metricsConfig = requireNonNull(metricsConfig);
        metricsRegistry = new ConcurrentHashMap<>();
    }

    public MetricsRegistry(CatalogNameProvider catalogNameProvider, MetricsConfig metricsConfig)
    {
        this(catalogNameProvider, MBeanExporter.withPlatformMBeanServer(), metricsConfig);
    }

    public String getKey(String objectKey)
    {
        return objectKey + "." + catalogNameProvider.get();
    }

    public synchronized boolean registerMetric(WarpStatsBase statObject)
    {
        String jmxKey = getKey(statObject.getJmxKey());
        if (!metricsRegistry.containsKey(jmxKey)) {
            if (metricsConfig.isEnabled()) {
                exporter.exportWithGeneratedName(statObject, statObject.getClass(), jmxKey);
            }
            metricsRegistry.put(jmxKey, statObject);
            logger.debug("register new metric: %s (%s)", statObject.getJmxKey(), jmxKey);
            return true;
        }
        return false;
    }

    public void unregisterMetric(String key)
    {
        String jmxKey = getKey(key);
        logger.debug(" unregisterMetric: %s (%s)", key, jmxKey);
        WarpStatsBase removedObject = metricsRegistry.remove(jmxKey);
        if (removedObject != null) {
            if (metricsConfig.isEnabled()) {
                exporter.unexportWithGeneratedName(removedObject.getClass(), jmxKey);
            }
            removedObject.reset();
        }
    }

    @SuppressWarnings("unchecked")
    public WarpStatsBase get(String key)
    {
        return metricsRegistry.get(getKey(key));
    }

    public Map<String, WarpStatsBase> getAll()
    {
        return new HashMap<>(metricsRegistry);
    }

    public Collection<WarpStatsBase> getRegisteredInstances()
    {
        return metricsRegistry.values();
    }

    public void mergeMetrics(Collection<WarpStatsBase> persistDataList)
    {
        logger.debug("merge new persist metrics. current metrics size %d, new persistent metrics from db of size: %d",
                metricsRegistry.size(),
                persistDataList.size());
        persistDataList.forEach(this::mergeMetric);
    }

    @SuppressWarnings("unchecked")
    private <T extends WarpStatsBase> void mergeMetric(T persistData)
    {
        boolean newMetric = registerMetric(persistData);

        if (newMetric) {
            logger.debug("new metric %s registered", getKey(persistData.getJmxKey()));
            persistData.merge(null);
        }
        else {
            logger.debug("merge metric: %s with persistent metric data", getKey(persistData.getJmxKey()));
            T currentStat = (T) metricsRegistry.get(getKey(persistData.getJmxKey()));
            currentStat.merge(persistData);
        }
    }
}
