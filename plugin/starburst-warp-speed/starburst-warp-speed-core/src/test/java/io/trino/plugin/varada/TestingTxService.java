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
package io.trino.plugin.varada;

import io.trino.plugin.varada.config.MetricsConfig;
import io.trino.plugin.varada.config.NativeConfig;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.MetricsRegistry;
import io.varada.tools.CatalogNameProvider;

public class TestingTxService
{
    private TestingTxService() {}

    public static StorageEngineTxService create()
    {
        return new StorageEngineTxService(new NativeConfig(), createMetricsManager());
    }

    @SuppressWarnings("UnstableApiUsage")
    public static MetricsManager createMetricsManager()
    {
        MetricsConfig metricsConfig = new MetricsConfig();
        metricsConfig.setEnabled(false);
        MetricsRegistry metricsRegistry = new MetricsRegistry(new CatalogNameProvider("catalog-name"), metricsConfig);
        return new MetricsManager(metricsRegistry);
    }
}
