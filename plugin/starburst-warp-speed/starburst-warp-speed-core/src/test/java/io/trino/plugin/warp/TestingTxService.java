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
package io.trino.plugin.warp;

import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsRegistry;
import io.trino.plugin.warp.tools.CatalogNameProvider;

public class TestingTxService
{
    private TestingTxService() {}

    public static StorageEngineTxService create()
    {
        return new StorageEngineTxService(new NativeConfig(), createMetricsManager());
    }

    public static MetricsManager createMetricsManager()
    {
        MetricsConfig metricsConfig = new MetricsConfig();
        metricsConfig.setEnabled(false);
        MetricsRegistry metricsRegistry = new MetricsRegistry(new CatalogNameProvider("catalog-name"), metricsConfig);
        return new MetricsManager(metricsRegistry);
    }
}
