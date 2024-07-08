
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
package io.trino.plugin.warp.gen.stats;

import io.trino.plugin.warp.metrics.MetricsManager;

@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName", "unused"})
public final class WarpStatsMgr
{
    /* This class file is auto-generated from xml file for statistics and counters */

    public WarpStatsMgr(MetricsManager metricsManager)
    {
        metricsManager.registerMetric(new BtreeStats());
        metricsManager.registerMetric(new CollecttimeStats());
        metricsManager.registerMetric(new ConnectorStats());
        metricsManager.registerMetric(new DatacompressionStats());
        metricsManager.registerMetric(new FastwarmingStats());
        metricsManager.registerMetric(new IndexcompressionStats());
        metricsManager.registerMetric(new LuceneStats());
        metricsManager.registerMetric(new MatchtimeStats());
        metricsManager.registerMetric(new MemoryStats());
        metricsManager.registerMetric(new QueryresultStats());
        metricsManager.registerMetric(new RowgroupStats());
        metricsManager.registerMetric(new StoragecacheStats());
        metricsManager.registerMetric(new StorageioStats());
    }
}
