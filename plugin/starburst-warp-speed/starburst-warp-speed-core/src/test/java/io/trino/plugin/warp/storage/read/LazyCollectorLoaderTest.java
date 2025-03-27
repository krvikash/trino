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
package io.trino.plugin.warp.storage.read;

import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LazyCollectorLoaderTest
{
    private LazyCollectTxService lazyCollectTxService;
    private DictionaryStats dictionaryStats;
    private DispatcherPageSourceStats dispatcherPageSourceStats;

    @BeforeEach
    public void before()
    {
        lazyCollectTxService = mock(LazyCollectTxService.class);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, Collections.emptyList());
        customStatsContext.getOrRegister(new DictionaryStats());
        customStatsContext.getOrRegister(new DispatcherPageSourceStats());
        dictionaryStats = (DictionaryStats) customStatsContext.getStat(DictionaryStats.createKey());
        dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
    }

    @Test
    public void testCollectOpenFailureStats()
    {
        LazyCollectorLoaderArgs lazyCollectorLoaderArgs = mock(LazyCollectorLoaderArgs.class);
        when(lazyCollectorLoaderArgs.chunkSize()).thenReturn(65536);
        when(lazyCollectorLoaderArgs.catalogName()).thenReturn(new CatalogName("c"));
        QueryParams queryParams = mock(QueryParams.class);
        when(queryParams.getQueryId()).thenReturn("testQueryId");
        when(lazyCollectorLoaderArgs.queryParams()).thenReturn(queryParams);

        LazyCollectorLoader lazyCollectorLoader = new LazyCollectorLoader(
                lazyCollectTxService,
                lazyCollectorLoaderArgs,
                dictionaryStats,
                dispatcherPageSourceStats,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                mock(NativeStats.class));
        when(lazyCollectTxService.collectOpen(any(LazyCollectorLoaderArgs.class), any())).thenThrow(new RuntimeException());

        Assertions.assertThrows(RuntimeException.class, lazyCollectorLoader::load);
        assertThat(dispatcherPageSourceStats.getlazy_collect_failed_load()).isEqualTo(1);
    }
}
