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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LazyCollectorLoaderTest
{
    private LazyCollectTxService lazyCollectTxService;
    private GlobalConfig globalConfig;
    private DictionaryStats dictionaryStats;
    private DispatcherPageSourceStats dispatcherPageSourceStats;

    @BeforeEach
    public void before()
    {
        this.lazyCollectTxService = mock(LazyCollectTxService.class);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, Collections.emptyList());
        customStatsContext.getOrRegister(new DictionaryStats(DictionaryCacheService.DICTIONARY_STAT_GROUP));
        customStatsContext.getOrRegister(new DispatcherPageSourceStats(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
        this.dictionaryStats = (DictionaryStats) customStatsContext.getStat(DictionaryCacheService.DICTIONARY_STAT_GROUP);
        this.dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);
        this.globalConfig = new GlobalConfig();
    }

    @Test
    public void testCollectOpenFailureStats()
    {
        LazyCollectorArgs lazyCollectorArgs = mock(LazyCollectorArgs.class);
        LazyCollectorLoader lazyCollectorLoader = new LazyCollectorLoader(
                lazyCollectTxService,
                lazyCollectorArgs,
                dictionaryStats,
                dispatcherPageSourceStats,
                globalConfig);
        when(lazyCollectTxService.collectOpen(anyInt(), any())).thenThrow(new RuntimeException());

        Assertions.assertThrows(RuntimeException.class, lazyCollectorLoader::load);
        assertThat(dispatcherPageSourceStats.getlazy_collect_failed_load()).isEqualTo(1);
    }
}
