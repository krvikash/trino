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
package io.trino.plugin.warp.dispatcher.warmup.fetcher;

import com.google.common.eventbus.EventBus;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.dispatcher.warmup.events.CacheMgrWarmRulesChangedEvent;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmRulesChangedEvent;
import io.trino.plugin.warp.gen.stats.WarmupRuleFetcherStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;

import static io.trino.plugin.warp.dispatcher.warmup.fetcher.CacheMgrWarmupRuleCloudFetcher.WARM_FETCHER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CacheMgrWarmupRuleCloudFetcherTest
{
    private WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig;
    private CloudVendorService cloudVendorService;
    private EventBus eventBus;
    private CacheMgrWarmupRuleCloudFetcher warmupRuleCloudFetcher;

    @BeforeEach
    public void beforeEach()
    {
        warmupRuleCloudFetcherConfig = new WarmupRuleCloudFetcherConfig();
        cloudVendorService = mock(CloudVendorService.class);
        CacheMgrWarmupRuleService warmupRuleService = mock(CacheMgrWarmupRuleService.class);
        eventBus = mock(EventBus.class);
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any()))
                .thenReturn(new WarmupRuleFetcherStats(WARM_FETCHER_STAT_GROUP));
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Timer timer = mock(Timer.class);

        warmupRuleCloudFetcher = new CacheMgrWarmupRuleCloudFetcher(
                warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                eventBus,
                metricsManager,
                objectMapperProvider,
                timer);
    }

    @Test
    public void testFetch()
            throws IOException
    {
        //nothing returned due warmupRuleCloudFetcherConfig.getStorePath == null
        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(false)).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        verify(eventBus, never()).post(any(WarmRulesChangedEvent.class));

        //nothing returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("path");
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        String path = warmupRuleCloudFetcherConfig.getStorePath();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(false)).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        verify(eventBus, never()).post(any(WarmRulesChangedEvent.class));

        // call cloud
        warmupRuleCloudFetcherConfig.setStorePath("path");
        storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setContentLength(1L);
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);
        Optional<String> optionalJson = Optional.of("[]");
        when(cloudVendorService.downloadCompressedFromCloud(path, true))
                .thenReturn(optionalJson);

        warmupRuleCloudFetcher.fetch();

        verify(eventBus, times(1)).post(any(CacheMgrWarmRulesChangedEvent.class));
    }

    @Test
    public void testNotUpdatedFlow()
    {
        //null returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("path");
        String path = CloudVendorService.concatenatePath(
                warmupRuleCloudFetcherConfig.getStorePath(),
                "starburst-cache-mgr");
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(null);

        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        verify(eventBus, never()).post(any(CacheMgrWarmRulesChangedEvent.class));

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        verify(eventBus, never()).post(any(CacheMgrWarmRulesChangedEvent.class));

        //validate nothing returned when nothing changed
        storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        verify(eventBus, never()).post(any(CacheMgrWarmRulesChangedEvent.class));
    }
}
