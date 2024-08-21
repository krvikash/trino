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
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmRulesChangedEvent;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsRegistry;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.plugin.warp.warmup.model.WarmupRuleResult;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmupRuleCloudFetcherTest
{
    @Test
    public void testFetch()
            throws IOException
    {
        WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig = new WarmupRuleCloudFetcherConfig();
        CloudVendorService cloudVendorService = mock(CloudVendorService.class);
        WarmupRuleService warmupRuleService = mock(WarmupRuleService.class);
        EventBus eventBus = mock(EventBus.class);
        CatalogNameProvider catalogNameProvider = new CatalogNameProvider("testCatalog");
        MetricsManager metricsManager = new MetricsManager(new MetricsRegistry(catalogNameProvider, new MetricsConfig()));

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Timer timer = new Timer();
        WarmupRuleCloudFetcher warmupRuleCloudFetcher = new WarmupRuleCloudFetcher(
                warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                eventBus,
                new CatalogName("testCatalog"),
                metricsManager,
                objectMapperProvider,
                timer);

        //nothing returned due warmupRuleCloudFetcherConfig.getStorePath == null
        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(false)).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        verify(eventBus, never()).post(any(WarmRulesChangedEvent.class));

        //nothing returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("");
        assertThat(warmupRuleCloudFetcher.getWarmupRules()).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(false)).isEmpty();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleCloudFetcher.getWarmupRules(true)).isEmpty();
        verify(eventBus, never()).post(any(WarmRulesChangedEvent.class));

        // call cloud
        warmupRuleCloudFetcherConfig.setStorePath("path");
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setContentLength(1L);
        String path = CloudVendorService.concatenatePath(
                warmupRuleCloudFetcherConfig.getStorePath(),
                catalogNameProvider.get());
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        Optional<String> optionalJson = Optional.of("[]");
        when(cloudVendorService.downloadCompressedFromCloud(path, true))
                .thenReturn(optionalJson);

        when(warmupRuleService.replaceAll(anyList())).thenReturn(new WarmupRuleResult(List.of(), Map.of()));

        warmupRuleCloudFetcher.fetch();

        verify(eventBus, times(1)).post(any(WarmRulesChangedEvent.class));
    }
}
