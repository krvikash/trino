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

import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.gen.stats.WarmupRuleFetcherStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.plugin.warp.warmup.model.WarmupRuleResult;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.BeforeEach;
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
    CatalogNameProvider catalogNameProvider = new CatalogNameProvider("test_catalog");
    private WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig;
    private CloudVendorService cloudVendorService;
    private WarmupRuleService warmupRuleService;
    private WarmupRuleFetcherStats warmupRuleFetcherStats;
    private WarmupRuleCloudFetcher warmupRuleCloudFetcher;

    @BeforeEach
    public void beforeEach()
    {
        warmupRuleCloudFetcherConfig = new WarmupRuleCloudFetcherConfig();
        cloudVendorService = mock(CloudVendorService.class);
        warmupRuleService = mock(WarmupRuleService.class);
        warmupRuleFetcherStats = new WarmupRuleFetcherStats();
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenReturn(warmupRuleFetcherStats);

        warmupRuleCloudFetcher = new WarmupRuleCloudFetcher(
                warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                new CatalogName(catalogNameProvider.get()),
                metricsManager,
                new JsonMapperProvider(),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                mock(Timer.class));
    }

    @Test
    public void testFetch()
            throws IOException
    {
        //nothing returned due warmupRuleCloudFetcherConfig.getStorePath == null
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());

        //nothing returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("path");
        String path = CloudVendorService.concatenatePath(
                warmupRuleCloudFetcherConfig.getStorePath(),
                catalogNameProvider.get());
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());

        // call cloud
        storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setContentLength(1L);
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);
        Optional<String> optionalJson = Optional.of("[]");
        when(cloudVendorService.downloadCompressedFromCloud(path, true))
                .thenReturn(optionalJson);
        when(warmupRuleService.replaceAll(anyList())).thenReturn(new WarmupRuleResult(List.of(), Map.of()));

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isEqualTo(1L);
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, times(1)).replaceAll(anyList());
    }

    @Test
    public void testNotUpdatedFlow()
    {
        //null returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("path");
        String path = CloudVendorService.concatenatePath(
                warmupRuleCloudFetcherConfig.getStorePath(),
                catalogNameProvider.get());

        when(cloudVendorService.getObjectMetadata(path)).thenReturn(null);

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isEqualTo(0L);
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());

        //validate nothing returned when nothing changed
        storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());
    }
}
