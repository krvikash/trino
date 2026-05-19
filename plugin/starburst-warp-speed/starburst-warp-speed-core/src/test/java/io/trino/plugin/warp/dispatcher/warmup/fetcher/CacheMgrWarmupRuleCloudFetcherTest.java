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
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.gen.stats.WarmupRuleFetcherStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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

public class CacheMgrWarmupRuleCloudFetcherTest
{
    private WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig;
    private CloudVendorService cloudVendorService;
    private CacheMgrWarmupRuleService warmupRuleService;
    private WarmupRuleFetcherStats warmupRuleFetcherStats;
    private CacheMgrWarmupRuleCloudFetcher warmupRuleCloudFetcher;

    @BeforeEach
    public void beforeEach()
    {
        warmupRuleCloudFetcherConfig = new WarmupRuleCloudFetcherConfig();
        cloudVendorService = mock(CloudVendorService.class);
        warmupRuleService = mock(CacheMgrWarmupRuleService.class);
        warmupRuleFetcherStats = new WarmupRuleFetcherStats();
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenReturn(warmupRuleFetcherStats);
        JsonMapperProvider jsonMapperProvider = new JsonMapperProvider();
        Timer timer = mock(Timer.class);

        warmupRuleCloudFetcher = new CacheMgrWarmupRuleCloudFetcher(
                warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                metricsManager,
                jsonMapperProvider,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                timer);
    }

    @Test
    public void testFetch()
            throws IOException
    {
        // nothing returned due warmupRuleCloudFetcherConfig.getStorePath == null
        warmupRuleCloudFetcher.fetch();
        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());

        // nothing returned from CloudVendorService
        warmupRuleCloudFetcherConfig.setStorePath("path");
        String path = warmupRuleCloudFetcherConfig.getStorePath();
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

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isEqualTo(1L);
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, times(1)).replaceAll(anyList());
    }

    @Test
    public void testNotUpdatedFlow()
    {
        warmupRuleCloudFetcherConfig.setStorePath("path");
        String path = warmupRuleCloudFetcherConfig.getStorePath();
        // null returned from CloudVendorService
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

        // validate nothing returned when nothing changed
        storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(path)).thenReturn(storageObjectMetadata);

        warmupRuleCloudFetcher.fetch();

        assertThat(warmupRuleFetcherStats.getsuccess()).isZero();
        assertThat(warmupRuleFetcherStats.getfail()).isZero();
        verify(warmupRuleService, never()).replaceAll(anyList());
    }
}
