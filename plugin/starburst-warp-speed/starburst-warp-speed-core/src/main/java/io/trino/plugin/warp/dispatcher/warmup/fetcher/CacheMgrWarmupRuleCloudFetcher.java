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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarmupRuleCloudFetcher;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.gen.stats.WarmupRuleFetcherStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

@Singleton
public class CacheMgrWarmupRuleCloudFetcher
        implements WarmupRuleFetcher<CacheManagerRule>
{
    private static final Logger logger = Logger.get(CacheMgrWarmupRuleCloudFetcher.class);

    private final WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig;
    private final CloudVendorService cloudVendorService;
    private final CacheMgrWarmupRuleService warmupRuleService;
    private final JsonMapperProvider jsonMapperProvider;
    private final ShapingLogger shapingLogger;

    @SuppressWarnings("FieldCanBeLocal")
    private final Timer timer;
    private final WarmupRuleFetcherStats warmupRuleFetcherStats;
    private StorageObjectMetadata currentStorageObjectMetadata;
    private final Lock writeLock;

    @SuppressWarnings("unused")
    @Inject
    public CacheMgrWarmupRuleCloudFetcher(
            @ForWarmupRuleCloudFetcher WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig,
            @ForWarmupRuleCloudFetcher CloudVendorService cloudVendorService,
            CacheMgrWarmupRuleService warmupRuleService,
            MetricsManager metricsManager,
            JsonMapperProvider jsonMapperProvider,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this(warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                metricsManager,
                jsonMapperProvider,
                shapingLoggerFactory,
                new Timer());
    }

    @VisibleForTesting
    CacheMgrWarmupRuleCloudFetcher(
            WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig,
            CloudVendorService cloudVendorService,
            CacheMgrWarmupRuleService warmupRuleService,
            MetricsManager metricsManager,
            JsonMapperProvider jsonMapperProvider,
            ShapingLoggerFactory shapingLoggerFactory,
            Timer timer)
    {
        this.warmupRuleCloudFetcherConfig = requireNonNull(warmupRuleCloudFetcherConfig);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.warmupRuleService = requireNonNull(warmupRuleService);
        this.jsonMapperProvider = requireNonNull(jsonMapperProvider);
        this.timer = requireNonNull(timer);

        shapingLogger = shapingLoggerFactory.getInstance(
                this.getClass(),
                logger,
                10,
                Duration.ZERO,
                1,
                ShapingLogger.MODE.FORMAT);

        writeLock = new ReentrantReadWriteLock().writeLock();
        currentStorageObjectMetadata = null;

        this.timer.scheduleAtFixedRate(
                new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        fetch();
                    }
                },
                warmupRuleCloudFetcherConfig.getFetchDelayDuration().toMillis(),
                warmupRuleCloudFetcherConfig.getFetchDuration().toMillis());

        this.warmupRuleFetcherStats = requireNonNull(metricsManager)
                .registerMetric(WarmupRuleFetcherStats.create());
    }

    @Override
    public void fetch()
    {
        if (warmupRuleCloudFetcherConfig.getStorePath() == null) {
            return;
        }

        String path = warmupRuleCloudFetcherConfig.getStorePath();

        if (writeLock.tryLock()) {
            try {
                StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(path);

                if ((currentStorageObjectMetadata != null && storageObjectMetadata != null) &&
                        currentStorageObjectMetadata.equals(storageObjectMetadata)) {
                    return; // nothing changed, no need to continue
                }

                currentStorageObjectMetadata = storageObjectMetadata;

                if ((currentStorageObjectMetadata != null) &&
                        currentStorageObjectMetadata.getContentLength().isPresent()) {
                    Optional<String> optionalJson = cloudVendorService.downloadCompressedFromCloud(path, true);

                    logger.debug("fetching from %s -> %s", path, optionalJson.orElse(""));

                    optionalJson.ifPresent(json -> {
                        try {
                            List<CacheManagerRule> cacheManagerRules = jsonMapperProvider.get()
                                    .readerFor(new TypeReference<List<CacheManagerRule>>() {})
                                    .readValue(json);

                            warmupRuleService.replaceAll(cacheManagerRules);

                            shapingLogger.info("%d cache rules were applied", cacheManagerRules.size());

                            warmupRuleFetcherStats.incsuccess();
                        }
                        catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                else {
                    logger.debug("rules file does not exist %s", path);
                }
            }
            catch (Throwable e) {
                warmupRuleFetcherStats.incfail();
                shapingLogger.error(e, "failed fetching rules from %s", path);
            }
            finally {
                writeLock.unlock();
            }
        }
    }
}
