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
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarmupRuleCloudFetcher;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmRulesChangedEvent;
import io.trino.plugin.warp.gen.stats.WarmupRuleFetcherStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.warmup.WarmupRuleApiMapper;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.plugin.warp.warmup.model.WarmupRuleResult;
import io.trino.spi.catalog.CatalogName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupRuleCloudFetcher
        implements WarmupRuleFetcher
{
    private static final Logger logger = Logger.get(WarmupRuleCloudFetcher.class);
    public static final String WARM_FETCHER_STAT_GROUP = "WarmupRuleCloudFetcher";

    private static final ShapingLogger shapingLogger = ShapingLogger.getInstance(
            logger,
            10,
            Duration.ZERO,
            1);

    private final WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig;
    private final CloudVendorService cloudVendorService;
    private final WarmupRuleService warmupRuleService;
    private final EventBus eventBus;
    private final CatalogName catalogName;
    private final ObjectMapperProvider objectMapperProvider;
    @SuppressWarnings("FieldCanBeLocal")
    private final Timer timer;
    private final WarmupRuleFetcherStats warmupRuleFetcherStats;
    private StorageObjectMetadata currentStorageObjectMetadata;
    private final Lock readLock;

    @SuppressWarnings("unused")
    @Inject
    public WarmupRuleCloudFetcher(
            @ForWarmupRuleCloudFetcher WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig,
            @ForWarmupRuleCloudFetcher CloudVendorService cloudVendorService,
            WarmupRuleService warmupRuleService,
            EventBus eventBus,
            CatalogName catalogName,
            MetricsManager metricsManager,
            ObjectMapperProvider objectMapperProvider)
    {
        this(warmupRuleCloudFetcherConfig,
                cloudVendorService,
                warmupRuleService,
                eventBus,
                catalogName,
                metricsManager,
                objectMapperProvider,
                new Timer());
    }

    @VisibleForTesting
    WarmupRuleCloudFetcher(
            WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig,
            CloudVendorService cloudVendorService,
            WarmupRuleService warmupRuleService,
            EventBus eventBus,
            CatalogName catalogName,
            MetricsManager metricsManager,
            ObjectMapperProvider objectMapperProvider,
            Timer timer)
    {
        this.warmupRuleCloudFetcherConfig = requireNonNull(warmupRuleCloudFetcherConfig);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.warmupRuleService = requireNonNull(warmupRuleService);
        this.eventBus = requireNonNull(eventBus);
        this.catalogName = requireNonNull(catalogName);
        this.objectMapperProvider = requireNonNull(objectMapperProvider);
        this.timer = requireNonNull(timer);
        this.readLock = new ReentrantReadWriteLock().readLock();
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

        this.warmupRuleFetcherStats = requireNonNull(metricsManager).registerMetric(WarmupRuleFetcherStats.create(WARM_FETCHER_STAT_GROUP));
    }

    @Override
    public List<WarmupRule> getWarmupRules(boolean force)
    {
        if (force) {
            if (readLock.tryLock()) {
                try {
                    currentStorageObjectMetadata = null;
                }
                finally {
                    readLock.unlock();
                }
            }
        }
        return getWarmupRules();
    }

    @Override
    public List<WarmupRule> getWarmupRules()
    {
        fetch();
        return warmupRuleService.getAll();
    }

    @VisibleForTesting
    void fetch()
    {
        String path = CloudVendorService.concatenatePath(
                warmupRuleCloudFetcherConfig.getStorePath(),
                catalogName.toString());

        if (readLock.tryLock()) {
            try {
                if (path == null) {
                    logger.debug("rules path is null, storePath %s connectorName %s", warmupRuleCloudFetcherConfig.getStorePath(), catalogName.toString());
                    return;
                }
                StorageObjectMetadata storageObjectMetadata = Failsafe.with(RetryPolicy.builder()
                                .withMaxRetries(warmupRuleCloudFetcherConfig.getDownloadRetries())
                                .withDelay(warmupRuleCloudFetcherConfig.getDownloadDuration())
                                .build())
                        .get(() -> cloudVendorService.getObjectMetadata(path));

                if ((currentStorageObjectMetadata != null && storageObjectMetadata != null) && currentStorageObjectMetadata.equals(storageObjectMetadata)) {
                    return; // nothing changed, no need to continue
                }

                currentStorageObjectMetadata = storageObjectMetadata;

                if (currentStorageObjectMetadata.getContentLength().isPresent()) {
                    Optional<String> optionalJson = Failsafe.with(RetryPolicy.builder()
                                    .withMaxRetries(warmupRuleCloudFetcherConfig.getDownloadRetries())
                                    .withDelay(warmupRuleCloudFetcherConfig.getDownloadDuration())
                                    .build())
                            .get(() -> cloudVendorService.downloadCompressedFromCloud(path, true));
                    logger.debug("fetching from %s -> %s", path, optionalJson.orElse(""));

                    WarmupRuleResult warmupRuleResult = warmupRuleService.replaceAll(optionalJson
                            .map(json -> {
                                try {
                                    List<WarmupColRuleData> warmupColRuleDataList = objectMapperProvider.get()
                                            .readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                                            .readValue(json);
                                    return warmupColRuleDataList.stream().map(WarmupRuleApiMapper::toModel).toList();
                                }
                                catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            }).orElse(List.of()));

                    if (!warmupRuleResult.rejectedRules().isEmpty()) {
                        logger.warn("the following faulty warmup rules were not applied - %s", warmupRuleResult.rejectedRules());
                    }
                    if (!warmupRuleResult.appliedRules().isEmpty()) {
                        shapingLogger.info("%d warmup rules were applied", warmupRuleResult.appliedRules().size());
                    }

                    warmupRuleFetcherStats.incsuccess();
                    eventBus.post(new WarmRulesChangedEvent());
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
                readLock.unlock();
            }
        }
    }
}
