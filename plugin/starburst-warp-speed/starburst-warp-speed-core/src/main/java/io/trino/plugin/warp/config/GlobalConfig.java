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
package io.trino.plugin.warp.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.log.Logger;
import io.trino.plugin.warp.tools.certification.SwaggerExposingLevel;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class GlobalConfig
{
    public static final String CONFIG_IS_SINGLE = "warp-speed.config.is-single";
    public static final String CONFIG_IS_CACHE = "warp-speed.config.is-cache";
    public static final String CLUSTER_UP_TIME = "warp-speed.cluster_up_time";
    public static final String ENABLE_DEFAULT_WARMING = "warp-speed.enable-default-warming";
    public static final String DEFAULT_WARMING_INDEX = "warp-speed.default-warming-index";
    public static final String LOCAL_STORE_PATH = "warp-speed.local-store.path";
    public static final String LOCAL_STORE_CLEAN_ON_LOAD = "warp-speed.local-store.clean-on-load";
    public static final String MAX_COLLECT_COLUMNS_SKIP_DEFAULT_WARMING = "warp-speed.max-collect-columns-skip-default-warming";
    public static final String CERT_LOCAL_LOCATION = "warp-speed.config.cert-local-location";
    public static final String AZURE_CONNECTION_STRING = "warp-speed.config.azure.connection-string";
    public static final String STATS_COLLECTION_ENABLED = "warp-speed.config.stats-collection-enabled";
    public static final String FAILURE_GENERATOR_ENABLED = "warp-speed.config.failure-generator-enabled";
    public static final String DATA_ONLY_WARMING = "warp-speed.data-only-warming";
    public static final String EMPTY_PAGE_ITERATIONS = "warp-speed.config-empty-page-iterations";
    public static final String CACHE_MANAGER_MAX_PARALLEL_WARMUP_ELEMENTS = "warp-speed.cache-manager.max-parallel-warmup-elements";
    public static final int MAX_NUMBER_OF_MAPPED_MATCH_COLLECT_ELEMENTS = 1 << Byte.SIZE; //256
    private static final Logger logger = Logger.get(GlobalConfig.class);

    private final Optional<String> authorization = Optional.empty();  // by default, no authorization
    private int stripeSize = 32;
    private String cardinalityBuckets = "1000,1000000"; // allows applying most selective predicate first when using predicate push-down
    private boolean isSingle;
    private boolean isCache;
    private Set<String> unsupportedFunctions = Collections.emptySet();
    private long clusterUpTime;
    private boolean enableDefaultWarming = true;
    private boolean createIndexInDefaultWarming;
    private int consistentSplitBucketsPerWorker = 2048;
    private int exportDelayInSeconds;
    private String localStorePath = "/opt/data/";
    private boolean enableLocalStoreCleanOnLoad = true;
    private int maxCollectColumnsSkipDefaultWarming = 128;
    private int predicateSimplifyThreshold = 1_000_000;
    private long reservationUsageForSingleTxInBytes = 1024L * 1024 * 128;
    private int maxWarmRetries = 2;
    private int warmRetryBackoffFactorInMillis = 1000;
    private int maxWarmupIterationsPerQuery = 2000;
    private int warmDataVarcharMaxLength = 2048;
    private String certLocalLocalLocation;
    private String deviceIdentifier;
    private String azureConnectionString;
    private boolean failureGeneratorEnabled;
    private long emptyPageIterations = 5000;
    private SwaggerExposingLevel swaggerExposingLevel = SwaggerExposingLevel.DEBUG;

    private boolean debugWarmingSingleThreaded;
    private boolean debugWarming;
    private boolean debugNoPredicateBuffer;

    private boolean enableImportExport;
    private boolean enableExportAppendOnCloud = true;
    private boolean enableMatchCollect = true;
    private boolean enableMappedMatchCollect = true;
    private boolean enableVarcharMappedMatchCollect;
    private boolean enableOrPushdown = true;
    private boolean enableRangeFilter = true;
    private int shapingLoggerThreshold = 1000;
    private Duration shapingLoggerDuration = Duration.ofSeconds(60);
    private int shapingLoggerNumberOfSamples = 3;
    private boolean dataOnlyWarming;
    private boolean enableInverseWithNulls;
    private int cacheManagerMaxParallelWarmupElements = 200;
    private int cloudExecutorPoolSize = Runtime.getRuntime().availableProcessors() * 100;
    private int prioritizeExecutorPoolSize = 1000;

    public int getStripeSize()
    {
        return stripeSize;
    }

    @Config("warp-speed.config.stripesize")
    public void setStripeSize(int stripeSize)
    {
        this.stripeSize = stripeSize;
    }

    public boolean getIsSingle()
    {
        return isSingle;
    }

    @Config(CONFIG_IS_SINGLE)
    public void setIsSingle(boolean isSingle)
    {
        this.isSingle = isSingle;
    }

    public boolean getIsCache()
    {
        return isCache;
    }

    @Config(CONFIG_IS_CACHE)
    public void setIsCache(boolean isCache)
    {
        this.isCache = isCache;
    }

    public String getCardinalityBuckets()
    {
        return cardinalityBuckets;
    }

    @Config("warp-speed.config.cardinality-buckets")
    public void setCardinalityBuckets(String cardinalityBuckets)
    {
        this.cardinalityBuckets = cardinalityBuckets;
    }

    public boolean getEnableImportExport()
    {
        return enableImportExport;
    }

    @Config("warp-speed.enable.import-export")
    public void setEnableImportExport(boolean enableImportExport)
    {
        this.enableImportExport = enableImportExport;
    }

    public boolean getEnableExportAppendOnCloud()
    {
        return enableExportAppendOnCloud;
    }

    public int getCloudExecutorPoolSize()
    {
        return cloudExecutorPoolSize;
    }

    @Config("warp-speed.config.task.cloud-executor-pool-size")
    public void setCloudExecutorPoolSize(int cloudExecutorPoolSize)
    {
        this.cloudExecutorPoolSize = cloudExecutorPoolSize;
    }

    public int getPrioritizeExecutorPoolSize()
    {
        return prioritizeExecutorPoolSize;
    }

    @Config("warp-speed.config.task.prioritize-executor-pool-size")
    public void setPrioritizeExecutorPoolSize(int prioritizeExecutorPoolSize)
    {
        this.prioritizeExecutorPoolSize = prioritizeExecutorPoolSize;
    }

    @Config("warp-speed.enable.export-append-on-cloud")
    public void setEnableExportAppendOnCloud(boolean enableExportAppendOnCloud)
    {
        this.enableExportAppendOnCloud = enableExportAppendOnCloud;
    }

    public boolean getEnableMatchCollect()
    {
        return enableMatchCollect;
    }

    public boolean getEnableMappedMatchCollect()
    {
        return enableMappedMatchCollect;
    }

    public boolean getEnableVarcharMappedMatchCollect()
    {
        return enableVarcharMappedMatchCollect;
    }

    public boolean getEnableInverseWithNulls()
    {
        return enableInverseWithNulls;
    }

    @Config("warp-speed.enable.match-collect")
    public void setEnableMatchCollect(boolean enableMatchCollect)
    {
        this.enableMatchCollect = enableMatchCollect;
    }

    @Config("warp-speed.enable.mapped-match-collect")
    public void setEnableMappedMatchCollect(boolean enableMappedMatchCollect)
    {
        this.enableMappedMatchCollect = enableMappedMatchCollect;
    }

    @Config("warp-speed.enable.varchar-mapped-match-collect")
    public void setEnableVarcharMappedMatchCollect(boolean enableVarcharMappedMatchCollect)
    {
        this.enableVarcharMappedMatchCollect = enableVarcharMappedMatchCollect;
    }

    @Config("warp-speed.enable.inverse-with-nulls")
    public void setEnableInverseWithNulls(boolean enableInverseWithNulls)
    {
        this.enableInverseWithNulls = enableInverseWithNulls;
    }

    public boolean getEnableOrPushdown()
    {
        return enableOrPushdown;
    }

    @Config("warp-speed.enable.or-pushdown")
    public void setEnableOrPushdown(boolean enableOrPushdown)
    {
        this.enableOrPushdown = enableOrPushdown;
    }

    public boolean isEnableDefaultWarming()
    {
        return enableDefaultWarming;
    }

    @Config(ENABLE_DEFAULT_WARMING)
    public void setEnableDefaultWarming(boolean enableDefaultWarming)
    {
        this.enableDefaultWarming = enableDefaultWarming;
    }

    public boolean isCreateIndexInDefaultWarming()
    {
        return createIndexInDefaultWarming;
    }

    @Config(DEFAULT_WARMING_INDEX)
    public void setCreateIndexInDefaultWarming(boolean createIndexInDefaultWarming)
    {
        this.createIndexInDefaultWarming = createIndexInDefaultWarming;
    }

    @Min(1000)
    @Max(7000)
    public int getWarmDataVarcharMaxLength()
    {
        return warmDataVarcharMaxLength;
    }

    @Config("warp-speed.config.warm-data-varchar-max-length")
    public void setWarmDataVarcharMaxLength(int warmDataVarcharMaxLength)
    {
        this.warmDataVarcharMaxLength = warmDataVarcharMaxLength;
    }

    public long getClusterUpTime()
    {
        return clusterUpTime;
    }

    @Config(CLUSTER_UP_TIME)
    public void setClusterUpTime(long clusterUpTime)
    {
        this.clusterUpTime = clusterUpTime;
    }

    public int getConsistentSplitBucketsPerWorker()
    {
        return consistentSplitBucketsPerWorker;
    }

    @Config("warp-speed.config.consistent-split-buckets-per-worker")
    public void setConsistentSplitBucketsPerWorker(int consistentSplitBucketsPerWorker)
    {
        this.consistentSplitBucketsPerWorker = consistentSplitBucketsPerWorker;
    }

    @Min(0)
    @Max(3600)
    public int getExportDelayInSeconds()
    {
        return exportDelayInSeconds;
    }

    @Config("warp-speed.config.export.delay-in-seconds")
    public void setExportDelayInSeconds(int exportDelayInSeconds)
    {
        this.exportDelayInSeconds = exportDelayInSeconds;
    }

    public Set<String> getUnsupportedFunctions()
    {
        return unsupportedFunctions;
    }

    @Config("warp-speed.debug.unsupported-functions")
    public void setUnsupportedFunctions(String unsupportedFunctionsAsString)
    {
        try {
            unsupportedFunctions = Arrays.stream(unsupportedFunctionsAsString.trim().split(",")).map(String::trim).collect(Collectors.toSet());
        }
        catch (Exception e) {
            logger.error("failed to set warp-speed.debug.unsupported-functions list");
        }
    }

    public String getLocalStorePath()
    {
        return localStorePath;
    }

    @Config(LOCAL_STORE_PATH)
    public void setLocalStorePath(String localStorePath)
    {
        this.localStorePath = localStorePath;
    }

    public int getMaxCollectColumnsSkipDefaultWarming()
    {
        return maxCollectColumnsSkipDefaultWarming;
    }

    public boolean isEnableLocalStoreCleanOnLoad()
    {
        return enableLocalStoreCleanOnLoad;
    }

    @Config(LOCAL_STORE_CLEAN_ON_LOAD)
    public void setEnableLocalStoreCleanOnLoad(boolean enableLocalStoreCleanOnLoad)
    {
        this.enableLocalStoreCleanOnLoad = enableLocalStoreCleanOnLoad;
    }

    @Config(MAX_COLLECT_COLUMNS_SKIP_DEFAULT_WARMING)
    public void setMaxCollectColumnsSkipDefaultWarming(int maxCollectColumnsSkipDefaultWarming)
    {
        this.maxCollectColumnsSkipDefaultWarming = maxCollectColumnsSkipDefaultWarming;
    }

    public long getReservationUsageForSingleTxInBytes()
    {
        return reservationUsageForSingleTxInBytes;
    }

    @Config("warp-speed.config.reservation-usage-for-single-tx-in-bytes")
    public void setReservationUsageForSingleTxInBytes(long reservationUsageForSingleTxInBytes)
    {
        this.reservationUsageForSingleTxInBytes = reservationUsageForSingleTxInBytes;
    }

    @Min(32)
    @Max(1_000_000)
    public int getPredicateSimplifyThreshold()
    {
        return predicateSimplifyThreshold;
    }

    @Config("warp-speed.config.predicate-simplify-threshold")
    public void setPredicateSimplifyThreshold(int predicateSimplifyThreshold)
    {
        this.predicateSimplifyThreshold = predicateSimplifyThreshold;
    }

    public int getMaxWarmRetries()
    {
        return maxWarmRetries;
    }

    @Config("warp-speed.config.max-warm-retries")
    public void setMaxWarmRetries(int maxWarmRetries)
    {
        this.maxWarmRetries = maxWarmRetries;
    }

    public int getWarmRetryBackoffFactorInMillis()
    {
        return warmRetryBackoffFactorInMillis;
    }

    @Config("warp-speed.config.warm-retry-backoff-factor-in-millis")
    public void setWarmRetryBackoffFactorInMillis(int warmRetryBackoffFactorInMillis)
    {
        this.warmRetryBackoffFactorInMillis = warmRetryBackoffFactorInMillis;
    }

    public int getMaxWarmupIterationsPerQuery()
    {
        return maxWarmupIterationsPerQuery;
    }

    @Config("warp-speed.config.max-warmup-iterations-per-query")
    public void setMaxWarmupIterationsPerQuery(int maxWarmupIterationsPerQuery)
    {
        this.maxWarmupIterationsPerQuery = maxWarmupIterationsPerQuery;
    }

    public String getCertLocalLocalLocation()
    {
        return certLocalLocalLocation;
    }

    @Config(CERT_LOCAL_LOCATION)
    public void setCertLocalLocalLocation(String certLocalLocalLocation)
    {
        this.certLocalLocalLocation = certLocalLocalLocation;
    }

    public String getAzureConnectionString()
    {
        return azureConnectionString;
    }

    @ConfigSecuritySensitive
    @Config(AZURE_CONNECTION_STRING)
    public void setAzureConnectionString(String azureConnectionString)
    {
        this.azureConnectionString = azureConnectionString;
    }

    public boolean getDebugWarming()
    {
        return debugWarming;
    }

    @Config("warp-speed.debug.warming")
    public void setDebugWarming(boolean debugWarming)
    {
        this.debugWarming = debugWarming;
    }

    public boolean isDebugWarmingSingleThreaded()
    {
        return debugWarmingSingleThreaded;
    }

    @Config("warp-speed.debug.warming-single-threaded")
    public void setDebugWarmingSingleThreaded(boolean debugWarmingSingleThreaded)
    {
        this.debugWarmingSingleThreaded = debugWarmingSingleThreaded;
    }

    public boolean isDebugNoPredicateBuffer()
    {
        return debugNoPredicateBuffer;
    }

    @Config("warp-speed.debug.no-predicate-buffer")
    public void setDebugNoPredicateBuffer(boolean debugNoPredicateBuffer)
    {
        this.debugNoPredicateBuffer = debugNoPredicateBuffer;
    }

    public boolean isFailureGeneratorEnabled()
    {
        return failureGeneratorEnabled;
    }

    @Config(FAILURE_GENERATOR_ENABLED)
    public void setFailureGeneratorEnabled(boolean failureGeneratorEnabled)
    {
        this.failureGeneratorEnabled = failureGeneratorEnabled;
    }

    public SwaggerExposingLevel getSwaggerExposingLevel()
    {
        return swaggerExposingLevel;
    }

    @Config("warp-speed.config.swagger-exposing-level")
    public void setSwaggerExposingLevel(SwaggerExposingLevel swaggerExposingLevel)
    {
        this.swaggerExposingLevel = swaggerExposingLevel;
    }

    @Config("warp-speed.enable.range-filter")
    public void setEnableRangeFilter(boolean enableRangeFilter)
    {
        this.enableRangeFilter = enableRangeFilter;
    }

    public boolean getEnableRangeFilter()
    {
        return enableRangeFilter;
    }

    public int getShapingLoggerThreshold()
    {
        return shapingLoggerThreshold;
    }

    @Config("warp-speed.shaping-logger.threshold")
    public void setShapingLoggerThreshold(int threshold)
    {
        this.shapingLoggerThreshold = threshold;
    }

    public Duration getShapingLoggerDuration()
    {
        return shapingLoggerDuration;
    }

    @Config("warp-speed.shaping-logger.duration")
    public void setShapingLoggerDuration(io.airlift.units.Duration duration)
    {
        this.shapingLoggerDuration = duration.toJavaTime();
    }

    public int getShapingLoggerNumberOfSamples()
    {
        return shapingLoggerNumberOfSamples;
    }

    @Config("warp-speed.shaping-logger-num-samples")
    public void setShapingLoggerNumberOfSamples(int shapingLoggerNumberOfSamples)
    {
        this.shapingLoggerNumberOfSamples = shapingLoggerNumberOfSamples;
    }

    public boolean isDataOnlyWarming()
    {
        return dataOnlyWarming;
    }

    @Config(DATA_ONLY_WARMING)
    public void setDataOnlyWarming(boolean dataOnlyWarming)
    {
        this.dataOnlyWarming = dataOnlyWarming;
    }

    public int getCacheManagerMaxParallelWarmupElements()
    {
        return cacheManagerMaxParallelWarmupElements;
    }

    @Config(CACHE_MANAGER_MAX_PARALLEL_WARMUP_ELEMENTS)
    public void setCacheManagerMaxParallelWarmupElements(int cacheManagerMaxParallelWarmupElements)
    {
        this.cacheManagerMaxParallelWarmupElements = cacheManagerMaxParallelWarmupElements;
    }

    @Config(EMPTY_PAGE_ITERATIONS)
    public void setEmptyPageIterations(long emptyPageIterations)
    {
        this.emptyPageIterations = emptyPageIterations;
    }

    public long getEmptyPageIterations()
    {
        return emptyPageIterations;
    }

    @Override
    public String toString()
    {
        return "GlobalConfig{" +
                "authorization=" + authorization +
                ", stripeSize=" + stripeSize +
                ", cardinalityBuckets='" + cardinalityBuckets + '\'' +
                ", isSingle=" + isSingle +
                ", isCache=" + isCache +
                ", unsupportedFunctions=" + unsupportedFunctions +
                ", clusterUpTime=" + clusterUpTime +
                ", enableDefaultWarming=" + enableDefaultWarming +
                ", createIndexInDefaultWarming=" + createIndexInDefaultWarming +
                ", consistentSplitBucketsPerWorker=" + consistentSplitBucketsPerWorker +
                ", exportDelayInSeconds=" + exportDelayInSeconds +
                ", localStorePath='" + localStorePath + '\'' +
                ", maxCollectColumnsSkipDefaultWarming=" + maxCollectColumnsSkipDefaultWarming +
                ", predicateSimplifyThreshold=" + predicateSimplifyThreshold +
                ", reservationUsageForSingleTxInBytes=" + reservationUsageForSingleTxInBytes +
                ", maxWarmRetries=" + maxWarmRetries +
                ", warmRetryBackoffFactorInMillis=" + warmRetryBackoffFactorInMillis +
                ", maxWarmupIterationsPerQuery=" + maxWarmupIterationsPerQuery +
                ", enableImportExport=" + enableImportExport +
                ", cloudExecutorPoolSize=" + cloudExecutorPoolSize +
                ", prioritizeExecutorPoolSize=" + prioritizeExecutorPoolSize +
                ", enableExportAppendOnCloud=" + enableExportAppendOnCloud +
                ", enableMatchCollect=" + enableMatchCollect +
                ", certLocalLocalLocation='" + certLocalLocalLocation + '\'' +
                ", deviceIdentifier='" + deviceIdentifier + '\'' +
                ", azureConnectionString='" + azureConnectionString + '\'' +
                ", failureGeneratorEnabled=" + failureGeneratorEnabled +
                ", swaggerExposingLevel=" + swaggerExposingLevel +
                ", enableRangeFilter=" + enableRangeFilter +
                ", shapingLoggerThreshold=" + shapingLoggerThreshold +
                ", shapingLoggerDuration=" + shapingLoggerDuration +
                ", shapingLoggerNumberOfSamplings=" + shapingLoggerNumberOfSamples +
                ", dataOnlyWarming=" + dataOnlyWarming +
                ", debugWarming=" + debugWarming +
                ", debugWarmingSingleThreaded=" + debugWarmingSingleThreaded +
                ", debugNoPredicateBuffer=" + debugNoPredicateBuffer +
                ", emptyPageIterations=" + emptyPageIterations +
                ", cacheManagerMaxParallelWarmupElements=" + cacheManagerMaxParallelWarmupElements +
                '}';
    }
}
