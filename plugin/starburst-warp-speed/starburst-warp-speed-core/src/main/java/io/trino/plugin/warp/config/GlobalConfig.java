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
import io.airlift.units.DataSize;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class GlobalConfig
{
    public static final String CONFIG_IS_SINGLE = "warp-speed.config.is-single";
    public static final String ENABLE_DEFAULT_WARMING = "warp-speed.enable-default-warming";
    public static final String LOCAL_STORE_PATH = "warp-speed.local-store.path";
    public static final String FAILURE_GENERATOR_ENABLED = "warp-speed.config.failure-generator-enabled";
    public static final int MAX_NUMBER_OF_MAPPED_MATCH_COLLECT_ELEMENTS = 1 << Byte.SIZE; //256

    private boolean isSingle;
    private Set<String> unsupportedFunctions = Collections.emptySet();
    private long clusterUpTime;
    private int consistentSplitBucketsPerWorker = 2048;
    private int exportDelayInSeconds;
    private String localStorePath = "/opt/data/INDEX-CACHE";
    private int predicateSimplifyThreshold = 1_000_000;
    private long reservationUsageForSingleTxInBytes = 1024L * 1024 * 128;
    private long emptyPageIterations = 5000;
    private int cloudExecutorPoolSize = Runtime.getRuntime().availableProcessors() * 100;
    private int prioritizeExecutorPoolSize = 1000;
    private String cardinalityBuckets = "1000,1000000"; // allows applying most selective predicate first when using predicate push-down
    private long preAllocMemorySize;
    private boolean enableDefaultWarming = true;
    private boolean createIndexInDefaultWarming;
    private boolean dataOnlyWarming;
    private int maxCollectColumnsSkipDefaultWarming = 128;
    private int maxWarmRetries = 2;
    private int warmRetryBackoffFactorInMillis = 1000;
    private int maxWarmupIterationsPerQuery = 2000;
    private int warmDataVarcharMaxLength = 2048;

    private int shapingLoggerThreshold = 1000;
    private Duration shapingLoggerDuration = Duration.ofSeconds(60);
    private int shapingLoggerNumberOfSamples = 3;

    private boolean debugWarmingSingleThreaded;
    private boolean debugWarming;
    private boolean debugNoPredicateBuffer;
    private boolean debugFailureGenerator;

    private boolean enableFSCacheMode;
    private boolean enableImportExport;
    private boolean enableExportAppendOnCloud = true;
    private boolean enableOrPushdown = true;
    private boolean enableRangeFilter = true;
    private boolean enableInverseWithNulls;
    private boolean enableLazyForSelective = true;

    public boolean getIsSingle()
    {
        return isSingle;
    }

    public boolean getEnableFSCacheMode()
    {
        return enableFSCacheMode;
    }

    @Config("warp-speed.enable.fs-cache-mode")
    public void setEnableFSCacheMode(boolean enableFSCacheMode)
    {
        this.enableFSCacheMode = enableFSCacheMode;
    }

    @Config(CONFIG_IS_SINGLE)
    public void setIsSingle(boolean isSingle)
    {
        this.isSingle = isSingle;
    }

    public boolean getEnableImportExport()
    {
        return enableImportExport;
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

    public boolean getEnableInverseWithNulls()
    {
        return enableInverseWithNulls;
    }

    public boolean getEnableLazyForSelective()
    {
        return enableLazyForSelective;
    }

    @Config("warp-speed.enable.inverse-with-nulls")
    public void setEnableInverseWithNulls(boolean enableInverseWithNulls)
    {
        this.enableInverseWithNulls = enableInverseWithNulls;
    }

    @Config("warp-speed.enable.lazy-for-selective")
    public void setEnableLazyForSelective(boolean enableLazyForSelective)
    {
        this.enableLazyForSelective = enableLazyForSelective;
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

    @Config("warp-speed.default-warming-index")
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

    @Config("warp-speed.cluster_up_time")
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
        unsupportedFunctions = Arrays.stream(unsupportedFunctionsAsString.trim().split(",")).map(String::trim).collect(Collectors.toSet());
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

    @Config("warp-speed.max-collect-columns-skip-default-warming")
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
        return debugFailureGenerator;
    }

    @Config(FAILURE_GENERATOR_ENABLED)
    public void setFailureGeneratorEnabled(boolean debugFailureGenerator)
    {
        this.debugFailureGenerator = debugFailureGenerator;
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

    @Config("warp-speed.data-only-warming")
    public void setDataOnlyWarming(boolean dataOnlyWarming)
    {
        this.dataOnlyWarming = dataOnlyWarming;
    }

    @Config("warp-speed.config-empty-page-iterations")
    public void setEmptyPageIterations(long emptyPageIterations)
    {
        this.emptyPageIterations = emptyPageIterations;
    }

    public long getEmptyPageIterations()
    {
        return emptyPageIterations;
    }

    public long getPreAllocMemorySize()
    {
        return preAllocMemorySize;
    }

    @Config("warp-speed.config.pre-alloc-memory-size-mb")
    public void setPreAllocMemorySize(int preAllocMemorySizeInMegaBytes)
    {
        this.preAllocMemorySize = DataSize.of(preAllocMemorySizeInMegaBytes, DataSize.Unit.MEGABYTE).toBytes();
    }

    @Override
    public String toString()
    {
        return "GlobalConfig{" +
                "isSingle=" + isSingle +
                ", cardinalityBuckets='" + cardinalityBuckets + '\'' +
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
                ", debugFailureGenerator=" + debugFailureGenerator +
                ", enableRangeFilter=" + enableRangeFilter +
                ", shapingLoggerThreshold=" + shapingLoggerThreshold +
                ", shapingLoggerDuration=" + shapingLoggerDuration +
                ", shapingLoggerNumberOfSamplings=" + shapingLoggerNumberOfSamples +
                ", dataOnlyWarming=" + dataOnlyWarming +
                ", debugWarming=" + debugWarming +
                ", debugWarmingSingleThreaded=" + debugWarmingSingleThreaded +
                ", debugNoPredicateBuffer=" + debugNoPredicateBuffer +
                ", emptyPageIterations=" + emptyPageIterations +
                '}';
    }
}
