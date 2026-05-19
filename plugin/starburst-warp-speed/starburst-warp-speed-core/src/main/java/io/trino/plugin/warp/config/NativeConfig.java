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
import io.trino.plugin.warp.gen.constants.CompressionUsers;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class NativeConfig
{
    private static final int READERS_WARMERS_RATIO = 32;
    private static final int MIN_WARMING_THREADS = 2;
    private static final int DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES = 1024 * 1024;
    private static final String EXCEPTIONAL_LIST_COMPRESSION = "enable.compression.exceptional-list";

    private DataSize maxRecJufferSize = DataSize.of(10, DataSize.Unit.MEGABYTE);
    private int lz4HcPercent = 10;
    private DataSize collectTxSize = DataSize.of(10, DataSize.Unit.MEGABYTE);
    private int storageCacheSizeInPages;
    private int skipIndexPercent = 80;
    private int limitNumIosInParallel = 2400;
    private int maxIOMetadataSize = 24; // maximal IO md size is two longs - pointer and offset, and 2 integers - size and returned value
    private int taskMaxWorkerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private int taskMinWarmingThreads; // used for limiting number of warming threads running in parallel to query
    private int debugPanicHaltPolicy;
    private int clusterLevel = -1;
    private int maxPageSourcesWithoutWarmingLimit = 8;
    private Set<String> unsupportedNativeFunctions = Collections.emptySet();

    private Duration storageTemporaryExceptionDuration = Duration.of(5, ChronoUnit.MINUTES);
    private int storageTemporaryExceptionNumTries = 3;
    private Duration storageTemporaryExceptionExpiryDuration = Duration.of(1, ChronoUnit.HOURS);

    //////////////////////////// Enable flags and lists ///////////////////////////////
    // Exceptional Lists of record types is optional and not used for all features
    // If exists, the exceptional list contains exceptions to the general rule:
    //     In case the general rule is Enable, the exceptional list will contain Disabled record types
    //     In case the general rule is Disable, the exceptional list will contain Enabled record types
    // With this approach field engineers can very easily disable/enable fully or partially any supported feature
    // Exception lists in NativeConfig are held as integer bitmaps while every bit represents each potential
    // enum value (on - in the list, off - not in the list)
    private boolean enableSingleChunk = true;
    private boolean enablePackedChunk = true;
    private boolean enableCompression = true;
    private int exceptionalListCompression;

    @Min(1 * 1024 * 1024)
    @Max(16 * 1024 * 1024)
    public int getMaxRecJufferSize()
    {
        return (int) maxRecJufferSize.toBytes();
    }

    @Config("warp-speed.config.max-rec-juffer-size-mb")
    public void setMaxRecJufferSize(int maxRecJufferSizeInMegaBytes)
    {
        this.maxRecJufferSize = DataSize.of(maxRecJufferSizeInMegaBytes, DataSize.Unit.MEGABYTE);
    }

    @Min(0)
    @Max(12)
    public int getCompressionLevel()
    {
        return lz4HcPercent;
    }

    @Config("warp-speed.config.task.lz4-hc-percent")
    public void setCompressionLevel(int lz4HcPercent)
    {
        this.lz4HcPercent = lz4HcPercent;
    }

    @Min(1024 * 1024)
    @Max(32 * 1024 * 1024)
    public int getCollectTxSize()
    {
        return (int) collectTxSize.toBytes();
    }

    @Config("warp-speed.config.collect-tx-size-mb")
    public void setCollectTxSize(int collectTxSizeInMegaBytes)
    {
        this.collectTxSize = DataSize.of(collectTxSizeInMegaBytes, DataSize.Unit.MEGABYTE);
    }

    @Min(32 * 1024)
    @Max(16 * 1024 * 1024)
    public int getStorageCacheSizeInPages()
    {
        if (storageCacheSizeInPages == 0) {
            storageCacheSizeInPages = switch (getClusterLevel()) {
                case 0 -> (DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 4) - 1;
                case 1 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 2;
                case 3 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES << 1;
                case 4 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES << 2;
                default -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES; // 2 is the default level
            };
        }
        return storageCacheSizeInPages;
    }

    @Config("warp-speed.config.storage-cache-size-in-pages")
    public void setStorageCacheSizeInPages(int storageCacheSizeInPages)
    {
        this.storageCacheSizeInPages = storageCacheSizeInPages;
    }

    /**
     * In case of RANGE predicate, it sometimes faster to get complete chunk without entering to index-chunk itself.
     * if our estimation of records to read is higher than the percent you give us, we skip the index.
     */
    public int getSkipIndexPercent()
    {
        return skipIndexPercent;
    }

    @Config("warp-speed.config.skip-index-percent")
    public void setSkipIndexPercent(int skipIndexPercent)
    {
        this.skipIndexPercent = skipIndexPercent;
    }

    public int getLimitNumIosInParallel()
    {
        return limitNumIosInParallel;
    }

    @Config("warp-speed.config.limit-num-ios-in-parallel")
    public void setLimitNumIosInParallel(int limitNumIosInParallel)
    {
        this.limitNumIosInParallel = limitNumIosInParallel;
    }

    public int getMaxIOMetadataSize()
    {
        return maxIOMetadataSize;
    }

    @Config("warp-speed.config.max-io-metadata-size")
    public void setMaxIOMetadataSize(int maxIOMetadataSize)
    {
        this.maxIOMetadataSize = maxIOMetadataSize;
    }

    public boolean getEnableSingleChunk()
    {
        return enableSingleChunk;
    }

    @Config("warp-speed.enable.single-chunk")
    public void setEnableSingleChunk(boolean enableSingleChunk)
    {
        this.enableSingleChunk = enableSingleChunk;
    }

    public boolean getEnablePackedChunk()
    {
        return enablePackedChunk;
    }

    @Config("warp-speed.enable.packed-chunk")
    public void setEnablePackedChunk(boolean enablePackedChunk)
    {
        this.enablePackedChunk = enablePackedChunk;
    }

    public boolean getEnableCompression()
    {
        return enableCompression;
    }

    @Config("warp-speed.enable.compression")
    public void setEnableCompression(boolean enableCompression)
    {
        this.enableCompression = enableCompression;
    }

    public int getTaskMaxWorkerThreads()
    {
        return taskMaxWorkerThreads;
    }

    @Config("warp-speed." + EXCEPTIONAL_LIST_COMPRESSION)
    public void setExceptionalListCompression(String exceptionalListCompression)
    {
        this.exceptionalListCompression = string2CompressionUsersList(exceptionalListCompression);
    }

    public int getExceptionalListCompression()
    {
        return exceptionalListCompression;
    }

    @Config("warp-speed.config.task.max-worker-threads")
    public void setTaskMaxWorkerThreads(int maxWorkerThreads)
    {
        this.taskMaxWorkerThreads = maxWorkerThreads;
    }

    public int getDebugPanicHaltPolicy()
    {
        return debugPanicHaltPolicy;
    }

    @Config("warp-speed.debug.panic-halt-policy")
    public void setDebugPanicHaltPolicy(int debugPanicHaltPolicy)
    {
        this.debugPanicHaltPolicy = debugPanicHaltPolicy;
    }

    @Min(0)
    @Max(4)
    public int getClusterLevel()
    {
        if (clusterLevel == -1) {
            if (taskMaxWorkerThreads <= 16) { // 1x 2x
                clusterLevel = 0;
            }
            else if (taskMaxWorkerThreads <= 32) { // 4x
                clusterLevel = 1;
            }
            else if (taskMaxWorkerThreads <= 64) { // 8x which is the default
                clusterLevel = 2;
            }
            else if (taskMaxWorkerThreads <= 96) {
                clusterLevel = 3;
            }
            else { // 16x and up
                clusterLevel = 4;
            }
        }
        return clusterLevel;
    }

    @Config("warp-speed.config.cluster-level")
    public void setClusterLevel(int clusterLevel)
    {
        this.clusterLevel = clusterLevel;
    }

    @Config("warp-speed.config.max-page-sources-without-warming-limit")
    public void setMaxPageSourcesWithoutWarmingLimit(int maxPageSourcesWithoutWarmingLimit)
    {
        this.maxPageSourcesWithoutWarmingLimit = maxPageSourcesWithoutWarmingLimit;
    }

    public int getMaxPageSourcesWithoutWarmingLimit()
    {
        return maxPageSourcesWithoutWarmingLimit;
    }

    @Min(0)
    public int getTaskMinWarmingThreads()
    {
        return taskMinWarmingThreads == 0 ? Math.max((taskMaxWorkerThreads / READERS_WARMERS_RATIO), MIN_WARMING_THREADS) : taskMinWarmingThreads;
    }

    @Config("warp-speed.config.task.min-warming-threads")
    public void setTaskMinWarmingThreads(int minWarmingThreads)
    {
        this.taskMinWarmingThreads = minWarmingThreads;
    }

    public Set<String> getUnsupportedNativeFunctions()
    {
        return unsupportedNativeFunctions;
    }

    @Config("warp-speed.debug.unsupported-native-functions")
    public void setUnsupportedNativeFunctions(String unsupportedNativeFunctionsAsString)
    {
        unsupportedNativeFunctions = Arrays.stream(unsupportedNativeFunctionsAsString.trim().split(",")).map(String::trim).collect(Collectors.toSet());
    }

    public Duration getStorageTemporaryExceptionDuration()
    {
        return storageTemporaryExceptionDuration;
    }

    @Config("warp-speed.config.storage-temp-except.duration")
    public void setStorageTemporaryExceptionDuration(io.airlift.units.Duration duration)
    {
        this.storageTemporaryExceptionDuration = duration.toJavaTime();
    }

    public int getStorageTemporaryExceptionNumTries()
    {
        return storageTemporaryExceptionNumTries;
    }

    @Config("warp-speed.config.storage-temp-except.num-tries")
    public void setStorageTemporaryExceptionNumTries(int storageTemporaryExceptionNumTries)
    {
        this.storageTemporaryExceptionNumTries = storageTemporaryExceptionNumTries;
    }

    public Duration getStorageTemporaryExceptionExpiryDuration()
    {
        return storageTemporaryExceptionExpiryDuration;
    }

    @Config("warp-speed.config.storage-temp-except.expiry.duration")
    public void setStorageTemporaryExceptionExpiryDuration(io.airlift.units.Duration duration)
    {
        this.storageTemporaryExceptionExpiryDuration = duration.toJavaTime();
    }

    public boolean isDebug()
    {
        return debugPanicHaltPolicy > 0;
    }

    private int string2CompressionUsersList(String str)
    {
        List<CompressionUsers> compressionUsers = Arrays.stream(str.split(",")).map(CompressionUsers::valueOf).toList();
        int res = 0;

        for (CompressionUsers user : compressionUsers) {
            res |= (1 << user.ordinal());
        }
        return res;
    }

    @Override
    public boolean equals(Object object)
    {
        if ((object == null) || (getClass() != object.getClass())) {
            return false;
        }
        NativeConfig that = (NativeConfig) object;
        return (lz4HcPercent == that.lz4HcPercent) &&
                (storageCacheSizeInPages == that.storageCacheSizeInPages) &&
                (skipIndexPercent == that.skipIndexPercent) &&
                (limitNumIosInParallel == that.limitNumIosInParallel) &&
                (maxIOMetadataSize == that.maxIOMetadataSize) &&
                (taskMaxWorkerThreads == that.taskMaxWorkerThreads) &&
                (taskMinWarmingThreads == that.taskMinWarmingThreads) &&
                (debugPanicHaltPolicy == that.debugPanicHaltPolicy) &&
                (clusterLevel == that.clusterLevel) &&
                (maxPageSourcesWithoutWarmingLimit == that.maxPageSourcesWithoutWarmingLimit) &&
                (storageTemporaryExceptionNumTries == that.storageTemporaryExceptionNumTries) &&
                (enableSingleChunk == that.enableSingleChunk) &&
                (enablePackedChunk == that.enablePackedChunk) &&
                (enableCompression == that.enableCompression) &&
                (exceptionalListCompression == that.exceptionalListCompression) &&
                Objects.equals(maxRecJufferSize, that.maxRecJufferSize) &&
                Objects.equals(collectTxSize, that.collectTxSize) &&
                Objects.equals(unsupportedNativeFunctions, that.unsupportedNativeFunctions) &&
                Objects.equals(storageTemporaryExceptionDuration, that.storageTemporaryExceptionDuration) &&
                Objects.equals(storageTemporaryExceptionExpiryDuration, that.storageTemporaryExceptionExpiryDuration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                maxRecJufferSize,
                lz4HcPercent,
                collectTxSize,
                storageCacheSizeInPages,
                skipIndexPercent,
                limitNumIosInParallel,
                maxIOMetadataSize,
                taskMaxWorkerThreads,
                taskMinWarmingThreads,
                debugPanicHaltPolicy,
                clusterLevel,
                maxPageSourcesWithoutWarmingLimit,
                unsupportedNativeFunctions,
                storageTemporaryExceptionDuration,
                storageTemporaryExceptionNumTries,
                storageTemporaryExceptionExpiryDuration,
                enableSingleChunk,
                enablePackedChunk,
                enableCompression,
                exceptionalListCompression);
    }
}
