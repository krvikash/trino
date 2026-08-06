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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNativeConfig
{
    @Test
    public void testDefaults()
    {
        NativeConfig nativeConfig = new NativeConfig();
        assertThat(nativeConfig.getMaxRecJufferSize()).isEqualTo(10 * 1024 * 1024);
        assertThat(nativeConfig.getCompressionLevel()).isEqualTo(10);
        assertThat(nativeConfig.getCollectTxSize()).isEqualTo(10 * 1024 * 1024);
        // derived from host memory, bounded by the clamp
        assertThat(nativeConfig.getStorageCacheMemoryPercent()).isEqualTo(20);
        assertThat(nativeConfig.getSkipIndexPercent()).isEqualTo(80);
        assertThat(nativeConfig.getLimitNumIosInParallel()).isEqualTo(2400);
        assertThat(nativeConfig.getMaxIOMetadataSize()).isEqualTo(24);
        assertThat(nativeConfig.getEnableSingleChunk()).isTrue();
        assertThat(nativeConfig.getEnablePackedChunk()).isTrue();
        assertThat(nativeConfig.getEnableCompression()).isTrue();
        assertThat(nativeConfig.getExceptionalListCompression()).isEqualTo(0);
        assertThat(nativeConfig.getTaskMaxWorkerThreads()).isEqualTo(Runtime.getRuntime().availableProcessors() * 2);
        assertThat(nativeConfig.getDebugPanicHaltPolicy()).isEqualTo(0);
        // derived from the worker thread count
        assertThat(nativeConfig.getMaxPageSourcesWithoutWarmingLimit()).isEqualTo(8);
        assertThat(nativeConfig.getTaskMinWarmingThreads()).isEqualTo(Math.max(Runtime.getRuntime().availableProcessors() * 2 / 32, 2));
        assertThat(nativeConfig.getUnsupportedNativeFunctions()).isEmpty();
        assertThat(nativeConfig.getStorageTemporaryExceptionNumTries()).isEqualTo(3);
        assertThat(nativeConfig.getStorageTemporaryExceptionExpiryDuration()).isEqualTo(Duration.of(1, ChronoUnit.HOURS));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("warp-speed.config.max-rec-juffer-size-mb", "12")
                .put("warp-speed.config.task.lz4-hc-percent", "11")
                .put("warp-speed.config.collect-tx-size-mb", "12")
                .put("warp-speed.storage-cache.memory-percent", "40")
                .put("warp-speed.config.skip-index-percent", "70")
                .put("warp-speed.config.limit-num-ios-in-parallel", "1200")
                .put("warp-speed.config.max-io-metadata-size", "48")
                .put("warp-speed.enable.single-chunk", "false")
                .put("warp-speed.enable.packed-chunk", "false")
                .put("warp-speed.enable.compression", "false")
                .put("warp-speed.enable.compression.exceptional-list", "COMPRESSION_USERS_DATA,COMPRESSION_USERS_CHUNKS_MAP")
                .put("warp-speed.config.task.max-worker-threads", "7")
                .put("warp-speed.debug.panic-halt-policy", "2")
                .put("warp-speed.config.max-page-sources-without-warming-limit", "16")
                .put("warp-speed.config.task.min-warming-threads", "1")
                .put("warp-speed.debug.unsupported-native-functions", "lower, upper")
                .put("warp-speed.config.storage-temp-except.num-tries", "6")
                .put("warp-speed.config.storage-temp-except.expiry.duration", "10m")
                .buildOrThrow();

        NativeConfig expected = new NativeConfig();
        expected.setMaxRecJufferSize(12);
        expected.setCompressionLevel(11);
        expected.setCollectTxSize(12);
        expected.setStorageCacheMemoryPercent(40);
        expected.setSkipIndexPercent(70);
        expected.setLimitNumIosInParallel(1200);
        expected.setMaxIOMetadataSize(48);
        expected.setEnableSingleChunk(false);
        expected.setEnablePackedChunk(false);
        expected.setEnableCompression(false);
        expected.setExceptionalListCompression("COMPRESSION_USERS_DATA,COMPRESSION_USERS_CHUNKS_MAP");
        expected.setTaskMaxWorkerThreads(7);
        expected.setDebugPanicHaltPolicy(2);
        expected.setMaxPageSourcesWithoutWarmingLimit(16);
        expected.setTaskMinWarmingThreads(1);
        expected.setUnsupportedNativeFunctions("lower, upper");
        expected.setStorageTemporaryExceptionNumTries(6);
        expected.setStorageTemporaryExceptionExpiryDuration(new io.airlift.units.Duration(10, TimeUnit.MINUTES));

        assertThat(expected.getUnsupportedNativeFunctions()).isEqualTo(ImmutableSet.of("lower", "upper"));
        assertFullMapping(properties, expected);
    }
}
