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

import io.airlift.configuration.Config;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;

import java.time.Duration;

public class WarmupRuleCloudFetcherConfig
        extends CloudVendorConfig
{
    public static final String PREFIX = "warp-speed.objectstore";
    public static final String STORE_TYPE = PREFIX + ".store.type";
    public static final String STORE_PATH = PREFIX + ".store.path";
    public static final String HADOOP_ENABLED = PREFIX + ".fs.hadoop.enabled";
    public static final String NATIVE_S3_ENABLED = PREFIX + ".fs.s3.enabled";
    public static final String REGION = PREFIX + ".s3.region";
    public static final String WARMUP_FETCH_DURATION = PREFIX + ".warmup.fetch.duration";
    public static final String WARMUP_FETCH_DELAY_DURATION = PREFIX + ".warmup.fetch.delay.duration";

    private boolean hadoopEnabled;
    private boolean nativeS3Enabled;
    private String region;
    private Duration fetchDuration = Duration.ofHours(1);
    private Duration fetchDelayDuration = Duration.ofMinutes(1);

    public WarmupRuleCloudFetcherConfig() {}

    @Config(STORE_TYPE)
    @Override
    public void setStoreType(String storeType)
    {
        super.setStoreType(storeType);
    }

    @Config(STORE_PATH)
    @Override
    public void setStorePath(String storePath)
    {
        super.setStorePath(storePath);
    }

    public String getRegion()
    {
        return region;
    }

    @Config(REGION)
    public void setRegion(String region)
    {
        this.region = region;
    }

    public boolean isHadoopEnabled()
    {
        return hadoopEnabled;
    }

    @Config(HADOOP_ENABLED)
    public void setHadoopEnabled(boolean hadoopEnabled)
    {
        this.hadoopEnabled = hadoopEnabled;
    }

    public boolean isNativeS3Enabled()
    {
        return nativeS3Enabled;
    }

    @Config(NATIVE_S3_ENABLED)
    public void setNativeS3Enabled(boolean nativeS3Enabled)
    {
        this.nativeS3Enabled = nativeS3Enabled;
    }

    public Duration getFetchDuration()
    {
        return fetchDuration;
    }

    @Config(WARMUP_FETCH_DURATION)
    public void setFetchDuration(io.airlift.units.Duration fetchDuration)
    {
        this.fetchDuration = fetchDuration.toJavaTime();
    }

    public Duration getFetchDelayDuration()
    {
        return fetchDelayDuration;
    }

    @Config(WARMUP_FETCH_DELAY_DURATION)
    public void setFetchDelayDuration(io.airlift.units.Duration fetchDelayDuration)
    {
        this.fetchDelayDuration = fetchDelayDuration.toJavaTime();
    }
}
