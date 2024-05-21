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
    public static final String STORE_TYPE = "warp-speed.objectstore.store.type";
    public static final String STORE_PATH = "warp-speed.objectstore.store.path";
    public static final String REGION = "warp-speed.objectstore.store.region";
    public static final String WARMUP_FETCH_DURATION = "warp-speed.objectstore.warmup.fetch.duration";

    private Duration fetchDuration = Duration.ofHours(1);
    private Duration fetchDelayDuration = Duration.ofMinutes(1);
    private int downloadRetries = 3;
    private Duration downloadDuration = Duration.ofSeconds(10);

    public WarmupRuleCloudFetcherConfig() {}

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

    @Config("warp-speed.objectstore.warmup.fetch.delay.duration")
    public void setFetchDelayDuration(io.airlift.units.Duration fetchDelayDuration)
    {
        this.fetchDelayDuration = fetchDelayDuration.toJavaTime();
    }

    public int getDownloadRetries()
    {
        return downloadRetries;
    }

    @Config("warp-speed.objectstore.warmup.cloud.retries")
    public void setDownloadRetries(int downloadRetries)
    {
        this.downloadRetries = downloadRetries;
    }

    public Duration getDownloadDuration()
    {
        return downloadDuration;
    }

    @Config("warp-speed.objectstore.warmup.cloud.duration")
    public void setDownloadDuration(io.airlift.units.Duration downloadDuration)
    {
        this.downloadDuration = downloadDuration.toJavaTime();
    }

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

    @Config(REGION)
    @Override
    public void setRegion(String region)
    {
        super.setRegion(region);
    }
}
