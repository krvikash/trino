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
package io.trino.plugin.warp.cloudvendors.config;

import io.airlift.configuration.Config;

public class CloudVendorConfig
{
    public static final String STORE_TYPE = "warp-speed.config.store.type";
    public static final String STORE_PATH = "warp-speed.store.path";

    private StoreType storeType;
    private String storePath;

    public StoreType getStoreType()
    {
        if (storeType == null && storePath != null) {
            storeType = StoreType.byPathPrefix(getStorePath());
        }
        return storeType;
    }

    @Config(STORE_TYPE)
    public void setStoreType(String storeType)
    {
        this.storeType = StoreType.ofConfigName(null, storeType);
    }

    public String getStorePath()
    {
        return storePath;
    }

    @Config(STORE_PATH)
    public void setStorePath(String storePath)
    {
        this.storePath = storePath;
    }
}
