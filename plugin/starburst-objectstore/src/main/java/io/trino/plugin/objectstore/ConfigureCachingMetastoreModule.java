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
package io.trino.plugin.objectstore;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ConfigureCachingMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfigDefaults(CachingHiveMetastoreConfig.class, config -> {
            // CachingMetastore may cache the fact table does not exist. In ObjectStore connector there are 3 such metastores (Hive, Delta, Hudi).
            // Caching table non-existence may prevent accessing the table after it's created, as ObjectStoreMetadata.getTableHandle stops as soon as
            // any of the underlying connector reports "table does not exist".
            config.setCacheMissing(false)
                    .setCacheMissingPartitions(true)
                    // Caching non-existence of statistics is important to avoid repeatedly going to metastore for statistics from un-analyzed tables
                    .setCacheMissingStats(true);
        });
    }
}
