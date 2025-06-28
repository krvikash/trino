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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.warp.annotation.ForWarmupRuleCloudFetcher;
import io.trino.plugin.warp.cloudvendors.CloudVendorModule;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.CacheMgrWarmupRuleCloudFetcher;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.EmptyCacheMgrWarmupRuleFetcher;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.EmptyWarmupRuleFetcher;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcher;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.warp.di.WarpBaseModule.isSingle;
import static java.util.Objects.requireNonNull;

public class WarmupCloudFetcherModule
        implements InitializationModule
{
    private final Map<String, String> config;
    private final ConnectorContext context;
    private final String catalogName;

    @SuppressWarnings("unused")
    public WarmupCloudFetcherModule(
            Map<String, String> config,
            ConnectorContext context,
            String catalogName)
    {
        this.config = requireNonNull(config);
        this.context = context;
        this.catalogName = catalogName;
    }

    @SuppressWarnings("unused")
    @Override
    public InitializationModule createModule(
            Map<String, String> config,
            ConnectorContext context,
            String catalogName)
    {
        return new WarmupCloudFetcherModule(config, context, catalogName);
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(WarmupRuleCloudFetcherConfig.class, ForWarmupRuleCloudFetcher.class);

        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig = configFactory.build(WarmupRuleCloudFetcherConfig.class);
        boolean isWorker = isSingle(config) || !context.getCurrentNode().isCoordinator();
        if (isWorker) {
            if (StringUtils.isEmpty(warmupRuleCloudFetcherConfig.getStorePath())) {
                binder.bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(EmptyWarmupRuleFetcher.class);
                binder.bind(new TypeLiteral<WarmupRuleFetcher<CacheManagerRule>>() {}).to(EmptyCacheMgrWarmupRuleFetcher.class);
            }
            else {
                binder.install(
                        CloudVendorModule.getModule(
                                context,
                                WarmupRuleCloudFetcherConfig.PREFIX,
                                ForWarmupRuleCloudFetcher.class,
                                catalogName,
                                config,
                                WarmupRuleCloudFetcherConfig.STORE_PATH,
                                WarmupRuleCloudFetcherConfig.STORE_TYPE,
                                WarmupRuleCloudFetcherConfig.class));

                CacheManagerConfig cacheManagerConfig = configFactory.build(CacheManagerConfig.class);
                if (cacheManagerConfig.getIsCache()) {
                    binder.bind(new TypeLiteral<WarmupRuleFetcher<CacheManagerRule>>() {}).to(CacheMgrWarmupRuleCloudFetcher.class);
                    binder.bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(EmptyWarmupRuleFetcher.class);
                }
                else {
                    binder.bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(WarmupRuleCloudFetcher.class);
                    binder.bind(new TypeLiteral<WarmupRuleFetcher<CacheManagerRule>>() {}).to(EmptyCacheMgrWarmupRuleFetcher.class);
                }
            }
        }
        else {
            binder.bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(EmptyWarmupRuleFetcher.class);
            binder.bind(new TypeLiteral<WarmupRuleFetcher<CacheManagerRule>>() {}).to(EmptyCacheMgrWarmupRuleFetcher.class);
        }
    }
}
