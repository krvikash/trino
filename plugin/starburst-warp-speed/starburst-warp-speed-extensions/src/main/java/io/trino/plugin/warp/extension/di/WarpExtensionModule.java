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
package io.trino.plugin.warp.extension.di;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.trino.plugin.warp.annotation.ForWarmupRuleCloudFetcher;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.plugin.warp.di.WarmupCloudFetcherModule;
import io.trino.plugin.warp.di.WarpBaseModule;
import io.trino.plugin.warp.di.WarpClientModule;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.warp.extension.config.CallHomeConfig;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.extension.execution.ClusterReadyTaskExecutionIsAllowedSupplier;
import io.trino.plugin.warp.extension.execution.WarpTasksModule;
import io.trino.plugin.warp.extension.execution.WorkerReadyTaskExecutionIsAllowedSupplier;
import io.trino.plugin.warp.extension.execution.callhome.CallHomeService;
import io.trino.plugin.warp.extension.warmup.WorkerWarmupRuleFetcher;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;
import java.util.function.BooleanSupplier;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class WarpExtensionModule
        extends AbstractModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext connectorContext;
    private String catalogName;

    @SuppressWarnings("unused")
    public WarpExtensionModule() {}

    public WarpExtensionModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        this.config = requireNonNull(config);
        this.connectorContext = requireNonNull(connectorContext);
        this.catalogName = requireNonNull(catalogName);
    }

    @Override
    public void configure()
    {
        binder().install(new WarpClientModule(config));

        ImmutableSet.Builder<Class<? extends BooleanSupplier>> booleanSuppliers = ImmutableSet.builder();
        if (connectorContext.getNodeManager().getCurrentNode().isCoordinator()) {
            booleanSuppliers.add(ClusterReadyTaskExecutionIsAllowedSupplier.class);
        }
        else {
            booleanSuppliers.add(WorkerReadyTaskExecutionIsAllowedSupplier.class);
        }
        binder().install(
                new WarpTasksModule(
                        WarpBaseModule.isCoordinator(connectorContext),
                        WarpBaseModule.isWorker(connectorContext, config),
                        booleanSuppliers.build()));
        if (!Boolean.parseBoolean(config.getOrDefault(WarpExtensionConfig.USE_HTTP_SERVER_PORT, Boolean.TRUE.toString()))) {
            configureHttpServer();
        }

        binder().bind(CallHomeService.class);
        configBinder(binder()).bindConfig(WarpExtensionConfig.class);
        configBinder(binder()).bindConfig(CallHomeConfig.class);

        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig = configFactory.build(WarmupRuleCloudFetcherConfig.class);
        CacheManagerConfig cacheManagerConfig = configFactory.build(CacheManagerConfig.class);
        if (WarpBaseModule.isWorker(connectorContext, config) &&
                !cacheManagerConfig.getIsCache() &&
                StringUtils.isEmpty(warmupRuleCloudFetcherConfig.getStorePath())) {
            binder().bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(WorkerWarmupRuleFetcher.class);
            configBinder(binder()).bindConfig(WarmupRuleCloudFetcherConfig.class, ForWarmupRuleCloudFetcher.class);
        }
        else {
            binder().install(new WarmupCloudFetcherModule(config, connectorContext, catalogName));
        }
    }

    @Override
    public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        return new WarpExtensionModule(config, connectorContext, catalogName);
    }

    private void configureHttpServer()
    {
        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        JaxrsModule jaxrsModule = new JaxrsModule();
        configFactory.registerConfigurationClasses(jaxrsModule);
        HttpServerModule httpServerModule = new HttpServerModule();
        configFactory.registerConfigurationClasses(httpServerModule);
        binder().install(new NodeModule());
        binder().install(httpServerModule);
        binder().install(new JsonModule());
        WarpJaxrsModule module = new WarpJaxrsModule();
        module.setConfigurationFactory(configFactory);
        binder().install(module);
        binder().install(binder1 -> binder1.bind(HttpServerLifeCycleHandler.class));
    }
}
