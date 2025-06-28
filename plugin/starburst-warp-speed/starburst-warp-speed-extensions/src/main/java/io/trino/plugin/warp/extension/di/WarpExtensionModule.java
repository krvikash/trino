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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
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
        extends AbstractConfigurationAwareModule
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
    public void setup(Binder binder)
    {
        install(new WarpClientModule());

        ImmutableSet.Builder<Class<? extends BooleanSupplier>> booleanSuppliers = ImmutableSet.builder();
        boolean isCoordinator = connectorContext.getCurrentNode().isCoordinator();
        boolean isWorker = WarpBaseModule.isSingle(config) || !isCoordinator;
        if (isCoordinator) {
            booleanSuppliers.add(ClusterReadyTaskExecutionIsAllowedSupplier.class);
        }
        else {
            booleanSuppliers.add(WorkerReadyTaskExecutionIsAllowedSupplier.class);
        }
        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        CacheManagerConfig cacheManagerConfig = configFactory.build(CacheManagerConfig.class);
        install(
                new WarpTasksModule(
                        isCoordinator,
                        isWorker,
                        cacheManagerConfig.getIsCache(),
                        booleanSuppliers.build()));
        if (!Boolean.parseBoolean(config.getOrDefault(WarpExtensionConfig.USE_HTTP_SERVER_PORT, "true"))) {
            configureHttpServer();
        }

        configBinder(binder).bindConfig(CallHomeConfig.class);
        CallHomeConfig callHomeConfig = configFactory.build(CallHomeConfig.class);
        OptionalBinder.newOptionalBinder(binder, CallHomeService.class);
        if (callHomeConfig.isEnable()) {
            binder.bind(CallHomeService.class);
        }

        configBinder(binder).bindConfig(WarpExtensionConfig.class);

        WarmupRuleCloudFetcherConfig warmupRuleCloudFetcherConfig = configFactory.build(WarmupRuleCloudFetcherConfig.class);
        if (isWorker &&
                !cacheManagerConfig.getIsCache() &&
                StringUtils.isEmpty(warmupRuleCloudFetcherConfig.getStorePath())) {
            binder.bind(new TypeLiteral<WarmupRuleFetcher<WarmupRule>>() {}).to(WorkerWarmupRuleFetcher.class);
            configBinder(binder).bindConfig(WarmupRuleCloudFetcherConfig.class, ForWarmupRuleCloudFetcher.class);
        }
        else {
            install(new WarmupCloudFetcherModule(config, connectorContext, catalogName));
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

        install(new NodeModule());
        install(httpServerModule);
        install(new JsonModule());

        CacheManagerConfig cacheManagerConfig = configFactory.build(CacheManagerConfig.class);
        boolean isCoordinator = connectorContext.getCurrentNode().isCoordinator();
        boolean isWorker = WarpBaseModule.isSingle(config) || !isCoordinator;
        WarpJaxrsModule module = new WarpJaxrsModule(
                isCoordinator,
                isWorker,
                cacheManagerConfig.getIsCache());

        module.setConfigurationFactory(configFactory);
        install(module);
        install(binder1 -> binder1.bind(HttpServerLifeCycleHandler.class));
    }
}
