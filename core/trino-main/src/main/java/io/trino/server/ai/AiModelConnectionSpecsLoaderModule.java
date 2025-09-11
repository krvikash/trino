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
package io.trino.server.ai;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.trino.node.AnnounceNodeAnnouncerConfig;
import io.trino.node.CoordinatorLocator;
import io.trino.node.DnsNodeInventoryConfig;
import io.trino.node.NodeInventoryConfig;
import io.trino.server.ServerConfig;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;

import java.net.URI;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.SwitchModule.switchModule;
import static io.trino.spi.connector.ai.ModelConnectionSpecsLoader.EMPTY_LOADER;

public class AiModelConnectionSpecsLoaderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            newOptionalBinder(binder, ModelConnectionSpecsLoader.class)
                    .setDefault()
                    .toInstance(EMPTY_LOADER);
        }
        else {
            install(switchModule(
                    NodeInventoryConfig.class,
                    NodeInventoryConfig::getType,
                    type -> switch (type) {
                        case AIRLIFT_DISCOVERY -> new AirliftNodeInventoryCoordinatorLocatorModule();
                        case ANNOUNCE -> new AnnounceNodeInventoryCoordinatorLocatorModule();
                        case DNS -> new DnsNodeInventoryCoordinatorLocatorModule();
                    }));
            newOptionalBinder(binder, ModelConnectionSpecsLoader.class).setBinding().to(RemoteModelConnectionSpecsLoader.class);
        }
    }

    private static class AnnounceNodeInventoryCoordinatorLocatorModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(AnnounceNodeAnnouncerConfig.class);
        }

        @Provides
        private CoordinatorLocator coordinatorLocator(AnnounceNodeAnnouncerConfig config)
        {
            return config::getCoordinatorUris;
        }
    }

    private static class AirliftNodeInventoryCoordinatorLocatorModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(DiscoveryClientConfig.class);
        }

        @Provides
        private CoordinatorLocator coordinatorUris(DiscoveryClientConfig config)
        {
            return () -> ImmutableList.of(config.getDiscoveryServiceURI());
        }
    }

    private static class DnsNodeInventoryCoordinatorLocatorModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(DnsNodeInventoryConfig.class);
        }

        @Provides
        private CoordinatorLocator coordinatorUris(DnsNodeInventoryConfig config)
        {
            return () -> config.getHosts().stream().map(URI::create).collect(toImmutableList());
        }
    }
}
