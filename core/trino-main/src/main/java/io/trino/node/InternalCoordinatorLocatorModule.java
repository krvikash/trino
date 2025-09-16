package io.trino.node;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.DiscoveryClientConfig;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.SwitchModule.switchModule;

public class InternalCoordinatorLocatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(switchModule(
                NodeInventoryConfig.class,
                NodeInventoryConfig::getType,
                type -> switch (type) {
                    case AIRLIFT_DISCOVERY -> new AirliftNodeInventoryCoordinatorLocatorModule();
                    case ANNOUNCE -> new AnnounceNodeInventoryCoordinatorLocatorModule();
                    case DNS -> new DnsNodeInventoryCoordinatorLocatorModule();
                }));
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
        private InternalCoordinatorLocator coordinatorLocator(AnnounceNodeAnnouncerConfig config)
        {
            Set<URI> uris = ImmutableSet.copyOf(config.getCoordinatorUris());
            return () -> uris;
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
        private InternalCoordinatorLocator coordinatorUris(DiscoveryClientConfig config)
        {
            URI uris = config.getDiscoveryServiceURI();
            return () -> ImmutableSet.of(uris);
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
        private InternalCoordinatorLocator coordinatorUris(DnsNodeInventoryConfig config)
        {
            Set<String> hosts = config.getHosts();
            return () -> hosts.stream().map(URI::create).collect(toImmutableSet());
        }
    }
}
