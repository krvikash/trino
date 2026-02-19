package io.trino.node;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.node.NodeConfig;
import io.trino.server.InternalCommunicationConfig;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class InternalCoordinatorLocatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(switch (buildConfigObject(NodeInventoryConfig.class).getType()) {
            case AIRLIFT_DISCOVERY -> new AirliftNodeInventoryCoordinatorLocatorModule();
            case ANNOUNCE -> new AnnounceNodeInventoryCoordinatorLocatorModule();
            case DNS -> new DnsNodeInventoryCoordinatorLocatorModule();
        });
    }

    private static class AnnounceNodeInventoryCoordinatorLocatorModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(AnnounceNodeAnnouncerConfig.class);
            configBinder(binder).bindConfig(InternalCommunicationConfig.class);
        }

        @Provides
        private InternalCoordinatorLocator coordinatorLocator(InternalNode currentNode, AnnounceNodeAnnouncerConfig config, NodeConfig nodeConfig, InternalCommunicationConfig internalCommunicationConfig)
        {
            if (currentNode.isCoordinator()) {
                return () -> ImmutableSet.of(currentNode.getInternalUri());
            }
            Set<URI> uris = ImmutableSet.copyOf(config.getCoordinatorUris());
            return encode(uris, nodeConfig, internalCommunicationConfig);
        }
    }

    private static class AirliftNodeInventoryCoordinatorLocatorModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(DiscoveryClientConfig.class);
            configBinder(binder).bindConfig(InternalCommunicationConfig.class);
        }

        @Provides
        private InternalCoordinatorLocator coordinatorUris(DiscoveryClientConfig config, NodeConfig nodeConfig, InternalCommunicationConfig internalCommunicationConfig)
        {
            URI uri = config.getDiscoveryServiceURI();
            return encode(ImmutableSet.of(uri), nodeConfig, internalCommunicationConfig);
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

    private static InternalCoordinatorLocator encode(Set<URI> uris, NodeConfig nodeConfig, InternalCommunicationConfig internalCommunicationConfig)
    {
        if (uris.isEmpty() || nodeConfig.getInternalAddressSource() == NodeConfig.AddressSource.FQDN) {
            return () -> uris;
        }
        if (!internalCommunicationConfig.isHttpsRequired() || internalCommunicationConfig.getKeyStorePath() != null || internalCommunicationConfig.getTrustStorePath() != null) {
            return () -> uris;
        }
        return () -> uris.stream()
                .map(InternalCommunicationForDiscoveryModule.DiscoveryEncodeAddressAsHostname::toIpEncodedAsHostnameUri)
                .collect(toImmutableSet());
    }
}
