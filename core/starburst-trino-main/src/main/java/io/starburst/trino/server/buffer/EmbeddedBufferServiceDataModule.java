/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.trino.server.buffer;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.ForBufferDiscoveryClient;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;
import io.trino.node.InternalCoordinatorLocator;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.starburst.stargate.buffer.data.server.DataServerApplicationModules.getDataServerApplicationModules;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static io.trino.server.buffer.EmbeddedBufferServiceConfig.EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX;

public class EmbeddedBufferServiceDataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(getDataServerApplicationModules(EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX, true));
        configBinder(binder).bindConfigDefaults(DataServerConfig.class, config -> config.setTraceResourceReportingEnabled(false));

        // discovery client http config with internal communication
        install(internalHttpClientModule("buffer-discovery.http", ForBufferDiscoveryClient.class).build());
    }

    @Inject
    @Provides
    public DiscoveryApi getDiscoveryApi(@ForBufferDiscoveryClient HttpClient httpClient, InternalCoordinatorLocator locator)
    {
        return new HttpDiscoveryClient(
                () -> {
                    Set<URI> coordinatorUris = locator.getCoordinatorUris();
                    if (coordinatorUris.isEmpty()) {
                        throw new IllegalStateException("No coordinators available");
                    }
                    if (coordinatorUris.size() > 1) {
                        throw new IllegalStateException("Multiple coordinators not supported");
                    }
                    return getOnlyElement(coordinatorUris);
                },
                httpClient);
    }
}
