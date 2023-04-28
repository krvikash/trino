/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.ForTroubleshooting;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.ServerConfig;

import javax.inject.Provider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static java.util.Objects.requireNonNull;

public class FlightRecorderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FlightRecorderConfig.class);

        install(internalHttpClientModule("flight-recorder", ForTroubleshooting.class).build());
        binder.bind(LocalRecordingFactory.class);

        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            binder.bind(RemoteRecordingFactory.class).in(Scopes.SINGLETON);
            Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder, TroubleshootingProvider.class);
            setBinder.addBinding().to(FlightRecordingProvider.class);
            binder.bind(FlightRecordingFactory.class).toProvider(AggregatingRecordingFactoryProvider.class).in(Scopes.SINGLETON);
            binder.bind(FlightRecorderHttpClient.Factory.class).in(Scopes.SINGLETON);
            binder.bind(FlightRecorderHttpClient.WorkerNodesProvider.class).in(Scopes.SINGLETON);
        }
        else {
            jaxrsBinder(binder).bind(FlightRecorderWorkerResource.class);
        }
    }

    protected static class AggregatingRecordingFactoryProvider
            implements Provider<LocalRemoteCombiningFactory>
    {
        private final LocalRecordingFactory localRecordingFactory;
        private final RemoteRecordingFactory remoteRecordingFactory;

        @Inject
        public AggregatingRecordingFactoryProvider(LocalRecordingFactory localRecordingFactory, RemoteRecordingFactory remoteRecordingFactory)
        {
            this.localRecordingFactory = requireNonNull(localRecordingFactory, "localRecordingFactory is null");
            this.remoteRecordingFactory = requireNonNull(remoteRecordingFactory, "remoteRecordingFactory is null");
        }

        @Override
        public LocalRemoteCombiningFactory get()
        {
            return new LocalRemoteCombiningFactory(localRecordingFactory, remoteRecordingFactory);
        }
    }
}
