/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.jfr.FlightRecorderModule;
import io.starburst.server.troubleshooting.jmx.JmxTroubleshootingProvider;
import io.starburst.server.troubleshooting.providers.FailureInfoProvider;
import io.starburst.server.troubleshooting.providers.QueryJsonProvider;
import io.starburst.server.troubleshooting.providers.QueryPlanProvider;
import io.starburst.server.troubleshooting.providers.RawQueryProvider;
import io.starburst.server.troubleshooting.providers.SessionInfoProvider;
import io.starburst.server.troubleshooting.providers.SoftwareVersionProvider;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.ServerConfig;
import jakarta.annotation.PreDestroy;
import jdk.jfr.FlightRecorder;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class TroubleshootingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (FlightRecorder.isAvailable()) {
            install(new FlightRecorderModule());
        }

        if (!buildConfigObject(ServerConfig.class).isCoordinator()) {
            return;
        }

        configBinder(binder).bindConfig(TroubleshootingConfig.class);
        binder.bind(TroubleshootingEventListener.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(TroubleshootingCoordinatorResource.class);
        binder.bind(TroubleshootingManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduledExecutorService.class).annotatedWith(ForTroubleshooting.class)
                .toInstance(newScheduledThreadPool(4, daemonThreadsNamed("query-troubleshooting-%s")));
        binder.bind(FullQueryInfoProvider.class).to(FullQueryInfoProviderDispatchManager.class).in(Scopes.SINGLETON);

        Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder, TroubleshootingProvider.class);
        setBinder.addBinding().to(QueryPlanProvider.class);
        setBinder.addBinding().to(SoftwareVersionProvider.class);
        setBinder.addBinding().to(FailureInfoProvider.class);
        setBinder.addBinding().to(SessionInfoProvider.class);
        setBinder.addBinding().to(RawQueryProvider.class);
        setBinder.addBinding().to(JmxTroubleshootingProvider.class);
        setBinder.addBinding().to(QueryJsonProvider.class);
    }

    @PreDestroy
    public void cleanup(@ForTroubleshooting ScheduledExecutorService executorService)
    {
        executorService.shutdownNow();
    }
}
