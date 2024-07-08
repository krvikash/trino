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
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.configdump.ConfigDumpProvider;
import io.starburst.server.troubleshooting.configdump.ConfigDumpResource;
import io.starburst.server.troubleshooting.configdump.ConfigDumper;
import io.starburst.server.troubleshooting.configdump.RemoteConfigDumpClient;
import io.starburst.server.troubleshooting.jfr.FlightRecorderConfig;
import io.starburst.server.troubleshooting.jfr.FlightRecorderModule;
import io.starburst.server.troubleshooting.jmx.JmxTroubleshootingProvider;
import io.starburst.server.troubleshooting.providers.QueryJsonProvider;
import io.starburst.server.troubleshooting.providers.QueryPlanProvider;
import io.starburst.server.troubleshooting.providers.SoftwareVersionProvider;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.starburst.server.troubleshooting.tracing.OpenTelemetryTraceProvider;
import io.starburst.server.troubleshooting.tracing.RemoteTroubleshootingTraceClient;
import io.starburst.server.troubleshooting.tracing.SpanInterceptor;
import io.starburst.server.troubleshooting.tracing.SpanSerializer;
import io.starburst.server.troubleshooting.tracing.TroubleshootingSpanProcessor;
import io.starburst.server.troubleshooting.tracing.TroubleshootingTraceResource;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.server.ServerConfig;
import jdk.jfr.FlightRecorder;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class TroubleshootingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        FlightRecorderConfig flightRecorderConfig = buildConfigObject(FlightRecorderConfig.class);
        // flight recording file size can be bigger than maxRecordingSize by a few MB due to how jvm JFR profile flushing to disk works
        DataSize maxContentLength = DataSize.ofBytes(
                flightRecorderConfig.getMaxRecordingSize().toBytes() + DataSize.of(8, DataSize.Unit.MEGABYTE).toBytes());
        install(internalHttpClientModule("troubleshooting", ForTroubleshooting.class)
                .withConfigDefaults(httpClientConfig -> httpClientConfig.setMaxContentLength(maxContentLength))
                .build());
        if (FlightRecorder.isAvailable()) {
            install(new FlightRecorderModule());
        }
        binder.bind(SpanSerializer.class);
        binder.bind(SpanInterceptor.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SpanProcessor.class).addBinding().to(TroubleshootingSpanProcessor.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(TroubleshootingTraceResource.class);
        binder.bind(ConfigDumper.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(ConfigDumpResource.class);

        if (!buildConfigObject(ServerConfig.class).isCoordinator()) {
            return;
        }

        configBinder(binder).bindConfig(TroubleshootingConfig.class);
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(TroubleshootingSessionProperties.class);
        binder.bind(TroubleshootingEventListener.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(TroubleshootingCoordinatorResource.class);
        binder.bind(TroubleshootingContextManager.class).in(Scopes.SINGLETON);
        ScheduledExecutorService troubleshootingExecutor = newScheduledThreadPool(4, daemonThreadsNamed("query-troubleshooting-%s"));
        binder.bind(ScheduledExecutorService.class).annotatedWith(ForTroubleshooting.class)
                .toInstance(troubleshootingExecutor);
        binder.bind(FullQueryInfoProvider.class).to(FullQueryInfoProviderDispatchManager.class).in(Scopes.SINGLETON);
        binder.bind(TroubleshootingArchiver.class).in(Scopes.SINGLETON);
        binder.bind(RemoteTroubleshootingTraceClient.class).in(Scopes.SINGLETON);
        binder.bind(RemoteConfigDumpClient.class).in(Scopes.SINGLETON);

        Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder, TroubleshootingProvider.class);
        setBinder.addBinding().to(QueryPlanProvider.class);
        setBinder.addBinding().to(SoftwareVersionProvider.class);
        setBinder.addBinding().to(JmxTroubleshootingProvider.class);
        setBinder.addBinding().to(QueryJsonProvider.class);
        setBinder.addBinding().to(OpenTelemetryTraceProvider.class);
        setBinder.addBinding().to(ConfigDumpProvider.class);

        closingBinder(binder)
                .registerExecutor(Key.get(ScheduledExecutorService.class, ForTroubleshooting.class));
    }
}
