/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerModule;
import io.airlift.http.server.ServerFeature;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.starburst.stargate.buffer.BufferServiceSystemRequirements;
import io.starburst.stargate.buffer.status.StatusModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.ServerFeature.VIRTUAL_THREADS;
import static io.starburst.stargate.buffer.BufferNodeState.STARTED;
import static io.starburst.stargate.buffer.data.server.DataServerApplicationModules.getDataServerApplicationModule;
import static io.starburst.stargate.buffer.data.server.HttpServerFeaturesConfig.VIRTUAL_THREADS_CONFIG_PREFIX;
import static java.util.Objects.requireNonNullElse;

public final class DataServer
{
    private static final Logger log = Logger.get(DataServer.class);

    private DataServer() {}

    static void main()
    {
        BufferServiceSystemRequirements.verifySystemRequirements();
        String injectedVersion = System.getenv("BUFFER_DATA_SERVER_DOCKER_VERSION");
        checkState(injectedVersion == null || !injectedVersion.isEmpty(), "BUFFER_DATA_SERVER_DOCKER_VERSION is set but empty");
        String version = requireNonNullElse(injectedVersion, DataServer.class.getPackage().getImplementationVersion());

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(new NodeModule(),
                new HttpServerModule(),
                new JaxrsModule(),
                new JsonModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxOpenMetricsModule(),
                new LogJmxModule(),
                new TracingModule("buffer-data-server", version),
                new StatusModule());
        modules.add(getVirtualThreadsServerModule());
        modules.add(getDataServerApplicationModule());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.initialize();
            injector.getInstance(BufferNodeStateManager.class).transitionState(STARTED);

            log.info("======== SERVER STARTED ========");
        }
        catch (ApplicationConfigurationException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    private static AbstractConfigurationAwareModule getVirtualThreadsServerModule()
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                HttpServerFeaturesConfig featuresConfig = buildConfigObject(HttpServerFeaturesConfig.class);
                if (featuresConfig.isVirtualThreadsEnabled()) {
                    configBinder(binder)
                            .bindConfigDefaults(
                                    HttpServerConfig.class,
                                    VirtualThreadsDataServer.class,
                                    config -> config.setHttpPort(8085));

                    newSetBinder(binder, ServerFeature.class, VirtualThreadsDataServer.class)
                            .addBinding().toInstance(VIRTUAL_THREADS);

                    install(new HttpServerModule("virtual-thread-http-server", VirtualThreadsDataServer.class, VIRTUAL_THREADS_CONFIG_PREFIX));
                    install(new JaxrsModule(VirtualThreadsDataServer.class));
                }
            }
        };
    }
}
