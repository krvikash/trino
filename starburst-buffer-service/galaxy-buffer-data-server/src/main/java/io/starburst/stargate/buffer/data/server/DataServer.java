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

import com.google.inject.Injector;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.tracing.TracingModule;
import io.starburst.stargate.buffer.status.StatusModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.BufferNodeState.STARTED;

public final class DataServer
{
    private static final Logger log = Logger.get(DataServer.class);

    private DataServer() {}

    public static void main(String[] args)
    {
        String injectedVersion = System.getenv("BUFFER_DATA_SERVER_DOCKER_VERSION");
        checkState(injectedVersion == null || !injectedVersion.isEmpty(), "BUFFER_DATA_SERVER_DOCKER_VERSION is set but empty");
        String version = firstNonNull(injectedVersion, DataServer.class.getPackage().getImplementationVersion());

        Bootstrap app = new Bootstrap(
                new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxOpenMetricsModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new TracingModule("buffer-data-server", version),
                new EventModule(),
                new StatusModule(),
                new DiscoveryApiModule(),
                new MainModule());

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
}
