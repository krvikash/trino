/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.starburst.stargate.buffer.BufferServiceSystemRequirements;
import org.weakref.jmx.guice.MBeanModule;

import static io.starburst.stargate.buffer.discovery.server.DiscoveryServerApplicationModules.getDiscoveryServerApplicationModule;
import static java.util.Objects.requireNonNullElse;

public final class DiscoveryServer
{
    private static final Logger log = Logger.get(DiscoveryServer.class);

    private DiscoveryServer() {}

    public static void main(String[] args)
    {
        BufferServiceSystemRequirements.verifySystemRequirements();

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxOpenMetricsModule(),
                new LogJmxModule(),
                new TracingModule("buffer-discover-server", requireNonNullElse(DiscoveryServer.class.getPackage().getImplementationVersion(), "unknown")),
                getDiscoveryServerApplicationModule());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            app.initialize();
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
