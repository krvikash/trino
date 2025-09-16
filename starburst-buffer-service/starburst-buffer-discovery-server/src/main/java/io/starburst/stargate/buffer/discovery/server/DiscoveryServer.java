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
import io.starburst.stargate.buffer.discovery.server.failures.FailuresTrackingManagerModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class DiscoveryServer
{
    private static final Logger log = Logger.get(DiscoveryServer.class);

    private DiscoveryServer() {}

    public static void main(String[] args)
    {
        BufferServiceSystemRequirements.verifySystemRequirements();
        Bootstrap app = new Bootstrap(
                new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxOpenMetricsModule(),
                new LogJmxModule(),
                new TracingModule("buffer-discover-server", firstNonNull(DiscoveryServer.class.getPackage().getImplementationVersion(), "unknown")),
                DiscoveryManagerModule.withSystemTicker(),
                FailuresTrackingManagerModule.withSystemTicker(),
                new DiscoveryServerMainModule());

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
