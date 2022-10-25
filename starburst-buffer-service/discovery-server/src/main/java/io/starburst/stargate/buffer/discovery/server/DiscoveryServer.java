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
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

public final class DiscoveryServer
{
    private static final Logger log = Logger.get(DiscoveryServer.class);

    private DiscoveryServer() {}

    public static void main(String[] args)
    {
        Bootstrap app = new Bootstrap(
                new EventModule(),
                new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                DiscoveryManagerModule.withSystemTicker(),
                new ServerModule());

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
