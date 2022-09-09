/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server.testing;

import com.google.common.io.Closer;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.starburst.stargate.buffer.data.server.MainModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestingDataServer
        implements Closeable
{
    public static final long BUFFER_NODE_ID = 0L;

    private final URI baseUri;
    private final Closer closer = Closer.create();

    private TestingDataServer(boolean discoveryBroadcastEnabled, Map<String, String> configProperties)
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new TestingJmxModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new EventModule(),
                new MainModule(BUFFER_NODE_ID, discoveryBroadcastEnabled));

        Injector injector = app
                .quiet()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(configProperties)
                .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        closer.register(lifeCycleManager::stop);

        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = UriBuilder.fromUri(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host("localhost")
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean discoveryBroadcastEnabled;
        private Map<String, String> configProperties = new HashMap<>();

        public Builder setDiscoveryBroadcastEnabled(boolean discoveryBroadcastEnabled)
        {
            this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
            return this;
        }

        public Builder setConfigProperties(Map<String, String> configProperties)
        {
            requireNonNull(configProperties, "configProperties is null");
            this.configProperties = new HashMap<>(configProperties);
            return this;
        }

        public Builder setConfigProperty(String key, String value)
        {
            this.configProperties.put(key, value);
            return this;
        }

        public TestingDataServer build()
        {
            return new TestingDataServer(discoveryBroadcastEnabled, configProperties);
        }
    }

    public URI getBaseUri()
    {
        return baseUri;
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
