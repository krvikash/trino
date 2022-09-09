/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server.testing;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.starburst.stargate.buffer.discovery.server.DiscoveryManagerModule;
import io.starburst.stargate.buffer.discovery.server.ServerModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingDiscoveryServer
        implements Closeable
{
    private final Injector injector;
    private final URI baseUri;

    private final Closer closer = Closer.create();

    private TestingDiscoveryServer(
            Map<String, String> configProperties,
            Optional<Ticker> discoveryManagerTicker)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new TestingJmxModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new ServerModule());

        modules.add(discoveryManagerTicker
                .map(DiscoveryManagerModule::withTicker)
                .orElse(DiscoveryManagerModule.withSystemTicker()));

        Bootstrap app = new Bootstrap(modules.build());

        injector = app
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

    public <T> T getInstance(Class<T> type)
    {
        return injector.getInstance(type);
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Map<String, String> configProperties = new HashMap<>();
        private Optional<Ticker> discoveryManagerTicker = Optional.empty();

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

        public Builder setDiscoveryManagerTicker(Ticker discoveryManagerTicker)
        {
            requireNonNull(discoveryManagerTicker, "discoveryManagerTicker is null");
            this.discoveryManagerTicker = Optional.of(discoveryManagerTicker);
            return this;
        }

        public TestingDiscoveryServer build()
        {
            return new TestingDiscoveryServer(configProperties, discoveryManagerTicker);
        }
    }
}
