/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.CoordinatorLocator;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerContext;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BufferExchangeManagerFactory
        implements ExchangeManagerFactory
{
    private final String name;
    private final Optional<ApiFactory> apiFactory;
    private final Optional<InternalCommunicationDependencies> internalCommunicationDependencies;

    public static class RealBufferExchangeManagerFactoryProvider
            implements Provider<BufferExchangeManagerFactory>
    {
        private final InternalCommunicationDependencies internalCommunicationDependencies;

        @Inject
        public RealBufferExchangeManagerFactoryProvider(InternalCommunicationDependencies internalCommunicationDependencies)
        {
            this.internalCommunicationDependencies = requireNonNull(internalCommunicationDependencies, "internalCommunicationConfig is null");
        }

        @Override
        public BufferExchangeManagerFactory get()
        {
            return new BufferExchangeManagerFactory("buffer", Optional.empty(), Optional.of(internalCommunicationDependencies));
        }
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory forRealBufferService()
    {
        return new BufferExchangeManagerFactory("buffer", Optional.empty(), Optional.empty());
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory withApiFactory(String name, ApiFactory apiFactory)
    {
        return new BufferExchangeManagerFactory(name, Optional.of(apiFactory), Optional.empty());
    }

    private BufferExchangeManagerFactory(String name, Optional<ApiFactory> apiFactory, Optional<InternalCommunicationDependencies> internalCommunicationDependencies)
    {
        this.name = requireNonNull(name, "name is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.internalCommunicationDependencies = requireNonNull(internalCommunicationDependencies, "internalCommunicationDependencies is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ExchangeManager create(Map<String, String> config, ExchangeManagerContext exchangeManagerContext)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.starburst.stargate.buffer.trino.exchange", "io.starburst.buffer.exchange"),
                new JsonModule(),
                new BufferExchangeModule(apiFactory, internalCommunicationDependencies),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(exchangeManagerContext.getOpenTelemetry());
                    binder.bind(CoordinatorLocator.class).toInstance(exchangeManagerContext.getCoordinatorLocator());
                    binder.bind(Tracer.class).toInstance(exchangeManagerContext.getTracer());
                });

        ImmutableMap.Builder<String, String> extendedConfig = ImmutableMap.builder();
        extendedConfig.putAll(config);
        internalCommunicationDependencies.ifPresent(internalCommunicationDependencies -> {
            InternalCommunicationConfig internalCommunicationConfig = internalCommunicationDependencies.getInternalCommunicationConfig();

            internalCommunicationConfig.getSharedSecret().ifPresent(sharedSecret -> {
                extendedConfig.put("internal-communication.shared-secret", sharedSecret);
            });
            extendedConfig.put("internal-communication.http2.enabled", String.valueOf(internalCommunicationConfig.isHttp2Enabled()));
            extendedConfig.put("internal-communication.https.required", String.valueOf(internalCommunicationConfig.isHttpsRequired()));
            if (internalCommunicationConfig.getKeyStorePath() != null) {
                extendedConfig.put("internal-communication.https.keystore.path", internalCommunicationConfig.getKeyStorePath());
            }
            if (internalCommunicationConfig.getKeyStorePassword() != null) {
                extendedConfig.put("internal-communication.https.keystore.key", internalCommunicationConfig.getKeyStorePassword());
            }
            if (internalCommunicationConfig.getTrustStorePath() != null) {
                extendedConfig.put("internal-communication.https.truststore.path", internalCommunicationConfig.getTrustStorePath());
            }
            if (internalCommunicationConfig.getTrustStorePassword() != null) {
                extendedConfig.put("internal-communication.https.truststore.key", internalCommunicationConfig.getTrustStorePassword());
            }
            extendedConfig.put("http-server.https.enabled", String.valueOf(internalCommunicationConfig.isHttpServerHttpsEnabled()));
        });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(extendedConfig.buildOrThrow())
                .initialize();

        return injector.getInstance(BufferExchangeManager.class);
    }

    public static class InternalCommunicationDependencies
    {
        private final InternalCommunicationConfig internalCommunicationConfig;
        private final NodeInfo nodeInfo;
        private final SecurityConfig securityConfig;

        @Inject
        public InternalCommunicationDependencies(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo, SecurityConfig securityConfig)
        {
            this.internalCommunicationConfig = requireNonNull(internalCommunicationConfig, "internalCommunicationConfig is null");
            this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
            this.securityConfig = requireNonNull(securityConfig, "securityConfig is null");
        }

        public InternalCommunicationConfig getInternalCommunicationConfig()
        {
            return internalCommunicationConfig;
        }

        public NodeInfo getNodeInfo()
        {
            return nodeInfo;
        }

        public SecurityConfig getSecurityConfig()
        {
            return securityConfig;
        }
    }
}
