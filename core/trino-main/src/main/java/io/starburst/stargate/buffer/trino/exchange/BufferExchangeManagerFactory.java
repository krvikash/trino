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
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.StartupStatus;
import io.trino.server.buffer.EmbeddedBufferServiceConfig;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.CoordinatorLocator;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerContext;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.starburst.stargate.buffer.trino.exchange.BufferExchangeConfig.USE_EMBEDDED_BUFFER_SERVICE_CONFIG_PROPERTY;
import static java.util.Objects.requireNonNull;

public class BufferExchangeManagerFactory
        implements ExchangeManagerFactory
{
    private final String name;
    private final Optional<ApiFactory> apiFactory;
    private final Optional<InternalCommunicationDependencies> internalCommunicationDependencies;
    private final Optional<EmbeddedBufferServiceConfigs> embeddedDataServerConfigs;

    public static class RealBufferExchangeManagerFactoryModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(BufferExchangeManagerFactory.InternalCommunicationDependencies.class).in(Scopes.SINGLETON);
            binder.bind(BufferExchangeManagerFactoryRegistrar.class).in(Scopes.SINGLETON);

            newOptionalBinder(binder, SpoolingDirectoryConfig.class);
            newOptionalBinder(binder, ChunkManagerConfig.class);
            newOptionalBinder(binder, S3ClientConfig.class);
            newOptionalBinder(binder, AzureBlobClientConfig.class);
        }

        @Provides
        @Singleton
        BufferExchangeManagerFactory getBufferExchangeManagerFactory(
                InternalCommunicationDependencies internalCommunicationDependencies,
                Optional<EmbeddedBufferServiceConfigs> embeddedDataServerConfigs)
        {
            return new BufferExchangeManagerFactory("buffer", Optional.empty(), Optional.of(internalCommunicationDependencies), embeddedDataServerConfigs);
        }

        @Provides
        @Singleton
        public Optional<EmbeddedBufferServiceConfigs> getEmbeddedBufferServiceConfigs(
                EmbeddedBufferServiceConfig embeddedBufferServiceConfig,
                Optional<SpoolingDirectoryConfig> spoolingDirectoryConfig,
                Optional<S3ClientConfig> s3ClientConfig,
                Optional<AzureBlobClientConfig> azureBlobClientConfig)
        {
            if (embeddedBufferServiceConfig.isEmbeddedBufferServiceEnabled()) {
                verify(spoolingDirectoryConfig.isPresent(), "SpoolingDirectoryConfig must be bound on worker node if embeddedBufferServiceConfig is enabled");
                return Optional.of(new EmbeddedBufferServiceConfigs(spoolingDirectoryConfig.orElseThrow(), s3ClientConfig, azureBlobClientConfig));
            }
            return Optional.empty();
        }
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory forRealBufferService()
    {
        return new BufferExchangeManagerFactory("buffer", Optional.empty(), Optional.empty(), Optional.empty());
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory withApiFactory(String name, ApiFactory apiFactory)
    {
        return new BufferExchangeManagerFactory(name, Optional.of(apiFactory), Optional.empty(), Optional.empty());
    }

    private BufferExchangeManagerFactory(
            String name,
            Optional<ApiFactory> apiFactory,
            Optional<InternalCommunicationDependencies> internalCommunicationDependencies,
            Optional<EmbeddedBufferServiceConfigs> embeddedDataServerConfigs)
    {
        this.name = requireNonNull(name, "name is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.internalCommunicationDependencies = requireNonNull(internalCommunicationDependencies, "internalCommunicationDependencies is null");
        this.embeddedDataServerConfigs = requireNonNull(embeddedDataServerConfigs, "embeddedDataServerConfigs is null");
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

        // directly check config map so we are not binding unnecessary stuff in case we are not in embedded mode
        boolean useEmbeddedBufferService = Boolean.parseBoolean(config.getOrDefault(USE_EMBEDDED_BUFFER_SERVICE_CONFIG_PROPERTY, "false"));

        Bootstrap app = new Bootstrap(
                "buffer-exchange-manager",
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.starburst.stargate.buffer.trino.exchange", "io.starburst.buffer.exchange"),
                new JsonModule(),
                new BufferExchangeModule(apiFactory, useEmbeddedBufferService ? internalCommunicationDependencies : Optional.empty()),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(exchangeManagerContext.getOpenTelemetry());
                    binder.bind(CoordinatorLocator.class).toInstance(exchangeManagerContext.getCoordinatorLocator());
                    binder.bind(Tracer.class).toInstance(exchangeManagerContext.getTracer());
                    if (useEmbeddedBufferService) {
                        StartupStatus startupStatus = new StartupStatus();
                        startupStatus.startupComplete();
                        binder.bind(StartupStatus.class).toInstance(startupStatus);
                    }
                });

        ImmutableMap.Builder<String, String> extendedConfig = ImmutableMap.builder();
        extendedConfig.putAll(config);
        if (useEmbeddedBufferService) {
            verify(internalCommunicationDependencies.isPresent(), "internalCommunicationDependencies must not be empty if embedded buffer service is in use");
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
        }

        embeddedDataServerConfigs.ifPresent(configs -> {
            URI spoolingDirectory = configs.spoolingDirectoryConfig().getSpoolingDirectory();
            String scheme = spoolingDirectory.getScheme();
            SpoolingStorageType spoolingStorageType = switch (scheme) {
                case null -> SpoolingStorageType.LOCAL;
                case "file" -> SpoolingStorageType.LOCAL;
                case "gs" -> SpoolingStorageType.GCS;
                case "s3" -> SpoolingStorageType.S3;
                case "abfs" -> SpoolingStorageType.AZURE;
                default -> throw new IllegalArgumentException("Cannot determine spooling storage type of embedded buffer service: " + scheme);
            };

            extendedConfig.put("exchange.buffer-data.spooling-storage-type", spoolingStorageType.name());

            if (spoolingStorageType == SpoolingStorageType.S3 || spoolingStorageType == SpoolingStorageType.GCS) {
                S3ClientConfig s3ClientConfig = configs.s3ClientConfig()
                        .orElseThrow(() -> new IllegalArgumentException("S3ClientConfig not set for embedded buffer service with storage type: " + spoolingStorageType));
                if (s3ClientConfig.getS3AwsAccessKey() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.s3.aws-access-key", s3ClientConfig.getS3AwsAccessKey());
                }
                if (s3ClientConfig.getS3AwsSecretKey() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.s3.aws-secret-key", s3ClientConfig.getS3AwsSecretKey());
                }
                if (s3ClientConfig.getRegion().isPresent()) {
                    extendedConfig.put("exchange.buffer-data.spooling.s3.region", s3ClientConfig.getRegion().orElseThrow().id());
                }
                if (s3ClientConfig.getS3Endpoint().isPresent()) {
                    extendedConfig.put("exchange.buffer-data.spooling.s3.endpoint", s3ClientConfig.getS3Endpoint().orElseThrow());
                }
                extendedConfig.put("exchange.buffer-data.spooling.s3.retry-mode", s3ClientConfig.getRetryMode().name());
                extendedConfig.put("exchange.buffer-data.spooling.s3.max-error-retries", String.valueOf(s3ClientConfig.getMaxErrorRetries()));
            }

            if (spoolingStorageType == SpoolingStorageType.AZURE) {
                AzureBlobClientConfig azureBlobClientConfig = configs.azureBlobClientConfig()
                        .orElseThrow(() -> new IllegalArgumentException("AzureBlobClientConfig not set for embedded buffer service with storage type: " + spoolingStorageType));

                extendedConfig.put("exchange.buffer-data.spooling.azure.connection-string", azureBlobClientConfig.getConnectionString());

                if (azureBlobClientConfig.getRetryPolicyType() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.azure.retry-policy", azureBlobClientConfig.getRetryPolicyType().name());
                }
                if (azureBlobClientConfig.getMaxTries() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.azure.max-tries", String.valueOf(azureBlobClientConfig.getMaxTries()));
                }
                extendedConfig.put("exchange.buffer-data.spooling.azure.try-timeout", azureBlobClientConfig.getTryTimeout().toString());
                if (azureBlobClientConfig.getRetryDelay() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.azure.retry-delay", azureBlobClientConfig.getRetryDelay().toString());
                }
                if (azureBlobClientConfig.getMaxRetryDelay() != null) {
                    extendedConfig.put("exchange.buffer-data.spooling.azure.max-retry-delay", azureBlobClientConfig.getMaxRetryDelay().toString());
                }
            }
        });

        // explicit properties take precedence
        extendedConfig.putAll(config);

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(extendedConfig.buildKeepingLast())
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

    public record EmbeddedBufferServiceConfigs(SpoolingDirectoryConfig spoolingDirectoryConfig,
                                               Optional<S3ClientConfig> s3ClientConfig,
                                               Optional<AzureBlobClientConfig> azureBlobClientConfig)
    {
        public EmbeddedBufferServiceConfigs
        {
            requireNonNull(spoolingDirectoryConfig, "spoolingDirectoryConfig is null");
            requireNonNull(s3ClientConfig, "s3ClientConfig is null");
            requireNonNull(azureBlobClientConfig, "azureBlobClientConfig is null");
        }
    }
}
