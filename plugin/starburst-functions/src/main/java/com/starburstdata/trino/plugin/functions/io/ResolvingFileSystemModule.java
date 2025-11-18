/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.functions.io.StorageConfig;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureAuthAccessKeyConfig;
import io.trino.filesystem.azure.AzureAuthDefault;
import io.trino.filesystem.azure.AzureAuthManagedIdentityConfig;
import io.trino.filesystem.azure.AzureAuthOAuthConfig;
import io.trino.filesystem.azure.AzureAuthOauth;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.filesystem.gcs.GcsAccessTokenAuth;
import io.trino.filesystem.gcs.GcsAuth;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.functions.io.StorageErrorCode.STORAGE_CLIENT_ERROR;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public class ResolvingFileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final OpenTelemetry openTelemetry;

    public ResolvingFileSystemModule(OpenTelemetry openTelemetry)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        MapBinder<String, TrinoFileSystemFactory> factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);
        newOptionalBinder(binder, CachingHostAddressProvider.class).setDefault().to(DefaultCachingHostAddressProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, CacheKeyProvider.class).setDefault().to(DefaultCacheKeyProvider.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, TrinoFileSystemCache.class);
        newOptionalBinder(binder, MemoryFileSystemCache.class);

        StorageConfig storageConfig = buildConfigObject(StorageConfig.class);
        StorageConfigurations configurations = readStorageConfig(storageConfig.getCredentialsKey(), storageConfig.getCredentialsFile());
        for (StorageConfiguration config : configurations.configurations()) {
            factories.addBinding(config.location()).toInstance(readStorageConfigurations(config.location(), config));
        }
    }

    @Provides
    @Singleton
    static TrinoFileSystemFactory createFileSystemFactory(Map<String, TrinoFileSystemFactory> factories, Tracer tracer)
    {
        Function<Location, TrinoFileSystemFactory> loader = location -> {
            try {
                return factories.entrySet().stream()
                        .filter(entry -> location.toString().startsWith(entry.getKey()))
                        .collect(toOptional())
                        .map(Map.Entry::getValue)
                        .orElseThrow(() -> new IllegalArgumentException("No factory for location: " + location));
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(CONFIGURATION_INVALID, "Invalid configuration: " + firstNonNull(e.getMessage(), e), e);
            }
        };

        TrinoFileSystemFactory delegate = new SwitchingFileSystemFactory(loader);
        return new TracingFileSystemFactory(tracer, delegate);
    }

    private static StorageConfigurations readStorageConfig(Optional<String> credentialsKey, Optional<File> credentialsFile)
    {
        if (credentialsKey.isPresent()) {
            byte[] base64 = BaseEncoding.base64().decode(credentialsKey.orElseThrow());
            return parseJson(base64, StorageConfigurations.class);
        }
        if (credentialsFile.isPresent()) {
            return readStorageConfigurations(credentialsFile.orElseThrow());
        }
        return new StorageConfigurations(ImmutableList.of());
    }

    private static StorageConfigurations readStorageConfigurations(File path)
    {
        try {
            String json = Files.readString(path.toPath());
            return parseConfiguration(json);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static StorageConfigurations parseConfiguration(String json)
    {
        try {
            return parseJson(json, StorageConfigurations.class);
        }
        catch (RuntimeException e) {
            // the error message can contain sensitive information, so just include the location of the parsing error
            getCausalChain(e).stream()
                    .filter(JsonParseException.class::isInstance)
                    .map(JsonParseException.class::cast)
                    .findFirst()
                    .ifPresent(jpe -> {
                        throw new TrinoException(STORAGE_CLIENT_ERROR, "Error parsing storage configuration at: %s".formatted(jpe.getLocation().offsetDescription()));
                    });
            throw e;
        }
    }

    private TrinoFileSystemFactory readStorageConfigurations(String location, StorageConfiguration spec)
    {
        String scheme = URI.create(location).getScheme();
        checkState(scheme != null, "Location must have a scheme: %s".formatted(location));

        ConfigurationFactory configFactory = new ConfigurationFactory(spec.configuration());
        return switch (scheme) {
            case "s3", "s3a", "s3n" -> {
                S3FileSystemConfig config = configFactory.build(S3FileSystemConfig.class);
                yield new S3FileSystemFactory(openTelemetry, config, new S3FileSystemStats());
            }
            case "gs" -> {
                GcsFileSystemConfig config = configFactory.build(GcsFileSystemConfig.class);
                try {
                    GcsAuth gcsAuth = switch (config.getAuthType()) {
                        case ACCESS_TOKEN -> new GcsAccessTokenAuth();
                        case SERVICE_ACCOUNT -> {
                            GcsServiceAccountAuthConfig authConfig = configFactory.build(GcsServiceAccountAuthConfig.class);
                            yield new GcsServiceAccountAuth(authConfig);
                        }
                    };
                    GcsStorageFactory storageFactory = new GcsStorageFactory(config, gcsAuth);
                    yield new GcsFileSystemFactory(config, storageFactory);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            case "abfs", "abfss", "wasb", "wasbs" -> {
                AzureFileSystemConfig config = configFactory.build(AzureFileSystemConfig.class);
                AzureAuth azureAuth = switch (config.getAuthType()) {
                    case ACCESS_KEY -> new AzureAuthAccessKey(configFactory.build(AzureAuthAccessKeyConfig.class));
                    case OAUTH -> new AzureAuthOauth(configFactory.build(AzureAuthOAuthConfig.class));
                    case DEFAULT -> new AzureAuthDefault(configFactory.build(AzureAuthManagedIdentityConfig.class));
                };
                yield new AzureFileSystemFactory(openTelemetry, azureAuth, config);
            }
            default -> throw new IllegalArgumentException("Unsupported file system: " + location);
        };
    }
}
