/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConditionalModule;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientBinder;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.azure.AzureBlobSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.noop.NoopSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.S3SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobClientConfig;
import io.starburst.stargate.buffer.data.spooling.azure.BlobServiceAsyncClientProvider;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.function.Consumer;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.AZURE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.LOCAL;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.NONE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.S3;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DataApiBinder
{
    private final Binder binder;
    private final Consumer<Module> moduleInstall;
    private final HttpClientBinder httpClientBinder;

    public static DataApiBinder dataApiBinder(Binder binder, Consumer<Module> moduleInstall)
    {
        return new DataApiBinder(requireNonNull(binder, "binder is null"), requireNonNull(moduleInstall, "moduleInstall is null"));
    }

    private DataApiBinder(Binder binder, Consumer<Module> moduleInstall)
    {
        this.binder = binder;
        this.moduleInstall = moduleInstall;
        this.httpClientBinder = HttpClientBinder.httpClientBinder(binder);
    }

    @CanIgnoreReturnValue
    public DataApiBinder bindHttpDataApi(String dataApiName)
    {
        bindCommon(dataApiName, moduleInstall);
        binder.bind(DataApiFactory.class)
                .to(HttpDataApiFactory.class)
                .in(Scopes.SINGLETON);
        return this;
    }

    private void bindCommon(String dataApiName, Consumer<Module> moduleInstall)
    {
        httpClientBinder.bindHttpClient(dataApiName, ForBufferDataClient.class)
                .withConfigDefaults(config -> config
                        .setMaxContentLength(DataSize.of(64, MEGABYTE)) // should equal to chunk.max-size
                        .setIdleTimeout(succinctDuration(30, SECONDS)));
        configBinder(binder).bindConfig(DataApiConfig.class, dataApiName);

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == NONE,
                binder -> binder.bind(SpooledChunkReader.class).to(NoopSpooledChunkReader.class).in(Scopes.SINGLETON)));

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == LOCAL,
                binder -> binder.bind(SpooledChunkReader.class).to(LocalSpooledChunkReader.class).in(Scopes.SINGLETON)));

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == S3,
                binder -> {
                    configBinder(binder).bindConfig(S3ClientConfig.class, dataApiName);
                    binder.bind(S3AsyncClient.class).toProvider(S3ClientProvider.class).in(Scopes.SINGLETON);
                    binder.bind(SpooledChunkReader.class).to(S3SpooledChunkReader.class).in(Scopes.SINGLETON);
                }));

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == AZURE,
                binder -> {
                    configBinder(binder).bindConfig(AzureBlobClientConfig.class, dataApiName);
                    binder.bind(BlobServiceAsyncClient.class).toProvider(BlobServiceAsyncClientProvider.class).in(Scopes.SINGLETON);
                    binder.bind(SpooledChunkReader.class).to(AzureBlobSpooledChunkReader.class).in(Scopes.SINGLETON);
                }));
    }

    @CanIgnoreReturnValue
    public DataApiBinder withRetryingDataApiConfigDefaults(ConfigDefaults<DataApiConfig> configDefaults)
    {
        configBinder(binder).bindConfigDefaults(DataApiConfig.class, configDefaults);
        return this;
    }

    @CanIgnoreReturnValue
    public DataApiBinder withHttpClientConfigDefaults(ConfigDefaults<HttpClientConfig> configDefaults)
    {
        configBinder(binder).bindConfigDefaults(HttpClientConfig.class, ForBufferDataClient.class, configDefaults);
        return this;
    }
}
