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
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.noop.NoopSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.S3SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.SpoolingS3ReaderConfig;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
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

    @CanIgnoreReturnValue
    public DataApiBinder bindRetryingHttpDataApi(String dataApiName)
    {
        bindCommon(dataApiName, moduleInstall);
        binder.bind(DataApiFactory.class)
                .annotatedWith(ForRetryingDataApiFactory.class)
                .to(HttpDataApiFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(ScheduledExecutorService.class)
                .annotatedWith(ForRetryingDataApiFactory.class)
                .toInstance(new ScheduledThreadPoolExecutor(4, daemonThreadsNamed("retrying-data-api-%s")));
        binder.bind(DataApiFactory.class)
                .to(RetryingDataApiFactory.class)
                .in(Scopes.SINGLETON);
        return this;
    }

    private void bindCommon(String dataApiName, Consumer<Module> moduleInstall)
    {
        httpClientBinder.bindHttpClient(dataApiName, ForBufferDataClient.class)
                .withConfigDefaults(config -> config
                        .setMaxContentLength(DataSize.of(32, MEGABYTE))
                        .setIdleTimeout(succinctDuration(30, SECONDS)));
        configBinder(binder).bindConfig(DataApiConfig.class, dataApiName);

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == SpoolingStorageType.NONE,
                binder -> binder.bind(SpooledChunkReader.class).to(NoopSpooledChunkReader.class).in(Scopes.SINGLETON)));

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == SpoolingStorageType.LOCAL,
                binder -> binder.bind(SpooledChunkReader.class).to(LocalSpooledChunkReader.class).in(Scopes.SINGLETON)));

        moduleInstall.accept(ConditionalModule.conditionalModule(
                DataApiConfig.class,
                dataApiName,
                config -> config.getSpoolingStorageType() == SpoolingStorageType.S3,
                binder -> {
                    configBinder(binder).bindConfig(SpoolingS3ReaderConfig.class, dataApiName);
                    binder.bind(SpooledChunkReader.class).to(S3SpooledChunkReader.class).in(Scopes.SINGLETON);
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
