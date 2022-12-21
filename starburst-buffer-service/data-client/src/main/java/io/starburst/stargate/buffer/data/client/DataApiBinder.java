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
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientBinder;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class DataApiBinder
{
    private final Binder binder;
    private final HttpClientBinder httpClientBinder;

    public static DataApiBinder dataApiBinder(Binder binder)
    {
        return new DataApiBinder(requireNonNull(binder, "binder is null"));
    }

    private DataApiBinder(Binder binder)
    {
        this.binder = binder;
        this.httpClientBinder = HttpClientBinder.httpClientBinder(binder);
    }

    @CanIgnoreReturnValue
    public DataApiBinder bindHttpDataApi(String dataApiName)
    {
        bindCommon(dataApiName);
        binder.bind(DataApiFactory.class)
                .to(HttpDataApiFactory.class)
                .in(Scopes.SINGLETON);
        return this;
    }

    @CanIgnoreReturnValue
    public DataApiBinder bindRetryingHttpDataApi(String dataApiName)
    {
        bindCommon(dataApiName);
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

    private void bindCommon(String dataApiName)
    {
        httpClientBinder.bindHttpClient(dataApiName, ForBufferDataClient.class)
                .withConfigDefaults(config -> config.setMaxContentLength(DataSize.of(32, MEGABYTE)));
        configBinder(binder).bindConfig(DataApiConfig.class, dataApiName);
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
