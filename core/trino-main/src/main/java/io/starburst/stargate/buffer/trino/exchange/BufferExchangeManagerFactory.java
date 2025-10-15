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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
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

    public static BufferExchangeManagerFactory forRealBufferService()
    {
        return new BufferExchangeManagerFactory("buffer", Optional.empty());
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory withApiFactory(String name, ApiFactory apiFactory)
    {
        return new BufferExchangeManagerFactory(name, Optional.of(apiFactory));
    }

    private BufferExchangeManagerFactory(String name, Optional<ApiFactory> apiFactory)
    {
        this.name = requireNonNull(name, "name is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
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
                new BufferExchangeModule(apiFactory),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(exchangeManagerContext.getOpenTelemetry());
                    binder.bind(CoordinatorLocator.class).toInstance(exchangeManagerContext.getCoordinatorLocator());
                    binder.bind(Tracer.class).toInstance(exchangeManagerContext.getTracer());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(BufferExchangeManager.class);
    }
}
