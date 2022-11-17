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
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BufferExchangeManagerFactory
        implements ExchangeManagerFactory
{
    private final Optional<ApiFactory> apiFactory;

    public static BufferExchangeManagerFactory forRealBufferService()
    {
        return new BufferExchangeManagerFactory(Optional.empty());
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory withApiFactory(ApiFactory apiFactory)
    {
        return new BufferExchangeManagerFactory(Optional.of(apiFactory));
    }

    private BufferExchangeManagerFactory(Optional<ApiFactory> apiFactory)
    {
        this.apiFactory = apiFactory;
    }

    @Override
    public String getName()
    {
        return "buffer";
    }

    @Override
    public ExchangeManager create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.starburst.stargate.buffer.trino.exchange", "io.starburst.buffer.exchange"),
                new BufferExchangeModule(apiFactory));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(BufferExchangeManager.class);
    }
}
