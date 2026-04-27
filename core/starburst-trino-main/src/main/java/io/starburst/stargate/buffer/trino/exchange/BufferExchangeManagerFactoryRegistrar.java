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

import com.google.inject.Inject;
import io.trino.exchange.ExchangeManagerRegistry;
import jakarta.annotation.PostConstruct;

import static java.util.Objects.requireNonNull;

public class BufferExchangeManagerFactoryRegistrar
{
    private final BufferExchangeManagerFactory bufferExchangeManagerFactory;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    @Inject
    public BufferExchangeManagerFactoryRegistrar(BufferExchangeManagerFactory bufferExchangeManagerFactory, ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this.bufferExchangeManagerFactory = requireNonNull(bufferExchangeManagerFactory, "bufferExchangeManagerFactory is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
    }

    @PostConstruct
    public void register()
    {
        exchangeManagerRegistry.addExchangeManagerFactory(bufferExchangeManagerFactory);
    }
}
