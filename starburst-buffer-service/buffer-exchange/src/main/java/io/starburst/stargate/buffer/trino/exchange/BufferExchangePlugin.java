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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.exchange.ExchangeManagerFactory;

public class BufferExchangePlugin
        implements Plugin
{
    @Override
    public Iterable<ExchangeManagerFactory> getExchangeManagerFactories()
    {
        return ImmutableList.of(BufferExchangeManagerFactory.forRealBufferService());
    }
}
