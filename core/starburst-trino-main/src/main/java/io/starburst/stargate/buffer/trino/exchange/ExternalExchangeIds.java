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

import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeId;

import static com.google.common.base.Preconditions.checkArgument;

public class ExternalExchangeIds
{
    private ExternalExchangeIds() {}

    public static String externalExchangeId(QueryId queryId, ExchangeId exchangeId)
    {
        return queryId.toString() + "-" + exchangeId.getId();
    }

    public static ExchangeId internalExchangeId(String externalExchangeId)
    {
        int dashIndex = externalExchangeId.indexOf('-');
        checkArgument(dashIndex != -1, "Expected '-' in external exchange id: %s", externalExchangeId);
        return new ExchangeId(externalExchangeId.substring(dashIndex + 1));
    }
}
