/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SnowflakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(SnowflakePageSourceProvider.class);
    private final ConnectorPageSourceProvider jdbcPageSourceProvider;
    private final StarburstResultStreamProvider streamProvider;
    private final JdbcClient jdbcClient;

    public SnowflakePageSourceProvider(ConnectorPageSourceProvider jdbcPageSourceProvider, StarburstResultStreamProvider streamProvider, JdbcClient jdbcClient)
    {
        this.jdbcPageSourceProvider = requireNonNull(jdbcPageSourceProvider, "jdbcPageSourceProvider is null");
        this.streamProvider = requireNonNull(streamProvider, "streamProvider is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        log.debug("createPageSource(transaction=%s, session=%s, split=%s, table=%s, columns=%s)", transaction, session, split, table, columns);
        if (split instanceof SnowflakeArrowSplit snowflakeArrowSplit) {
            List<JdbcColumnHandle> jdbcColumnHandles = columns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .collect(toImmutableList());
            return new SnowflakeArrowPageSource(session, jdbcClient, (JdbcTableHandle) table, snowflakeArrowSplit, jdbcColumnHandles, streamProvider);
        }
        return jdbcPageSourceProvider.createPageSource(transaction, session, split, table, tableCredentials, columns, dynamicFilter);
    }
}
