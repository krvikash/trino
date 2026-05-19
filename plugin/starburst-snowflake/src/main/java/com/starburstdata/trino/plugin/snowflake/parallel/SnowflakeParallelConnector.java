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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.Optional;
import java.util.Set;

public class SnowflakeParallelConnector
        extends JdbcConnector
{
    private final ConnectorPageSourceProvider connectorPageSourceProvider;

    @Inject
    public SnowflakeParallelConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            JdbcTransactionManager transactionManager,
            ConnectorPageSourceProvider jdbcPageSourceProvider,
            StarburstResultStreamProvider streamProvider,
            JdbcClient jdbcClient)
    {
        super(lifeCycleManager,
                jdbcSplitManager,
                jdbcPageSourceProvider,
                jdbcPageSinkProvider,
                accessControl,
                procedures,
                connectorTableFunctions,
                sessionProperties,
                tableProperties,
                transactionManager);
        this.connectorPageSourceProvider = new SnowflakePageSourceProvider(jdbcPageSourceProvider, streamProvider, jdbcClient);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return connectorPageSourceProvider;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }
}
