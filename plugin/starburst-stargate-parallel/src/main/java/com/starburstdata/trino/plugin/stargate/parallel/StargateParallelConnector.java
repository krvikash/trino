/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StargateParallelConnector
        extends JdbcConnector
{
    private final ConnectorRecordSetProvider connectorRecordSetProvider;

    @Inject
    public StargateParallelConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorRecordSetProvider jdbcRecordSetProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures, Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            JdbcTransactionManager transactionManager,
            ConnectorRecordSetProvider connectorRecordSetProvider)
    {
        super(lifeCycleManager, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider, accessControl, procedures, connectorTableFunctions, sessionProperties, tableProperties, transactionManager);
        this.connectorRecordSetProvider = requireNonNull(connectorRecordSetProvider, "connectorRecordSetProvider is null");
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return connectorRecordSetProvider;
    }
}
