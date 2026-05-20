/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;

/*
 * The OEM key requires a com.starburstdata.* class to be on the stack trace when making calls to Salesforce.
 * This class extends JdbcPageSinkProvider but forwards all calls to the base to make sure it is on the stack for testing the connector.
 */
public class SalesforceJdbcPageSinkProvider
        extends JdbcPageSinkProvider
{
    @Inject
    public SalesforceJdbcPageSinkProvider(JdbcClient jdbcClient, RemoteQueryModifier queryModifier, QueryBuilder queryBuilder)
    {
        super(jdbcClient, queryModifier, queryBuilder);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return super.createPageSink(transactionHandle, session, tableHandle, tableCredentials, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return super.createPageSink(transactionHandle, session, tableHandle, tableCredentials, pageSinkId);
    }
}
