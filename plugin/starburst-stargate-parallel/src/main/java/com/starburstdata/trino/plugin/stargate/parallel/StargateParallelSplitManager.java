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

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.stargate.parallel.LiteralFormatter.formatLiteral;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StargateParallelSplitManager
        implements ConnectorSplitManager
{
    private final StargateClientFactory clientFactory;
    private final JdbcClient stargateClient;
    private final RemoteQueryModifier queryModifier;
    private final JdbcSplitManager jdbcSplitManager;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(StargateParallelSplitManager.class.getName() + "-%d"));

    @Inject
    public StargateParallelSplitManager(
            StargateClientFactory clientFactory,
            JdbcClient stargateClient,
            RemoteQueryModifier queryModifier,
            JdbcSplitManager jdbcSplitManager)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.stargateClient = requireNonNull(stargateClient, "stargateClient is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }

        // Synthetic handles represent operations that haven't been pushed down (sort, aggregations etc.)
        // In Stargate Parallel the only thing "parallel" is the data transfer - each split doesn't result in its own table scan
        // which makes it safe to generate multiple splits even for synthetic handles because exactly the same data is returned in the parallel and no-parallel paths.
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;
        List<JdbcColumnHandle> columns = jdbcTableHandle.getColumns()
                .orElseGet(() -> stargateClient.getColumns(session, jdbcTableHandle));

        PreparedQuery preparedQuery = stargateClient.prepareQuery(
                session,
                dynamicFilteringEnabled(session) ? jdbcTableHandle.intersectedWithConstraint(dynamicFilter.getCurrentPredicate()) : jdbcTableHandle,
                Optional.empty(),
                columns,
                Map.of());

        String query = getExecuteStatement(session, preparedQuery);
        return new StargateParallelSplitSource(executor, clientFactory.createFactory(query));
    }

    private String getExecuteStatement(ConnectorSession session, PreparedQuery preparedQuery)
    {
        List<QueryParameter> parameters = preparedQuery.parameters();
        String finalQuery = queryModifier.apply(session, preparedQuery.query());

        if (parameters.isEmpty()) {
            return finalQuery;
        }

        List<String> binds = parameters.stream()
                .map(parameter -> bindParameter(session, parameter.getType(), parameter.getValue().orElseThrow()))
                .collect(toImmutableList());

        return """
                EXECUTE IMMEDIATE '%s' USING %s
                """.formatted(finalQuery.replace("'", "''"), Joiner.on(",").join(binds));
    }

    private String bindParameter(ConnectorSession session, Type type, Object value)
    {
        return stargateClient.toWriteMapping(session, type)
                .getWriteFunction()
                .getBindExpression()
                .replace("?", formatLiteral(type, value));
    }
}
