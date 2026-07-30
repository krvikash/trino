/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static java.lang.String.format;

public class StarburstSynapseQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public StarburstSynapseQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String toPredicate(
            JdbcClient client,
            ConnectorSession session,
            JdbcColumnHandle column,
            JdbcTypeHandle jdbcType,
            Type type,
            WriteFunction writeFunction,
            String operator,
            Object value,
            Consumer<QueryParameter> accumulator)
    {
        if (operator.equals("=") && requiresDatalengthGuard(jdbcType)) {
            // Synapse PAD SPACE: 'abc' = 'abc ' remotely. DATALENGTH(col) = DATALENGTH(?) rejects
            // trailing-space false positives without needing a Trino re-check.
            String columnName = client.quoted(column.getColumnName());
            String bind = writeFunction.getBindExpression();
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("(%s = %s AND DATALENGTH(%s) = DATALENGTH(%s))", columnName, bind, columnName, bind);
        }
        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    @Override
    protected String toInPredicate(
            JdbcClient client,
            ConnectorSession session,
            JdbcColumnHandle column,
            JdbcTypeHandle jdbcType,
            Type type,
            List<Object> singleValues,
            WriteFunction writeFunction,
            Consumer<QueryParameter> accumulator)
    {
        if (!requiresDatalengthGuard(jdbcType)) {
            return super.toInPredicate(client, session, column, jdbcType, type, singleValues, writeFunction, accumulator);
        }
        // Expand IN to OR-joined equalities, each with a DATALENGTH guard.
        String columnName = client.quoted(column.getColumnName());
        String bind = writeFunction.getBindExpression();
        List<String> equalities = new ArrayList<>(singleValues.size());
        for (Object value : singleValues) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            equalities.add(format("(%s = %s AND DATALENGTH(%s) = DATALENGTH(%s))", columnName, bind, columnName, bind));
        }
        return "(" + String.join(" OR ", equalities) + ")";
    }

    private static boolean requiresDatalengthGuard(JdbcTypeHandle jdbcType)
    {
        // Guard only NVARCHAR: both column and JDBC parameter use 2 bytes/char (Unicode), so
        // DATALENGTH(col) = DATALENGTH(?) is reliable. VARCHAR excluded: the driver sends parameters
        // as Unicode (2 bytes/char) but VARCHAR stores Latin-1 (1 byte/char).
        CaseSensitivity cs = jdbcType.caseSensitivity().orElse(CASE_INSENSITIVE);
        return cs == CASE_SENSITIVE && jdbcType.jdbcType() == Types.NVARCHAR;
    }
}
