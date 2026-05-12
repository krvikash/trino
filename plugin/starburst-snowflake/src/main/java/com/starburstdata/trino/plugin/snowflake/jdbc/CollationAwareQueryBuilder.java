/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.isCollatable;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class CollationAwareQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public CollationAwareQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        return format(
                "%s.%s %s %s %s.%s %s",
                leftRelationAlias,
                buildJoinColumn(client, condition.getLeftColumn()),
                isCollatable(condition.getLeftColumn()) ? "COLLATE 'utf8'" : "",
                condition.getOperator().getValue(),
                rightRelationAlias,
                buildJoinColumn(client, condition.getRightColumn()),
                isCollatable(condition.getRightColumn()) ? "COLLATE 'utf8'" : "");
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column)) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("%s COLLATE 'utf8' %s %s", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    @Override
    protected String toInPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, List<Object> singleValues, WriteFunction writeFunction, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column)) {
            for (Object value : singleValues) {
                accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), writeFunction.getBindExpression()));
            return client.quoted(column.getColumnName()) + " COLLATE 'utf8' IN (" + values + ")";
        }

        return super.toInPredicate(client, session, column, jdbcType, type, singleValues, writeFunction, accumulator);
    }
}
