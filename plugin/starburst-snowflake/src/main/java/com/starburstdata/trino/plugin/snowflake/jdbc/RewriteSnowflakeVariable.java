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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Variable;

import java.util.Optional;
import java.util.function.Function;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.isCollatable;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static java.util.Objects.requireNonNull;

/**
 * A specialized version of RewriteVariable that resets collation on collatable columns.
 */
public class RewriteSnowflakeVariable
        implements ConnectorExpressionRule<Variable, ParameterizedExpression>
{
    private final Function<String, String> identifierQuote;

    public RewriteSnowflakeVariable(Function<String, String> identifierQuote)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    @Override
    public Pattern<Variable> getPattern()
    {
        return variable();
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Variable variable, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(variable.getName());
        String quoted = identifierQuote.apply(columnHandle.getColumnName());
        String expression = quoted;
        if (isCollatable(columnHandle)) {
            expression = "%s COLLATE 'utf8'".formatted(quoted);
        }
        return Optional.of(new ParameterizedExpression(expression, ImmutableList.of()));
    }
}
