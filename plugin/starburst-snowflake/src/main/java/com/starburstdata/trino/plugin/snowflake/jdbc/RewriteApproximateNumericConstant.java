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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Constant;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;

public class RewriteApproximateNumericConstant
        implements ConnectorExpressionRule<Constant, ParameterizedExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(type -> type == DOUBLE || type == REAL));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Constant constant, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Object value = constant.getValue();
        return value == null
                ? Optional.empty()
                : Optional.of(new ParameterizedExpression("?", ImmutableList.of(new QueryParameter(constant.getType(), Optional.of(value)))));
    }
}
