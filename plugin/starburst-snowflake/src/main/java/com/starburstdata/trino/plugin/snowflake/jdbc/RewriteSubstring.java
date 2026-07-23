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
import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionUnqualifiedName;
import static io.trino.spi.type.BigintType.BIGINT;

public class RewriteSubstring
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Pattern<Call> PATTERN = call()
            .with(functionUnqualifiedName().equalTo("substring"))
            .with(argumentCount().matching(count -> count == 2 || count == 3));

    @Override
    public boolean isEnabled(ConnectorSession session)
    {
        return SnowflakeSessionProperties.getExperimentalPushdownEnabled(session);
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        List<ConnectorExpression> arguments = call.getArguments();

        ConnectorExpression source = arguments.getFirst();
        if (!(source.getType() instanceof VarcharType)) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> rewrittenSource = context.defaultRewrite(source);
        if (rewrittenSource.isEmpty()) {
            return Optional.empty();
        }

        // Do not pushdown if start <= 0, as Snowflake behaves differently in that case
        ConnectorExpression start = arguments.get(1);
        if (asBigintConstant(start).map(bigint -> bigint <= 0).orElse(true)) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> rewrittenStart = context.defaultRewrite(start);
        if (rewrittenStart.isEmpty()) {
            return Optional.empty();
        }

        // Do not pushdown if length < 0, as Snowflake behaves differently in that case
        Optional<ParameterizedExpression> rewrittenLength = Optional.empty();
        if (arguments.size() == 3) {
            ConnectorExpression length = arguments.get(2);
            if (asBigintConstant(length).map(bigint -> bigint < 0).orElse(true)) {
                return Optional.empty();
            }
            rewrittenLength = context.defaultRewrite(length);
            if (rewrittenLength.isEmpty()) {
                return Optional.empty();
            }
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.<QueryParameter>builder()
                .addAll(rewrittenSource.get().parameters())
                .addAll(rewrittenStart.get().parameters());
        rewrittenLength.ifPresent(length -> parameters.addAll(length.parameters()));
        String expression = rewrittenLength.map(length -> "SUBSTR(%s, %s, %s)".formatted(
                        rewrittenSource.get().expression(),
                        rewrittenStart.get().expression(),
                        length.expression()))
                .orElseGet(() -> "SUBSTR(%s, %s)".formatted(
                        rewrittenSource.get().expression(),
                        rewrittenStart.get().expression()));
        return Optional.of(new ParameterizedExpression(expression, parameters.build()));
    }

    private static Optional<Long> asBigintConstant(ConnectorExpression expression)
    {
        if (!(expression instanceof Constant constant && BIGINT.equals(constant.getType()))) {
            return Optional.empty();
        }

        Object value = constant.getValue();
        return value instanceof Long longValue ? Optional.of(longValue) : Optional.empty();
    }
}
