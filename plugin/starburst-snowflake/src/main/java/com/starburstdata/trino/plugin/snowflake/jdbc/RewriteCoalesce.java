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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.COALESCE_FUNCTION_NAME;

public class RewriteCoalesce
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<List<ConnectorExpression>> OPERANDS = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(COALESCE_FUNCTION_NAME))
            .with(argumentCount().matching(count -> count >= 2))
            .with(arguments().capturedAs(OPERANDS));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!SnowflakeSessionProperties.getExperimentalPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }

        List<ConnectorExpression> operands = captures.get(OPERANDS);
        verify(operands.size() >= 2, "COALESCE requires at least two operands");

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        ImmutableList.Builder<String> rewrittenOperands = ImmutableList.builderWithExpectedSize(operands.size());
        for (ConnectorExpression operand : operands) {
            Optional<ParameterizedExpression> rewritten = context.defaultRewrite(operand);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }

            rewrittenOperands.add(rewritten.get().expression());
            parameters.addAll(rewritten.get().parameters());
        }

        return Optional.of(new ParameterizedExpression(
                "COALESCE(%s)".formatted(String.join(", ", rewrittenOperands.build())),
                parameters.build()));
    }
}
