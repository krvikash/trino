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
import com.google.common.collect.ImmutableSet;
import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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

public class RewriteTrim
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final String TRIM_FUNCTION_NAME = "trim";
    private static final String LTRIM_FUNCTION_NAME = "ltrim";
    private static final String RTRIM_FUNCTION_NAME = "rtrim";

    // Base name of io.trino.type.CodePointsType, which isn't included in the SPI.
    private static final String CODE_POINTS_BASE_NAME = "CodePoints";

    private static final Slice TRINO_WHITESPACE_CHARS;

    static {
        // Mirror io.airlift.slice.SliceUtf8's whitespace set, which is defined by Character.isWhitespace.
        // Snowflake's TRIM/LTRIM/RTRIM defaults differ, so this set is emitted explicitly.
        StringBuilder builder = new StringBuilder();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isWhitespace(codePoint)) {
                builder.appendCodePoint(codePoint);
            }
        }
        TRINO_WHITESPACE_CHARS = Slices.utf8Slice(builder.toString());
    }

    private static final Pattern<Call> PATTERN = call()
            .with(functionUnqualifiedName().matching(ImmutableSet.of(TRIM_FUNCTION_NAME, LTRIM_FUNCTION_NAME, RTRIM_FUNCTION_NAME)::contains))
            .with(argumentCount().matching(count -> count == 1 || count == 2));

    private final boolean collationCorrectionEnabled;

    public RewriteTrim(boolean collationCorrectionEnabled)
    {
        this.collationCorrectionEnabled = collationCorrectionEnabled;
    }

    @Override
    public boolean isEnabled(ConnectorSession session)
    {
        return collationCorrectionEnabled && SnowflakeSessionProperties.getExperimentalPushdownEnabled(session);
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ConnectorExpression sourceExpression = call.getArguments().getFirst();
        if (!(sourceExpression.getType() instanceof VarcharType)) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> source = context.defaultRewrite(sourceExpression);
        if (source.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> trimmedChars = call.getArguments().size() == 1
                ? Optional.of(trinoWhitespaceParameter())
                : rewriteCharsArgument(call.getArguments().get(1));
        if (trimmedChars.isEmpty()) {
            return Optional.empty();
        }

        String rewritten = "%s(%s, %s)".formatted(call.getFunctionName().getName(), source.get().expression(), trimmedChars.get().expression());
        List<QueryParameter> parameters = ImmutableList.<QueryParameter>builder()
                .addAll(source.get().parameters())
                .addAll(trimmedChars.get().parameters())
                .build();
        return Optional.of(new ParameterizedExpression(rewritten, parameters));
    }

    private static ParameterizedExpression trinoWhitespaceParameter()
    {
        return new ParameterizedExpression(
                "?",
                ImmutableList.of(new QueryParameter(
                        VarcharType.createVarcharType(TRINO_WHITESPACE_CHARS.length()),
                        Optional.of(TRINO_WHITESPACE_CHARS))));
    }

    private static Optional<ParameterizedExpression> rewriteCharsArgument(ConnectorExpression argument)
    {
        if (!CODE_POINTS_BASE_NAME.equals(argument.getType().getBaseName())
                || !(argument instanceof Constant constant)
                || !(constant.getValue() instanceof int[] codePoints)) {
            return Optional.empty();
        }
        if (codePoints.length == 0) {
            return Optional.empty();
        }

        // Do not push down with an empty char set, as Snowflake will reject it
        VarcharType varcharType = VarcharType.createVarcharType(codePoints.length);
        return Optional.of(new ParameterizedExpression(
                "?",
                ImmutableList.of(new QueryParameter(
                        varcharType,
                        Optional.of(Slices.utf8Slice(new String(codePoints, 0, codePoints.length)))))));
    }
}
