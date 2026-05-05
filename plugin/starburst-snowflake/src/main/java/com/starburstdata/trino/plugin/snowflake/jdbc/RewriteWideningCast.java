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

import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;

/**
 * Strips engine-inserted widening casts that are no-ops for predicate evaluation against Snowflake.
 *
 * <p>Trino inserts implicit casts during type unification (for example, in {@code COALESCE},
 * {@code IN}, or comparisons against literals). When the cast widens within a single type family
 * the value round-trips losslessly, and stripping the cast lets the surrounding expression push
 * down.
 *
 * <p>Covered widening cases:
 * <ul>
 *   <li>{@code varchar(N) -> varchar(M)} where {@code M >= N}, or target unbounded</li>
 *   <li>integer family widening ({@code tinyint -> smallint -> integer -> bigint})</li>
 *   <li>integer family -> {@code decimal(p, 0)} where {@code p} fits the source's max digit count</li>
 *   <li>{@code decimal(p, 0)} -> int family where {@code p} is less than the target's max digit count</li>
 *   <li>{@code real -> double}</li>
 *   <li>{@code decimal(p1, s) -> decimal(p2, s)} where {@code p2 >= p1} (same scale)</li>
 *   <li>{@code timestamp(p1) -> timestamp(p2)} where {@code p2 >= p1}</li>
 *   <li>{@code timestamp(p1) with time zone -> timestamp(p2) with time zone} where {@code p2 >= p1}</li>
 * </ul>
 */
public class RewriteWideningCast
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final int TINYINT_MAX_PRECISION = String.valueOf(Byte.MAX_VALUE).length();
    private static final int SMALLINT_MAX_PRECISION = String.valueOf(Short.MAX_VALUE).length();
    private static final int INT_MAX_PRECISION = String.valueOf(Integer.MAX_VALUE).length();
    private static final int BIGINT_MAX_PRECISION = String.valueOf(Long.MAX_VALUE).length();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(CAST_FUNCTION_NAME))
            .with(argumentCount().equalTo(1))
            .with(argument(0).capturedAs(VALUE));

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

        ConnectorExpression value = captures.get(VALUE);
        return isWidening(value.getType(), call.getType())
                ? context.defaultRewrite(value)
                : Optional.empty();
    }

    private static boolean isWidening(Type source, Type target)
    {
        if (source.equals(target)) {
            return true;
        }

        return switch (source) {
            // VARCHAR -> VARCHAR
            case VarcharType s when target instanceof VarcharType t -> t.isUnbounded() || (!s.isUnbounded() && s.getBoundedLength() <= t.getBoundedLength());

            // int family -> int family
            case TinyintType _ when target instanceof SmallintType || target instanceof IntegerType || target instanceof BigintType -> true;
            case SmallintType _ when target instanceof IntegerType || target instanceof BigintType -> true;
            case IntegerType _ when target instanceof BigintType -> true;

            // int family -> DECIMAL
            case TinyintType _ when target instanceof DecimalType t && t.getScale() == 0 -> t.getPrecision() >= TINYINT_MAX_PRECISION;
            case SmallintType _ when target instanceof DecimalType t && t.getScale() == 0 -> t.getPrecision() >= SMALLINT_MAX_PRECISION;
            case IntegerType _ when target instanceof DecimalType t && t.getScale() == 0 -> t.getPrecision() >= INT_MAX_PRECISION;
            case BigintType _ when target instanceof DecimalType t && t.getScale() == 0 -> t.getPrecision() >= BIGINT_MAX_PRECISION;

            // DECIMAL -> int family
            case DecimalType s when target instanceof TinyintType && s.getScale() == 0 -> s.getPrecision() < TINYINT_MAX_PRECISION;
            case DecimalType s when target instanceof SmallintType && s.getScale() == 0 -> s.getPrecision() < SMALLINT_MAX_PRECISION;
            case DecimalType s when target instanceof IntegerType && s.getScale() == 0 -> s.getPrecision() < INT_MAX_PRECISION;
            case DecimalType s when target instanceof BigintType && s.getScale() == 0 -> s.getPrecision() < BIGINT_MAX_PRECISION;

            // REAL -> DOUBLE
            case RealType _ when target instanceof DoubleType -> true;

            // DECIMAL -> DECIMAL
            case DecimalType s when target instanceof DecimalType t && s.getScale() == t.getScale() -> s.getPrecision() <= t.getPrecision();

            // TIMESTAMP -> TIMESTAMP
            case TimestampType s when target instanceof TimestampType t -> s.getPrecision() <= t.getPrecision();

            // TIMESTAMP WITH TIME ZONE -> TIMESTAMP WITH TIME ZONE
            case TimestampWithTimeZoneType s when target instanceof TimestampWithTimeZoneType t -> s.getPrecision() <= t.getPrecision();

            default -> false;
        };
    }
}
