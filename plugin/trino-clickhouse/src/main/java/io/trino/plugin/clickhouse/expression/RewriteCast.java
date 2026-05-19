/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse.expression;

import com.clickhouse.client.ClickHouseVersionUtils;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.isAllowTimestampUnsafeCastPushdown;
import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.util.Objects.requireNonNull;

public class RewriteCast
        extends AbstractRewriteCast
{
    private final Function<ConnectorSession, ClickHouseVersionUtils> clickHouseVersionProvider;

    public RewriteCast(Function<ConnectorSession, ClickHouseVersionUtils> clickHouseVersionProvider, BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
        this.clickHouseVersionProvider = requireNonNull(clickHouseVersionProvider, "clickHouseVersionProvider is null");
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(ConnectorSession session, JdbcTypeHandle sourceTypeHandle, Type sourceType, Type targetType)
    {
        if (!pushdownSupported(session, sourceTypeHandle, sourceType, targetType)) {
            return Optional.empty();
        }

        return switch (targetType) {
            case DateType _ -> Optional.of(new JdbcTypeHandle(DATE, Optional.of("Date32"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case TimestampType timestampType -> Optional.of(new JdbcTypeHandle(TIMESTAMP, Optional.of("DateTime64(%s)".formatted(timestampType.getPrecision())), Optional.empty(), Optional.of(timestampType.getPrecision()), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }

    @Override
    protected String buildCast(@SuppressWarnings("unused") ConnectorSession session, @SuppressWarnings("unused") JdbcTypeHandle sourceTypeJdbcHandle, Type sourceType, Type targetType, String expression, String castType)
    {
        if (sourceType instanceof TimestampType sourceTimestampType
                && targetType instanceof TimestampType targetTimestampType
                && isTimestampDowncast(sourceTimestampType, targetTimestampType)) {
            return buildTimestampDownCast(expression, sourceTimestampType, targetTimestampType);
        }
        return "CAST(%s AS Nullable(%s))".formatted(expression, castType);
    }

    private static String buildTimestampDownCast(String expression, TimestampType sourceType, TimestampType targetType)
    {
        int sourcePrecision = sourceType.getPrecision();
        int targetPrecision = targetType.getPrecision();
        // For source precision 9 (nanoseconds), rounding to any lower precision will never cause an overflow,
        // since the maximum representable DateTime64 value at precision 9 is within safe bounds.
        if (sourcePrecision == 9) {
            // Use higher precision decimal for accurate rounding
            return "toDateTime64(round(toDecimal128(%1$s, 9), %2$d), %2$d)"
                    .formatted(expression, targetPrecision);
        }
        else {
            // For source precision ≤ 8, cap values to the maximum DateTime64 representable in ClickHouse
            // ('2299-12-31 23:59:59.99999999') to prevent overflow when rounding to a lower precision.
            return "if(isNull(%1$s), NULL, toDateTime64(least(round(toDecimal64(%1$s, 8), %2$d), toDecimal64(toDateTime64('2299-12-31 23:59:59.99999999', 8), 8)), %2$d))"
                    .formatted(expression, targetPrecision);
        }
    }

    private boolean pushdownSupported(ConnectorSession session, JdbcTypeHandle sourceTypeHandle, Type sourceType, Type targetType)
    {
        ClickHouseVersionUtils clickHouseVersion = clickHouseVersionProvider.apply(session);
        return switch (targetType) {
            case DateType _ -> sourceTypeHandle.jdbcType() == DATE || sourceTypeHandle.jdbcType() == TIMESTAMP;
            case TimestampType targetTimestampType -> {
                if (sourceType instanceof DateType) {
                    // Existing issue: Overflow from Date to DateTime64 prevention (https://github.com/ClickHouse/ClickHouse/pull/83982)
                    // So not pushing down cast from Date to Timestamp before ClickHouse 25.8
                    yield !clickHouseVersion.isOlderThan("25.8");
                }
                else if (sourceType instanceof TimestampType sourceTimestampType) {
                    // Timestamp downcast may cause rounding discrepancies. Allow pushdown only if explicitly enabled.
                    if (isUnsafeDownCast(sourceTimestampType, targetTimestampType) && !isAllowTimestampUnsafeCastPushdown(session)) {
                        yield false;
                    }
                    yield true;
                }
                yield false;
            }
            default -> false;
        };
    }

    private static boolean isUnsafeDownCast(TimestampType sourceType, TimestampType targetType)
    {
        return isTimestampDowncast(sourceType, targetType) && sourceType.getPrecision() <= 8;
    }

    private static boolean isTimestampDowncast(TimestampType sourceType, TimestampType targetType)
    {
        return targetType.getPrecision() < sourceType.getPrecision();
    }
}
