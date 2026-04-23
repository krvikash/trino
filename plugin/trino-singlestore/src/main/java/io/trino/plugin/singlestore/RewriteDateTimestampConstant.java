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
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteDateTimestampConstant
        implements ConnectorExpressionRule<Constant, ParameterizedExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(type -> type instanceof DateType || type instanceof TimestampType));

    // SingleStore officially supported DATE range: 1000-01-01 to 9999-12-31 (possible values for data_conversion_compatibility_level >= 7.0)
    private static final long MIN_DATE_EPOCH_DAYS = LocalDate.of(1000, 1, 1).toEpochDay();
    private static final long MAX_DATE_EPOCH_DAYS = LocalDate.of(9999, 12, 31).toEpochDay();

    // SingleStore officially supported DATETIME range: 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999 (possible values for data_conversion_compatibility_level >= 7.0)
    private static final long MICROSECONDS_PER_DAY = 24L * 60 * 60 * 1_000_000;
    private static final long MIN_DATETIME_MICROS = MIN_DATE_EPOCH_DAYS * MICROSECONDS_PER_DAY;
    private static final long MAX_DATETIME_MICROS = (MAX_DATE_EPOCH_DAYS + 1) * MICROSECONDS_PER_DAY - 1;

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Constant constant, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Object value = constant.getValue();
        if (value == null) {
            return Optional.empty();
        }
        long longValue = (long) value;

        // Trino supports broader timestamps/date ranges than SingleStore - which in case of pushdown produce "Invalid DATE/TIME in type conversion" error.
        // SingleStore also has data_conversion_compatibility_level https://docs.singlestore.com/cloud/reference/sql-reference/data-types/data-type-conversion/
        // which when set below 7.0 can allow to express dates/timestamps to some degree outside officially supported ranges (e.g. dates/datetimes before 1000-01-01)
        // and produces null for some date/timestamp casts instead of explicit error.
        // For those reasons we don't allow pushdown of literals that are outside official ranges https://docs.singlestore.com/cloud/reference/sql-reference/data-types/time-and-date/
        Type constantType = constant.getType();
        if (constantType instanceof DateType) {
            if (longValue < MIN_DATE_EPOCH_DAYS || longValue > MAX_DATE_EPOCH_DAYS) {
                return Optional.empty();
            }
        }
        else if (constantType instanceof TimestampType) {
            if (longValue < MIN_DATETIME_MICROS || longValue > MAX_DATETIME_MICROS) {
                return Optional.empty();
            }
        }
        return Optional.of(new ParameterizedExpression("?", ImmutableList.of(new QueryParameter(constantType, Optional.of(value)))));
    }
}
