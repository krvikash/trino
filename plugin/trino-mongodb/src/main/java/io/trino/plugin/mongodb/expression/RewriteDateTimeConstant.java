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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.DateType;
import io.trino.spi.type.ShortTimestampType;
import io.trino.spi.type.ShortTimestampWithTimeZoneType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toDate;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;

public class RewriteDateTimeConstant
        implements ConnectorExpressionRule<Constant, MongoExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(
            type -> type instanceof TimeType || type instanceof DateType || type instanceof ShortTimestampType || type instanceof ShortTimestampWithTimeZoneType));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<MongoExpression> rewrite(Constant constant, Captures captures, RewriteContext<MongoExpression> context)
    {
        Object value = constant.getValue();
        Type type = constant.getType();
        if (value == null) {
            return Optional.empty();
        }

        if (type instanceof TimeType) {
            long picos = (long) value;
            Instant instant = Instant.ofEpochSecond(0, LocalTime.ofNanoOfDay(roundDiv(picos, PICOSECONDS_PER_NANOSECOND)).toNanoOfDay());
            return Optional.of(new MongoExpression(toDate(instant.toString())));
        }

        if (type instanceof DateType) {
            long days = (long) value;
            return Optional.of(new MongoExpression(toDate(LocalDate.ofEpochDay(days).toString())));
        }

        if (type instanceof ShortTimestampType) {
            long epochMicros = (long) value;
            long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
            return Optional.of(new MongoExpression(toDate(LocalDateTime.ofInstant(instant, UTC).toString())));
        }

        if (type instanceof ShortTimestampWithTimeZoneType) {
            long millisUtc = unpackMillisUtc((long) value);
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return Optional.of(new MongoExpression(toDate(LocalDateTime.ofInstant(instant, UTC).toString())));
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
