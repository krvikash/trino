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

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteDateTrunc;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.TrinoException;

import java.sql.Types;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.expression.AbstractRewriteDateTrunc.Unit.MILLISECOND;
import static io.trino.plugin.jdbc.expression.AbstractRewriteDateTrunc.Unit.WEEK;

public class RewriteDateTrunc
        extends AbstractRewriteDateTrunc
{
    @Override
    protected boolean pushdownSupported(JdbcTypeHandle jdbcTypeHandle, Unit unit)
    {
        // ClickHouse rounds fractional values for the 'millisecond' unit, whereas Trino truncates them.
        // To avoid inconsistent results, this case is not supported.
        // TODO: https://starburstdata.atlassian.net/browse/ENG-2640
        if (unit.equals(MILLISECOND)) {
            return false;
        }
        return switch (jdbcTypeHandle.jdbcType()) {
            case Types.DATE, Types.TIMESTAMP -> true;
            default -> false;
        };
    }

    @Override
    protected String buildDateTrunc(JdbcTypeHandle typeHandle, ParameterizedExpression expression, Unit unit)
    {
        if (requiresCastToDateTime64(typeHandle, unit)) {
            // it is safe to do the cast because we don't support unit of dateTrunc higher than second
            return "dateTrunc('%s', CAST(%s AS Nullable(DateTime64(0))))".formatted(unit, expression.expression());
        }
        return "dateTrunc('%s', %s)".formatted(unit, expression.expression());
    }

    private static boolean requiresCastToDateTime64(JdbcTypeHandle typeHandle, Unit unit)
    {
        // For Date and DateTime values '1970-01-01 00:00:00', using date_trunc with 'week' unit returns
        // '1969-12-29 00:00:00', which cannot be represented by Date or DateTime types.
        // Therefore, cast to DateTime64(0) to safely handle the result.
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        ClickHouseColumn column = ClickHouseColumn.of("", jdbcTypeName);
        ClickHouseDataType columnDataType = column.getDataType();
        if (columnDataType == ClickHouseDataType.DateTime || columnDataType == ClickHouseDataType.Date) {
            return unit.equals(WEEK);
        }
        // There is an existing issue with Date32 type which returns incorrect result for
        // date before 1970 (https://github.com/ClickHouse/ClickHouse/issues/70529).
        // So, for Date32 type, always cast to DateTime64(0) to avoid the issue.
        return columnDataType == ClickHouseDataType.Date32;
    }
}
