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

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.singlestore.SingleStoreSessionProperties.isEnableStringPushdownWithBinary;
import static java.util.stream.Collectors.joining;

public class BinaryComparisonQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public BinaryComparisonQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String getGroupBy(JdbcClient client, Optional<List<List<JdbcColumnHandle>>> groupingSets)
    {
        if (groupingSets.isEmpty()) {
            return "";
        }

        // Supporting only single grouping set for now
        verify(!groupingSets.get().isEmpty() && groupingSets.get().size() == 1, "Multiple grouping sets not supported: %s", groupingSets.get());
        List<JdbcColumnHandle> groupingSet = getOnlyElement(groupingSets.get());
        if (groupingSet.isEmpty()) {
            // global aggregation
            return "";
        }
        return " GROUP BY " + groupingSet.stream()
                .map(column -> {
                    String name = client.quoted(column.getColumnName());
                    // Use BINARY in GROUP BY to enforce case-sensitive grouping on string columns
                    // https://docs.singlestore.com/cloud/reference/sql-reference/character-encoding/collations-supported/
                    return isStringType(column) ? "BINARY " + name : name;
                })
                .collect(joining(", "));
    }

    @Override
    protected String toPredicate(
            JdbcClient client,
            ConnectorSession session,
            JdbcColumnHandle column,
            JdbcTypeHandle jdbcType,
            Type type,
            WriteFunction writeFunction,
            String operator,
            Object value,
            Consumer<QueryParameter> accumulator)
    {
        if (isStringType(column) && isEnableStringPushdownWithBinary(session)) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return "BINARY %s %s %s".formatted(client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    @Override
    protected String toInPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, List<Object> singleValues, WriteFunction writeFunction, Consumer<QueryParameter> accumulator)
    {
        if (isStringType(column) && isEnableStringPushdownWithBinary(session)) {
            return "BINARY " + super.toInPredicate(client, session, column, jdbcType, type, singleValues, writeFunction, accumulator);
        }
        return super.toInPredicate(client, session, column, jdbcType, type, singleValues, writeFunction, accumulator);
    }

    private static boolean isStringType(JdbcColumnHandle column)
    {
        return column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType;
    }
}
